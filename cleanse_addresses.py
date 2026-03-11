"""
cleanse_addresses.py - Libpostal Address Cleansing Script

Reads address data from a CSV file, parses each address using the Libpostal
library, and outputs a new CSV with all extracted components plus metadata.

Usage:
    python cleanse_addresses.py --input addresses.csv --output cleansed.csv
    python cleanse_addresses.py -i addresses.csv -o output.csv --verbose --skip-errors
    python cleanse_addresses.py -i addresses.tsv -o output.csv --delimiter $'\\t' --encoding iso-8859-1
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LIBPOSTAL_LABELS = [
    "house_number",
    "road",
    "unit",
    "level",
    "staircase",
    "entrance",
    "po_box",
    "postcode",
    "suburb",
    "city_district",
    "city",
    "island",
    "state_district",
    "state",
    "country_region",
    "country",
    "world_region",
    "house",
    "category",
    "near",
]

LABEL_TO_COLUMN = {label: f"lp_{label}" for label in LIBPOSTAL_LABELS}

# Output lp_* columns in alphabetical order per spec
LP_OUTPUT_COLUMNS = sorted(LABEL_TO_COLUMN.values())

# Metadata columns appended last
METADATA_COLUMNS = ["lp_components_count", "lp_parsing_success", "lp_parsing_error"]

# Input address column names (case-insensitive); defines concatenation order
INPUT_ADDRESS_COLUMNS = [
    "address_line_1",
    "address_line_2",
    "address_line_3",
    "address_line_4",
    "address_line_5",
    "city",
    "state_province",
    "postal_code",
    "country",
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Cleanse address data from a CSV using Libpostal.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        metavar="FILE",
        help="Path to input CSV file.",
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        metavar="FILE",
        help="Path to output CSV file.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="File encoding for input and output.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter character.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="Number of rows to process per batch before writing to disk.",
    )
    parser.add_argument(
        "--skip-errors",
        action="store_true",
        help="Continue processing if individual rows fail instead of aborting.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show progress bar (use --no-progress to disable).",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="FILE",
        help="Path to log file for errors/warnings (defaults to stderr).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool, log_file: str | None) -> logging.Logger:
    """Configure and return the root logger.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO.
        log_file: Optional path to a file for log output.

    Returns:
        Configured logger instance.
    """
    level = logging.DEBUG if verbose else logging.INFO
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(message)s",
        handlers=handlers,
    )
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_input_file(
    filepath: str,
    encoding: str,
    delimiter: str,
) -> tuple[list[str], int]:
    """Validate that the input CSV exists, is readable, and contains at least
    one known address column.

    Args:
        filepath: Path to the input CSV.
        encoding: File encoding.
        delimiter: CSV delimiter character.

    Returns:
        Tuple of (column_names, row_count).

    Raises:
        SystemExit: On any validation failure.
    """
    path = Path(filepath)
    if not path.exists():
        sys.exit(f"[ERROR] Input file not found: {filepath}")
    if not path.is_file():
        sys.exit(f"[ERROR] Input path is not a file: {filepath}")

    try:
        with path.open(encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            try:
                columns = next(reader)
            except StopIteration:
                sys.exit(f"[ERROR] Input file is empty: {filepath}")

            # Detect duplicate column names
            lower_cols = [c.lower() for c in columns]
            if len(lower_cols) != len(set(lower_cols)):
                sys.exit("[ERROR] Input CSV contains duplicate column names.")

            # Require at least one address-related column
            known = set(INPUT_ADDRESS_COLUMNS)
            if not any(c.lower() in known for c in columns):
                sys.exit(
                    "[ERROR] Input CSV has no recognized address columns. "
                    f"Expected at least one of: {', '.join(INPUT_ADDRESS_COLUMNS)}"
                )

            # Count data rows
            row_count = sum(1 for _ in reader)

    except (OSError, UnicodeDecodeError) as exc:
        sys.exit(f"[ERROR] Cannot read input file: {exc}")

    return columns, row_count


def validate_output_path(filepath: str) -> None:
    """Validate that the output path is writable.

    Args:
        filepath: Desired output file path.

    Raises:
        SystemExit: If the path cannot be written.
    """
    path = Path(filepath)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Attempt open to verify write permission
        path.open("a").close()
    except OSError as exc:
        sys.exit(f"[ERROR] Cannot write to output file: {exc}")


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def concatenate_address_fields(row: dict, present_address_cols: list[str]) -> str:
    """Assemble address fields into a single string for Libpostal.

    Fields are joined in the canonical INPUT_ADDRESS_COLUMNS order. Empty or
    whitespace-only values are skipped.

    Args:
        row: Dictionary of column name → value for a single CSV row.
        present_address_cols: Lowercase column names that exist in the input
            and are recognised address fields.

    Returns:
        Comma-space joined address string, or empty string if all fields empty.
    """
    parts = []
    for col in INPUT_ADDRESS_COLUMNS:
        if col not in present_address_cols:
            continue
        value = str(row.get(col, "") or "").strip()
        if value:
            parts.append(value)
    return ", ".join(parts)


def parse_with_libpostal(
    address_string: str,
    country_hint: str | None,
    postal_parser,
) -> dict[str, str]:
    """Parse an address string with Libpostal and return a components dict.

    When Libpostal returns the same label more than once the values are joined
    with a space separator.

    Args:
        address_string: Full address string to parse.
        country_hint: ISO 3166-1 alpha-2 country code hint, or None.
        postal_parser: The imported postal.parser module.

    Returns:
        Dict mapping Libpostal label → extracted value string.
    """
    if not address_string:
        return {}

    kwargs: dict = {}
    if country_hint:
        kwargs["country"] = country_hint.lower()

    raw = postal_parser.parse_address(address_string, **kwargs)

    components: dict[str, str] = {}
    for value, label in raw:
        if label in components:
            components[label] = components[label] + " " + value
        else:
            components[label] = value

    return components


def process_row(
    row: dict,
    present_address_cols: list[str],
    output_columns: list[str],
    skip_errors: bool,
    postal_parser,
    logger: logging.Logger,
    row_num: int,
) -> dict:
    """Process a single CSV row and return an enriched output dict.

    Args:
        row: Input row as a dict.
        present_address_cols: Lowercase address column names present in input.
        output_columns: Ordered list of all output column names.
        skip_errors: If True, errors populate lp_parsing_error instead of
            raising.
        postal_parser: The imported postal.parser module.
        logger: Logger instance.
        row_num: 1-based row number for logging.

    Returns:
        Dict with all output columns populated.

    Raises:
        Exception: Re-raised when skip_errors is False and parsing fails.
    """
    # Start with all original values
    out: dict = {col: row.get(col, "") for col in row}

    concatenated = concatenate_address_fields(row, present_address_cols)
    out["concatenated_address"] = concatenated

    # Initialise all lp_* columns to empty string
    for col in LP_OUTPUT_COLUMNS:
        out[col] = ""

    out["lp_components_count"] = 0
    out["lp_parsing_success"] = False
    out["lp_parsing_error"] = ""

    if not concatenated:
        logger.warning("Row %d: All address fields are empty — skipping parse.", row_num)
        return _select_columns(out, output_columns)

    # Determine country hint
    raw_country = str(row.get("country", "") or "").strip()
    country_hint = raw_country if (len(raw_country) == 2 and raw_country.isalpha()) else None

    try:
        components = parse_with_libpostal(concatenated, country_hint, postal_parser)

        for label, value in components.items():
            col_name = LABEL_TO_COLUMN.get(label)
            if col_name:
                out[col_name] = value

        count = len(components)
        out["lp_components_count"] = count
        out["lp_parsing_success"] = count > 0

        if count == 0:
            logger.warning("Row %d: No components extracted from: %s", row_num, concatenated)

    except Exception as exc:  # noqa: BLE001
        error_msg = str(exc)
        logger.error("Row %d: Libpostal parsing failed — %s", row_num, error_msg)
        out["lp_parsing_error"] = error_msg
        out["lp_parsing_success"] = False
        if not skip_errors:
            raise

    return _select_columns(out, output_columns)


def _select_columns(row_dict: dict, columns: list[str]) -> dict:
    """Return a new dict containing only the specified columns in order.

    Args:
        row_dict: Source dict (may have extra keys).
        columns: Ordered column names for the output.

    Returns:
        Ordered dict with exactly the requested columns.
    """
    return {col: row_dict.get(col, "") for col in columns}


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def build_output_columns(input_columns: list[str]) -> list[str]:
    """Determine the ordered list of output CSV columns.

    Order:
        1. All original input columns (original order)
        2. concatenated_address
        3. lp_* component columns (alphabetical)
        4. Metadata columns

    Args:
        input_columns: Column names from the input CSV header.

    Returns:
        Ordered list of output column names.
    """
    return list(input_columns) + ["concatenated_address"] + LP_OUTPUT_COLUMNS + METADATA_COLUMNS


def process_csv(args: argparse.Namespace, logger: logging.Logger, postal_parser) -> dict:
    """Main processing loop: read input CSV in chunks, parse addresses, write output.

    Args:
        args: Parsed CLI arguments.
        logger: Logger instance.
        postal_parser: The imported postal.parser module.

    Returns:
        Stats dict with keys: total, success, failed, components_sum, elapsed.
    """
    input_columns, row_count = validate_input_file(
        args.input, args.encoding, args.delimiter
    )
    validate_output_path(args.output)

    logger.info("Loading input file: %s", args.input)
    logger.info("Found %s rows to process", f"{row_count:,}")
    logger.info("Initializing Libpostal parser...")

    # Determine which address columns are present (case-insensitive lookup)
    lower_to_original = {c.lower(): c for c in input_columns}
    present_address_cols = [
        col for col in INPUT_ADDRESS_COLUMNS if col in lower_to_original
    ]
    # Normalise row access: map lowercase to the actual column header as-is
    # (pandas will use the original header names, so we work with lowercase col
    #  names for detection and original names for access)

    output_columns = build_output_columns(input_columns)

    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "components_sum": 0,
        "elapsed": 0.0,
    }

    start_time = time.monotonic()
    batch_num = 0
    total_batches = max(1, (row_count + args.batch_size - 1) // args.batch_size)

    # Write output header
    with open(args.output, "w", newline="", encoding=args.encoding) as out_f:
        writer = csv.DictWriter(out_f, fieldnames=output_columns)
        writer.writeheader()

    progress = tqdm(
        total=row_count,
        unit="rows",
        desc="Processing addresses",
        disable=not args.progress,
        file=sys.stdout,
    )

    try:
        for chunk in pd.read_csv(
            args.input,
            chunksize=args.batch_size,
            dtype=str,
            keep_default_na=False,
            encoding=args.encoding,
            delimiter=args.delimiter,
        ):
            batch_num += 1
            chunk_start = stats["total"] + 1
            chunk_end = stats["total"] + len(chunk)
            logger.info(
                "Processing batch %d/%d (rows %s-%s)...",
                batch_num,
                total_batches,
                f"{chunk_start:,}",
                f"{chunk_end:,}",
            )

            processed_rows = []
            for local_idx, (_, row_series) in enumerate(chunk.iterrows()):
                row_num = stats["total"] + local_idx + 1
                row = row_series.to_dict()

                # Normalise column access for case-insensitive address columns
                # (The actual keys in `row` match the CSV header casing.)
                try:
                    out_row = process_row(
                        row=row,
                        present_address_cols=present_address_cols,
                        output_columns=output_columns,
                        skip_errors=args.skip_errors,
                        postal_parser=postal_parser,
                        logger=logger,
                        row_num=row_num,
                    )
                    stats["success"] += 1
                    stats["components_sum"] += int(out_row.get("lp_components_count", 0))
                except Exception:  # noqa: BLE001
                    stats["failed"] += 1
                    # Row with error fields already logged; build minimal output
                    out_row = {col: row.get(col, "") for col in output_columns}
                    out_row["concatenated_address"] = concatenate_address_fields(
                        row, present_address_cols
                    )
                    out_row["lp_parsing_success"] = False
                    out_row["lp_parsing_error"] = "Processing aborted"
                    out_row["lp_components_count"] = 0
                    processed_rows.append(out_row)
                    stats["total"] += local_idx + 1
                    progress.update(len(chunk))
                    # Write what we have and exit
                    _append_rows(processed_rows, output_columns, args.output, args.encoding)
                    elapsed = time.monotonic() - start_time
                    stats["elapsed"] = elapsed
                    sys.exit(
                        f"[ERROR] Row {row_num}: Libpostal parsing failed. "
                        "Use --skip-errors to continue past failures."
                    )

                processed_rows.append(out_row)

            stats["total"] += len(chunk)
            progress.update(len(chunk))
            _append_rows(processed_rows, output_columns, args.output, args.encoding)

    finally:
        progress.close()

    stats["elapsed"] = time.monotonic() - start_time
    logger.info(
        "Processing complete: %s successful, %s failed",
        f"{stats['success']:,}",
        f"{stats['failed']:,}",
    )
    logger.info("Writing output to: %s", args.output)
    return stats


def _append_rows(rows: list[dict], columns: list[str], filepath: str, encoding: str) -> None:
    """Append a batch of processed rows to the output CSV.

    Args:
        rows: List of row dicts to write.
        columns: Ordered output column names.
        filepath: Output file path.
        encoding: File encoding.
    """
    if not rows:
        return
    with open(filepath, "a", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(stats: dict, output_file: str) -> None:
    """Print formatted summary statistics to stdout.

    Args:
        stats: Dict from process_csv with keys total, success, failed,
            components_sum, elapsed.
        output_file: Path to the output file (for display).
    """
    total = stats["total"]
    success = stats["success"]
    failed = stats["failed"]
    elapsed = stats["elapsed"]
    throughput = total / elapsed if elapsed > 0 else 0
    avg_components = (stats["components_sum"] / success) if success > 0 else 0.0
    success_pct = (success / total * 100) if total > 0 else 0.0
    failed_pct = (failed / total * 100) if total > 0 else 0.0

    print("\n=== Processing Summary ===")
    print(f"Total rows:          {total:>10,}")
    print(f"Successfully parsed: {success:>10,} ({success_pct:.2f}%)")
    print(f"Failed to parse:     {failed:>10,} ({failed_pct:.2f}%)")
    print(f"Average components:  {avg_components:>10.1f}")
    print(f"Processing time:     {elapsed:>9.1f}s")
    print(f"Throughput:          {throughput:>9.1f} rows/sec")
    print(f"Output file:         {output_file}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the address cleansing script."""
    args = parse_arguments()
    logger = setup_logging(args.verbose, args.log_file)

    # Validate Libpostal availability before doing any real work
    try:
        import postal.parser as postal_parser  # noqa: PLC0415
    except ImportError:
        sys.exit(
            "[ERROR] Could not import 'postal'. Ensure Libpostal and the Python "
            "bindings are installed.\n"
            "  System library: https://github.com/openvenues/libpostal\n"
            "  Python package:  pip install postal==1.1.10"
        )

    stats = process_csv(args, logger, postal_parser)
    print_summary(stats, args.output)
    logger.info(
        "Done! Processed %s rows in %.1f seconds",
        f"{stats['total']:,}",
        stats["elapsed"],
    )


if __name__ == "__main__":
    main()
