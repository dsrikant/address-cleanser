# address-cleanser

A command-line Python script that parses and cleanses address data from CSV files using [Libpostal](https://github.com/openvenues/libpostal).

## Overview

`cleanse_addresses.py` reads address fields from a CSV file, assembles them into a single string, passes each through Libpostal's statistical address parser, and writes a new CSV containing all extracted components alongside the original data.

---

## System Requirements

- Python 3.9+
- Libpostal C library (see installation below)

---

## Installation

### 1. Install the Libpostal C Library

Libpostal must be installed at the system level before installing the Python package.

**macOS (Homebrew):**
```bash
brew install libpostal
```

**Ubuntu/Debian (from source):**
```bash
sudo apt-get install curl autoconf automake libtool pkg-config

git clone https://github.com/openvenues/libpostal
cd libpostal
./bootstrap.sh
./configure --datadir=/usr/share/libpostal
make -j4
sudo make install
sudo ldconfig
```

> The `--datadir` flag sets where Libpostal downloads its model data (~2 GB). Adjust as needed.

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Usage

### Basic

```bash
python cleanse_addresses.py --input addresses.csv --output cleansed_addresses.csv
```

### With Options

```bash
python cleanse_addresses.py \
  --input addresses.csv \
  --output output.csv \
  --verbose \
  --skip-errors \
  --batch-size 500
```

### Custom Encoding and Delimiter

```bash
python cleanse_addresses.py \
  --input addresses.tsv \
  --output output.csv \
  --delimiter $'\t' \
  --encoding iso-8859-1
```

### Disable Progress Bar

```bash
python cleanse_addresses.py -i addresses.csv -o output.csv --no-progress
```

### Log Errors to File

```bash
python cleanse_addresses.py -i addresses.csv -o output.csv --log-file errors.log
```

---

## CLI Arguments

| Argument | Short | Required | Default | Description |
|---|---|---|---|---|
| `--input` | `-i` | Yes | — | Path to input CSV file |
| `--output` | `-o` | Yes | — | Path to output CSV file |
| `--encoding` | | No | `utf-8` | File encoding |
| `--delimiter` | | No | `,` | CSV delimiter character |
| `--batch-size` | | No | `1000` | Rows per processing batch |
| `--skip-errors` | | No | `False` | Continue on row-level failures |
| `--verbose` | `-v` | No | `False` | Enable verbose logging |
| `--progress` / `--no-progress` | | No | progress on | Show/hide progress bar |
| `--log-file` | | No | None | Write logs to this file |

---

## Input Format

The input CSV must have a header row and may include any of these address columns (case-insensitive):

| Column | Description | Example |
|---|---|---|
| `address_line_1` | First address line | `123 Main Street` |
| `address_line_2` | Second address line | `Apt 4B` |
| `address_line_3` | Third address line | `Building C` |
| `address_line_4` | Fourth address line | `Floor 2` |
| `address_line_5` | Fifth address line | `Suite 200` |
| `city` | City name | `New York` |
| `state_province` | State or province | `NY` |
| `postal_code` | Postal/ZIP code | `10001` |
| `country` | Country name or ISO code | `USA` or `US` |

Additional columns (e.g. `customer_id`) are preserved unchanged in the output.

**Notes:**
- At least one of the above columns must be present
- UTF-8 encoding by default (override with `--encoding`)
- Empty/null values in any field are allowed

---

## Output Format

The output CSV contains:

1. **All original input columns** (in original order)
2. `concatenated_address` — the full string sent to Libpostal
3. **Libpostal component columns** (alphabetically sorted):
   - `lp_category`, `lp_city`, `lp_city_district`, `lp_country`, `lp_country_region`
   - `lp_entrance`, `lp_house`, `lp_house_number`, `lp_island`, `lp_level`
   - `lp_near`, `lp_po_box`, `lp_postcode`, `lp_road`, `lp_staircase`
   - `lp_state`, `lp_state_district`, `lp_suburb`, `lp_unit`, `lp_world_region`
4. **Metadata columns:**
   - `lp_components_count` — number of components extracted
   - `lp_parsing_success` — `True` if at least one component was extracted
   - `lp_parsing_error` — error message if parsing failed, empty otherwise

All `lp_*` columns are always present in the output, even if all values are empty.

---

## Test Data

Sample files are in `test_data/`:

```bash
# US addresses
python cleanse_addresses.py -i test_data/test_simple.csv -o /tmp/out_simple.csv --verbose

# International addresses (Tokyo, London, Paris)
python cleanse_addresses.py -i test_data/test_international.csv -o /tmp/out_intl.csv

# Edge cases (empty rows, messy input) — use --skip-errors
python cleanse_addresses.py -i test_data/test_messy.csv -o /tmp/out_messy.csv --skip-errors
```

---

## Troubleshooting

**`ImportError: No module named 'postal'`**
The Python bindings are not installed. Run:
```bash
pip install postal==1.1.10
```
If that fails, ensure the Libpostal C library is installed first (see Installation above).

**`OSError: libpostal.so not found`**
The C library is installed but not on the linker path. Try:
```bash
sudo ldconfig          # Linux
# or
export DYLD_LIBRARY_PATH=/usr/local/lib  # macOS
```

**`UnicodeDecodeError` on input file**
Specify the correct encoding:
```bash
python cleanse_addresses.py -i file.csv -o out.csv --encoding latin-1
```

**Rows with no components extracted**
This is normal for non-address text or very sparse input. Check `lp_parsing_success` in the output. Use `--skip-errors` to continue past failures and inspect `lp_parsing_error` for details.

**Slow performance on large files**
Libpostal initialises its model data on first call (a few seconds). Subsequent rows are fast. Throughput is typically 150–300 rows/sec depending on hardware.

---

## License

MIT
