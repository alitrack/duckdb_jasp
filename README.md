# DuckDB JASP Extension

DuckDB table functions that read **JASP project files** (`.jasp`) directly
into SQL — no JASP install, no Python, no copy-paste.

A `.jasp` file is a ZIP archive containing an embedded `internal.sqlite`
(the data), `analyses.json` (the analysis registry) and `manifest.json`.
This extension opens that SQLite database, resolves JASP's column model
(`Columns` / `Labels`), and exposes clean typed tables.

## Table functions

| Function | Returns |
|----------|---------|
| `read_jasp(path)` | one row per data row, columns typed by JASP role |
| `read_jasp(path, dataset := N)` | a specific `DataSet_N` (default `1`) |
| `read_jasp_columns(path)` | `column_name`, `role`, `column_id` |
| `read_jasp_analyses(path)` | `analysis_id`, `analysis_name`, `module`, `dataset` |

### Column roles

JASP stores every logical column as a `Column_{id}_DBL` (real/text) +
`Column_{id}_INT` (categorical code) pair. This extension maps the logical
`columnType` to a DuckDB type:

| JASP `columnType` | DuckDB type | value source |
|-------------------|-------------|--------------|
| `scale`           | `DOUBLE`    | `_DBL` real (`nan` → `NULL`) |
| `ordinal`         | `DOUBLE`    | `_DBL` code |
| `nominal`         | `VARCHAR`   | `Labels[_INT]` |
| `nominalText`     | `VARCHAR`   | `_DBL` text |

## Usage

```sql
LOAD 'jasp_ext.duckdb_extension';   -- dev; use INSTALL+LOAD for distribution

SELECT * FROM read_jasp('/path/to/Album Sales.jasp') LIMIT 5;

-- run an analysis on JASP data with plain SQL
SELECT avg(sales) FROM read_jasp('/path/to/Album Sales.jasp');

-- what's in the file?
SELECT * FROM read_jasp_columns('/path/to/file.jasp');
SELECT * FROM read_jasp_analyses('/path/to/file.jasp');
```

## Build

Rust (C extension API, quack-rs 0.14 / libduckdb-sys 1.10504, DuckDB v1.2.0
host). Dependencies: `zip 0.6` (deflate) + `rusqlite 0.32` (bundled sqlite).

```bash
make                 # builds build/debug + build/release
make release
```

## Status

v0.1.0 — reads single & multi-dataset `.jasp` files; numeric, ordinal and
categorical (labelled) columns; missing values → `NULL`.
