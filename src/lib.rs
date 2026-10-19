//! duckdb_jasp — read JASP project files (.jasp) as SQL tables.
//!
//! A `.jasp` file is a ZIP containing:
//!   - `internal.sqlite`        the real data
//!   - `analyses.json`          analysis registry
//!   - `manifest.json`          metadata
//!   - `resources/N/jaspResults.json`  (per-analysis results)
//!
//! Data lives in `DataSet` / `DataSet_N`. Every logical column is stored as
//! a pair of physical columns: `Column_{id}_DBL` (a REAL or a TEXT) and
//! `Column_{id}_INT` (a categorical code). The `Columns` table maps the
//! logical `id` / `name` / `columnType`, and `Labels` maps categorical codes
//! to text.
//!
//! Table functions:
//!   read_jasp(path)                      → data rows (typed)
//!   read_jasp(path, dataset INTEGER)     → a specific DataSet_N
//!   read_jasp_columns(path [, dataset])  → column metadata
//!   read_jasp_analyses(path)             → one row per analysis
//!
//! Column role (from `Columns.columnType`):
//!   scale        → DOUBLE (the _DBL REAL value; 'nan' → NULL)
//!   ordinal      → DOUBLE (the _DBL code)
//!   nominal      → VARCHAR (Labels[_INT])
//!   nominalText  → VARCHAR (the _DBL TEXT value)

use std::collections::HashMap;
use std::io::Cursor;

use quack_rs::{
    entry_point::init_extension_v2,
    prelude::*,
    table::TableFunctionBuilder,
    types::TypeId,
};

use libduckdb_sys::{
    duckdb_data_chunk, duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size,
    duckdb_extension_access, duckdb_extension_info, duckdb_vector_assign_string_element_len,
    duckdb_vector_get_data, duckdb_vector_get_validity, duckdb_validity_set_row_invalid, idx_t,
};

// ── JASP value model ──────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Scale,
    Ordinal,
    Nominal,
    NominalText,
}

impl Role {
    fn type_id(self) -> TypeId {
        match self {
            Role::Scale | Role::Ordinal => TypeId::Double,
            Role::Nominal | Role::NominalText => TypeId::Varchar,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Role::Scale => "scale",
            Role::Ordinal => "ordinal",
            Role::Nominal => "nominal",
            Role::NominalText => "nominalText",
        }
    }
}

fn role_from_type(t: &str) -> Role {
    match t {
        "scale" => Role::Scale,
        "ordinal" => Role::Ordinal,
        "nominal" => Role::Nominal,
        "nominalText" => Role::NominalText,
        _ => Role::Scale,
    }
}

/// A resolved, output-ready cell.
#[derive(Clone)]
enum Cell {
    F64(f64),
    Str(String),
    Null,
}

#[derive(Clone)]
struct JaspColumn {
    id: i64,
    name: String,
    role: Role,
    labels: HashMap<i64, String>,
}

struct JaspData {
    columns: Vec<JaspColumn>,
    rows: Vec<Vec<Cell>>,
}

struct AnalysisEntry {
    id: i64,
    name: String,
    module: String,
    dataset: i64,
}

// ── parse a .jasp file ────────────────────────────────────────────

fn parse_jasp(path: &str, dataset: i64) -> Result<JaspData, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| format!("open zip {path}: {e}"))?;

    let mut sqlite_bytes = Vec::new();
    {
        let mut f = zip
            .by_name("internal.sqlite")
            .map_err(|e| format!("no internal.sqlite in {path}: {e}"))?;
        std::io::Read::read_to_end(&mut f, &mut sqlite_bytes)
            .map_err(|e| format!("reading internal.sqlite: {e}"))?;
    }
    let tmp = std::env::temp_dir().join(format!(
        "jasp_{}_{}.sqlite",
        std::process::id(),
        dataset
    ));
    std::fs::write(&tmp, &sqlite_bytes).map_err(|e| format!("write temp: {e}"))?;
    let conn = rusqlite::Connection::open(&tmp).map_err(|e| format!("open sqlite: {e}"))?;

    // Data tables are always DataSet_{id}; the first dataset has id 1.
    let dataset_name = format!("DataSet_{dataset}");
    let table_exists: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [dataset_name.as_str()],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if table_exists == 0 {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!(
            "dataset {dataset} ({dataset_name}) not found in {path}"
        ));
    }

    // columns, in display order. Physical storage column = Column_{id}.
    let mut columns: Vec<JaspColumn> = Vec::new();
    {
        let mut s = conn
            .prepare(
                "SELECT id, name, columnType FROM Columns
                 WHERE analysisId IS NULL OR analysisId=0
                 ORDER BY colIdx",
            )
            .map_err(|e| e.to_string())?;
        let rows = s
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        for r in rows {
            let (id, name, ctype) = r.map_err(|e| e.to_string())?;
            let role = role_from_type(&ctype);
            let mut labels: HashMap<i64, String> = HashMap::new();
            if matches!(role, Role::Nominal) {
                let mut ls = conn
                    .prepare("SELECT value, label FROM Labels WHERE columnId=?1")
                    .map_err(|e| e.to_string())?;
                let lrows = ls
                    .query_map([id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                    .map_err(|e| e.to_string())?;
                for lr in lrows {
                    if let Ok((code, label)) = lr {
                        labels.insert(code, label);
                    }
                }
            }
            columns.push(JaspColumn {
                id,
                name,
                role,
                labels,
            });
        }
    }
    if columns.is_empty() {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("no columns found in {path}"));
    }

    // data rows — SELECT Column_{id}_DBL, Column_{id}_INT for each logical column
    let mut sel = String::from("SELECT ");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            sel.push_str(", ");
        }
        sel.push_str(&format!("\"Column_{}_DBL\"", c.id));
        sel.push_str(&format!(" , \"Column_{}_INT\"", c.id));
    }
    sel.push_str(&format!(" FROM \"{}\" ORDER BY rowNumber", dataset_name));
    let mut s = conn.prepare(&sel).map_err(|e| format!("prepare: {e}"))?;
    let rows: Vec<Vec<Cell>> = {
        let ncol = columns.len();
        let mut out = Vec::new();
        let stream = s
            .query_map([], |row| {
                let mut cells = Vec::with_capacity(ncol * 2);
                for i in 0..ncol {
                    let dv = row.get::<_, rusqlite::types::Value>(i * 2)?;
                    let iv = row.get::<_, rusqlite::types::Value>(i * 2 + 1)?;
                    cells.push(dv);
                    cells.push(iv);
                }
                Ok(cells)
            })
            .map_err(|e| e.to_string())?;
        for r in stream {
            let raw = r.map_err(|e| e.to_string())?;
            let mut rowc = Vec::with_capacity(ncol);
            for i in 0..ncol {
                let dbl = &raw[i * 2];
                let intv = &raw[i * 2 + 1];
                rowc.push(resolve_cell(&columns[i], dbl, intv));
            }
            out.push(rowc);
        }
        out
    };
    let _ = std::fs::remove_file(&tmp);
    Ok(JaspData { columns, rows })
}

fn resolve_cell(
    c: &JaspColumn,
    dbl: &rusqlite::types::Value,
    intv: &rusqlite::types::Value,
) -> Cell {
    use rusqlite::types::Value;
    match c.role {
        Role::Scale => match dbl {
            Value::Real(v) => Cell::F64(*v),
            Value::Integer(v) => Cell::F64(*v as f64),
            Value::Text(t) => {
                let t = t.trim();
                if t.eq_ignore_ascii_case("nan") || t.is_empty() {
                    Cell::Null
                } else {
                    t.parse::<f64>()
                        .map(Cell::F64)
                        .unwrap_or(Cell::Null)
                }
            }
            Value::Null => Cell::Null,
            _ => Cell::Null,
        },
        Role::Ordinal => match dbl {
            Value::Real(v) => Cell::F64(*v),
            Value::Integer(v) => Cell::F64(*v as f64),
            Value::Text(t) => t.parse::<f64>().map(Cell::F64).unwrap_or(Cell::Null),
            _ => Cell::Null,
        },
        Role::Nominal => {
            let code = match intv {
                Value::Integer(v) => *v,
                Value::Real(v) => *v as i64,
                _ => return Cell::Null,
            };
            Cell::Str(c.labels.get(&code).cloned().unwrap_or_else(|| code.to_string()))
        }
        Role::NominalText => match dbl {
            Value::Text(t) => Cell::Str(t.clone()),
            Value::Real(v) => Cell::Str(format!("{}", v)),
            Value::Integer(v) => Cell::Str(format!("{}", v)),
            _ => Cell::Null,
        },
    }
}

// ── zip member helpers (manifest / analyses) ──────────────────────

fn read_zip_member(path: &str, name: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).ok()?;
    let mut f = zip.by_name(name).ok()?;
    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut f, &mut buf).ok()?;
    Some(String::from_utf8_lossy(&buf).into_owned())
}

fn parse_analyses(path: &str) -> Vec<AnalysisEntry> {
    let analyses =
        read_zip_member(path, "analyses.json").unwrap_or_else(|| "{\"analyses\":[]}".into());
    let manifest = read_zip_member(path, "manifest.json").unwrap_or_else(|| "{}".into());
    let manifest_v: serde_json::Value = serde_json::from_str(&manifest).unwrap_or_default();
    let archive_version = manifest_v
        .get("jaspArchiveVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let _ = archive_version;

    let mut entries = Vec::new();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&analyses) {
        if let Some(arr) = v.get("analyses").and_then(|a| a.as_array()) {
            for a in arr {
                let id = a.get("id").and_then(|x| x.as_i64()).unwrap_or(0);
                let name = a
                    .get("name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let dynm = a.get("dynamicModule");
                let module = dynm
                    .and_then(|d| d.get("moduleName"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let dataset = dynm
                    .and_then(|d| d.get("datasetId"))
                    .and_then(|x| x.as_i64())
                    .unwrap_or(1);
                entries.push(AnalysisEntry {
                    id,
                    name,
                    module,
                    dataset,
                });
            }
        }
    }
    entries
}

// ── FFI write helpers ─────────────────────────────────────────────

unsafe fn write_f64_col(
    chunk: duckdb_data_chunk,
    col: usize,
    data: &[f64],
    nulls: &[bool],
    n: usize,
) {
    let vec = duckdb_data_chunk_get_vector(chunk, col as idx_t);
    let ptr = duckdb_vector_get_data(vec).cast::<f64>();
    let validity = duckdb_vector_get_validity(vec);
    let has_null = nulls.iter().take(n).any(|&x| x);
    for (i, &v) in data.iter().enumerate().take(n) {
        *ptr.add(i) = v;
        if has_null && nulls[i] {
            duckdb_validity_set_row_invalid(validity, i as idx_t);
        }
    }
}

unsafe fn write_varchar_col(
    chunk: duckdb_data_chunk,
    col: usize,
    data: &[&str],
    nulls: &[bool],
    n: usize,
) {
    let vec = duckdb_data_chunk_get_vector(chunk, col as idx_t);
    let validity = duckdb_vector_get_validity(vec);
    let has_null = nulls.iter().take(n).any(|&x| x);
    for (i, s) in data.iter().enumerate().take(n) {
        duckdb_vector_assign_string_element_len(
            vec,
            i as idx_t,
            s.as_ptr().cast::<std::os::raw::c_char>(),
            s.len() as idx_t,
        );
        if has_null && nulls[i] {
            duckdb_validity_set_row_invalid(validity, i as idx_t);
        }
    }
}

unsafe fn write_i64_col(
    chunk: duckdb_data_chunk,
    col: usize,
    data: &[i64],
    n: usize,
) {
    let vec = duckdb_data_chunk_get_vector(chunk, col as idx_t);
    let ptr = duckdb_vector_get_data(vec).cast::<i64>();
    for (i, &v) in data.iter().enumerate().take(n) {
        *ptr.add(i) = v;
    }
}

// ── 1. read_jasp ──────────────────────────────────────────────────

struct ReadState {
    data: JaspData,
    offset: usize,
    batch: usize,
}

const BATCH: usize = 2048;

fn build_read_jasp() -> Result<TableFunctionBuilder, ExtensionError> {
    TableFunctionBuilder::new("read_jasp")
        .param(TypeId::Varchar)
        .named_param("dataset", TypeId::BigInt)
        .with_state::<ReadState, _>(|bind| {
            let path = unsafe { bind.get_parameter_value(0) }
                .as_str_or("")
                .to_string();
            if path.is_empty() {
                return Err(ExtensionError::new(
                    "read_jasp: missing path (read_jasp('file.jasp') or read_jasp('file.jasp', dataset := 1))",
                ));
            }
            let dataset =
                unsafe { bind.get_named_parameter_value("dataset") }.as_i64_or(1);
            let data = parse_jasp(&path, dataset)
                .map_err(|e| ExtensionError::new(&e))?;
            for c in &data.columns {
                bind.add_result_column(&c.name, c.role.type_id());
            }
            Ok(ReadState {
                data,
                offset: 0,
                batch: BATCH,
            })
        })
        .scan(|state, chunk| {
            if state.offset >= state.data.rows.len() {
                unsafe { duckdb_data_chunk_set_size(chunk.as_raw(), 0) };
                return Ok(());
            }
            let n = state
                .batch
                .min(state.data.rows.len() - state.offset);
            let ncols = state.data.columns.len();
            let raw = unsafe { chunk.as_raw() };
            for col in 0..ncols {
                let role = state.data.columns[col].role;
                let mut vals: Vec<f64> = Vec::with_capacity(n);
                let mut strs: Vec<String> = Vec::with_capacity(n);
                let mut nulls: Vec<bool> = Vec::with_capacity(n);
                for r in 0..n {
                    match &state.data.rows[state.offset + r][col] {
                        Cell::F64(v) => {
                            vals.push(*v);
                            nulls.push(false);
                        }
                        Cell::Str(s) => {
                            strs.push(s.clone());
                            nulls.push(false);
                        }
                        Cell::Null => {
                            if matches!(role, Role::Scale | Role::Ordinal) {
                                vals.push(f64::NAN);
                            } else {
                                strs.push(String::new());
                            }
                            nulls.push(true);
                        }
                    }
                }
                if matches!(role, Role::Scale | Role::Ordinal) {
                    unsafe { write_f64_col(raw, col, &vals, &nulls, n) };
                } else {
                    let refs: Vec<&str> = strs.iter().map(|s| s.as_str()).collect();
                    unsafe { write_varchar_col(raw, col, &refs, &nulls, n) };
                }
            }
            unsafe { duckdb_data_chunk_set_size(raw, n as idx_t) };
            state.offset += n;
            Ok(())
        })
        .build()
}

// ── 2. read_jasp_columns ──────────────────────────────────────────

struct ColsState {
    columns: Vec<JaspColumn>,
    offset: usize,
    batch: usize,
}

fn build_read_jasp_columns() -> Result<TableFunctionBuilder, ExtensionError> {
    TableFunctionBuilder::new("read_jasp_columns")
        .param(TypeId::Varchar)
        .named_param("dataset", TypeId::BigInt)
        .with_state::<ColsState, _>(|bind| {
            let path = unsafe { bind.get_parameter_value(0) }
                .as_str_or("")
                .to_string();
            if path.is_empty() {
                return Err(ExtensionError::new(
                    "read_jasp_columns: missing path",
                ));
            }
            let dataset =
                unsafe { bind.get_named_parameter_value("dataset") }.as_i64_or(1);
            let data = parse_jasp(&path, dataset)
                .map_err(|e| ExtensionError::new(&e))?;
            bind.add_result_column("column_name", TypeId::Varchar);
            bind.add_result_column("role", TypeId::Varchar);
            bind.add_result_column("column_id", TypeId::BigInt);
            Ok(ColsState {
                columns: data.columns,
                offset: 0,
                batch: BATCH,
            })
        })
        .scan(|state, chunk| {
            if state.offset >= state.columns.len() {
                unsafe { duckdb_data_chunk_set_size(chunk.as_raw(), 0) };
                return Ok(());
            }
            let n = state
                .batch
                .min(state.columns.len() - state.offset);
            let raw = unsafe { chunk.as_raw() };
            let mut names: Vec<String> = Vec::with_capacity(n);
            let mut roles: Vec<String> = Vec::with_capacity(n);
            let mut ids: Vec<i64> = Vec::with_capacity(n);
            let mut nulls: Vec<bool> = vec![false; n];
            for r in 0..n {
                let c = &state.columns[state.offset + r];
                names.push(c.name.clone());
                roles.push(c.role.as_str().to_string());
                ids.push(c.id);
            }
            let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let role_refs: Vec<&str> = roles.iter().map(|s| s.as_str()).collect();
            unsafe { write_varchar_col(raw, 0, &name_refs, &nulls, n) };
            unsafe { write_varchar_col(raw, 1, &role_refs, &nulls, n) };
            unsafe { write_i64_col(raw, 2, &ids, n) };
            unsafe { duckdb_data_chunk_set_size(raw, n as idx_t) };
            state.offset += n;
            Ok(())
        })
        .build()
}

// ── 3. read_jasp_analyses ─────────────────────────────────────────

struct AnalysesState {
    entries: Vec<AnalysisEntry>,
    offset: usize,
    batch: usize,
}

fn build_read_jasp_analyses() -> Result<TableFunctionBuilder, ExtensionError> {
    TableFunctionBuilder::new("read_jasp_analyses")
        .param(TypeId::Varchar)
        .with_state::<AnalysesState, _>(|bind| {
            let path = unsafe { bind.get_parameter_value(0) }
                .as_str_or("")
                .to_string();
            if path.is_empty() {
                return Err(ExtensionError::new(
                    "read_jasp_analyses: missing path",
                ));
            }
            let entries = parse_analyses(&path);
            bind.add_result_column("analysis_id", TypeId::BigInt);
            bind.add_result_column("analysis_name", TypeId::Varchar);
            bind.add_result_column("module", TypeId::Varchar);
            bind.add_result_column("dataset", TypeId::BigInt);
            Ok(AnalysesState {
                entries,
                offset: 0,
                batch: BATCH,
            })
        })
        .scan(|state, chunk| {
            if state.offset >= state.entries.len() {
                unsafe { duckdb_data_chunk_set_size(chunk.as_raw(), 0) };
                return Ok(());
            }
            let n = state
                .batch
                .min(state.entries.len() - state.offset);
            let raw = unsafe { chunk.as_raw() };
            let mut nulls: Vec<bool> = vec![false; n];
            let mut ids: Vec<i64> = Vec::with_capacity(n);
            let mut names: Vec<String> = Vec::with_capacity(n);
            let mut mods: Vec<String> = Vec::with_capacity(n);
            let mut ds: Vec<i64> = Vec::with_capacity(n);
            for r in 0..n {
                let e = &state.entries[state.offset + r];
                ids.push(e.id);
                names.push(e.name.clone());
                mods.push(e.module.clone());
                ds.push(e.dataset);
            }
            let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let mod_refs: Vec<&str> = mods.iter().map(|s| s.as_str()).collect();
            unsafe { write_i64_col(raw, 0, &ids, n) };
            unsafe { write_varchar_col(raw, 1, &name_refs, &nulls, n) };
            unsafe { write_varchar_col(raw, 2, &mod_refs, &nulls, n) };
            unsafe { write_i64_col(raw, 3, &ds, n) };
            unsafe { duckdb_data_chunk_set_size(raw, n as idx_t) };
            state.offset += n;
            Ok(())
        })
        .build()
}

// ── entrypoint ────────────────────────────────────────────────────

fn register_all(reg: &impl Registrar) -> Result<(), ExtensionError> {
    macro_rules! reg_table {
        ($builder:expr) => {
            unsafe { reg.register_table($builder?) }?
        };
    }
    reg_table!(build_read_jasp());
    reg_table!(build_read_jasp_columns());
    reg_table!(build_read_jasp_analyses());
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn jasp_ext_init_c_api(
    info: duckdb_extension_info,
    access: *const duckdb_extension_access,
) -> bool {
    unsafe {
        init_extension_v2(info, access, quack_rs::DUCKDB_API_VERSION, |con| register_all(con))
    }
}
