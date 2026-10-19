-- Test queries for the jasp_ext extension.
-- Run with a dev DuckDB CLI that allows unsigned extensions:
--   duckdb -unsigned -f test/jasp_basic.sql
--
-- Replace JASP_FILE with a real .jasp path.

LOAD 'jasp_ext.duckdb_extension';

-- 1. column metadata
SELECT column_name, role, column_id
FROM read_jasp_columns('/mnt/d/dev/JaspSource/jasp-data-library/Album Sales/Album Sales.jasp')
ORDER BY column_id;

-- 2. data (default dataset = 1)
SELECT * FROM read_jasp('/mnt/d/dev/JaspSource/jasp-data-library/Album Sales/Album Sales.jasp') LIMIT 5;

-- 3. row count
SELECT count(*) AS n
FROM read_jasp('/mnt/d/dev/JaspSource/jasp-data-library/Album Sales/Album Sales.jasp');

-- 4. analysis registry
SELECT analysis_id, analysis_name, module
FROM read_jasp_analyses('/mnt/d/dev/JaspSource/jasp-data-library/Album Sales/Album Sales.jasp')
ORDER BY analysis_id;

-- 5. a quick aggregation over JASP data
SELECT round(avg(sales), 2) AS avg_sales,
       round(max(sales), 2) AS max_sales,
       round(min(sales), 2) AS min_sales
FROM read_jasp('/mnt/d/dev/JaspSource/jasp-data-library/Album Sales/Album Sales.jasp');

-- 6. categorical (nominal) column — label mapping
SELECT * FROM read_jasp('/mnt/d/dev/JaspSource/jasp-data-library/Age and Fatigue/Age and Fatigue.jasp') LIMIT 3;
