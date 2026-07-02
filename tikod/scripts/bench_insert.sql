\set ON_ERROR_STOP on
\timing on

\set row_count 250000

DROP TABLE IF EXISTS bench;
CREATE TABLE bench (
    id      bigint PRIMARY KEY,
    val     double precision,
    tag     text,
    payload text
);

\echo '=== bulk INSERT: ' :row_count ' rows, ~1KB each ==='
INSERT INTO bench (id, val, tag, payload)
SELECT g, random(), md5(g::text), repeat(md5(g::text), 31)
FROM generate_series(1, :row_count) g;

SELECT count(*) AS rows,
       pg_size_pretty(pg_total_relation_size('bench')) AS total_size;

\echo '=== CHECKPOINT: flush dirty buffers to remote storage ==='
CHECKPOINT;

\echo '=== full table scan (warm cache) ==='
SELECT count(*) FROM bench;

DROP TABLE bench;
\echo '=== done ==='
