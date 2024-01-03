SELECT
    IFNULL(B.engine, 'Total') "Storage Engine",
    CONCAT(LPAD(REPLACE(FORMAT(B.DSize / POWER(1024, pw), 3), ',', ''), 17, ' '), ' ', SUBSTR(' KMGTP', pw + 1, 1), 'B') "Data Size",
    CONCAT(LPAD(REPLACE( FORMAT(B.ISize / POWER(1024, pw), 3), ',', ''), 17, ' '), ' ', SUBSTR(' KMGTP', pw + 1, 1), 'B') "Index Size",
    CONCAT(LPAD(REPLACE( FORMAT(B.TSize / POWER(1024, pw), 3), ',', ''), 17, ' '), ' ', SUBSTR(' KMGTP', pw + 1, 1), 'B') "Table Size" 
FROM
    (SELECT
            engine,
            SUM(data_length) DSize,
            SUM(index_length) ISize,
            SUM(data_length + index_length) TSize 
        FROM
            information_schema.tables 
        WHERE
            table_schema NOT IN ('mysql', 'information_schema', 'performance_schema')
            AND engine IS NOT NULL 
        GROUP BY engine WITH ROLLUP
    ) B,
    (SELECT 3 pw) A 
ORDER BY TSize;
