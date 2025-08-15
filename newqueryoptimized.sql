-- =====================================================================
-- Hive Beeline Optimized Script: Column + Partition Counts by Table
-- Works in CDP 7.1.9 on Hive/Tez
-- =====================================================================

-- Step 0: Tez performance settings (tune memory as per your env)
SET hive.execution.engine=tez;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET tez.am.resource.memory.mb=4096;
SET hive.tez.container.size=4096;
SET tez.grouping.min-size=1073741824;  -- 1GB
SET tez.grouping.max-size=2147483648;  -- 2GB

-- =====================================================================
-- Step 1: Column counts per table
-- =====================================================================
DROP TABLE IF EXISTS col_counts_temp;
CREATE TEMPORARY TABLE col_counts_temp AS
SELECT 
    t.TBL_ID,
    COUNT(*) AS colCount
FROM DBS d
JOIN TBLS t ON d.DB_ID = t.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
-- Optional filter to reduce load:
-- WHERE d.NAME IN ('db1','db2')
GROUP BY t.TBL_ID;

-- =====================================================================
-- Step 2: Partition counts per table
-- =====================================================================
DROP TABLE IF EXISTS part_counts_temp;
CREATE TEMPORARY TABLE part_counts_temp AS
SELECT 
    t.TBL_ID,
    COUNT(p.PART_ID) AS partCount
FROM TBLS t
LEFT JOIN PARTITIONS p 
    ON t.TBL_ID = p.TBL_ID
-- Optional filter:
-- WHERE t.DB_ID IN (SELECT DB_ID FROM DBS WHERE NAME IN ('db1','db2'))
GROUP BY t.TBL_ID;

-- =====================================================================
-- Step 3: Final join to get DB name, table name, column count, partition count
-- =====================================================================
SELECT 
    d.NAME AS DB_NAME,
    t.TBL_NAME,
    cc.colCount,
    pc.partCount
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
LEFT JOIN col_counts_temp cc ON t.TBL_ID = cc.TBL_ID
LEFT JOIN part_counts_temp pc ON t.TBL_ID = pc.TBL_ID
ORDER BY d.NAME, t.TBL_NAME;

-- =====================================================================
-- Cleanup (optional in case TEMPORARY not supported)
-- =====================================================================
DROP TABLE IF EXISTS col_counts_temp;
DROP TABLE IF EXISTS part_counts_temp;