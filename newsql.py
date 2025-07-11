SELECT 
    t."TBL_ID",
    d."NAME" AS "DB_NAME",
    t."TBL_NAME",
    split_part(a."PART_NAME", '=', 1) AS part_key,
    b."PKEY_NAME"
FROM "DBS" d
JOIN "TBLS" t ON d."DB_ID" = t."DB_ID"
JOIN "PARTITIONS" a ON t."TBL_ID" = a."TBL_ID"
LEFT JOIN "PARTITION_KEYS" b ON a."TBL_ID" = b."TBL_ID"
WHERE split_part(a."PART_NAME", '=', 1) != b."PKEY_NAME";