import psycopg2

conn = psycopg2.connect(database="your_db", user="your_user", password="your_pass", host="your_host", port="your_port")
cur = conn.cursor()

cur.execute("SELECT name FROM DBS")
databases = cur.fetchall()

for db in databases:
    db_name = db[0]
    print(f"Running for DB: {db_name}")
    
    cur.execute(f"""
        SELECT 
            t."TBL_ID",
            d."NAME" as "DB_NAME",
            t."TBL_NAME",
            split_part("PART_NAME", '=', 1),
            b."PKEY_NAME"
        FROM "DBS" d
        JOIN "TBLS" t ON d."DB_ID" = t."DB_ID"
        JOIN "PARTITIONS" a ON t."TBL_ID" = a."TBL_ID"
        LEFT JOIN "PARTITION_KEYS" b ON a."TBL_ID" = b."TBL_ID"
        WHERE split_part("PART_NAME", '=', 1) != b."PKEY_NAME"
        AND d."NAME" = %s
    """, (db_name,))
    
    rows = cur.fetchall()
    for row in rows:
        print(row)

cur.close()
conn.close()