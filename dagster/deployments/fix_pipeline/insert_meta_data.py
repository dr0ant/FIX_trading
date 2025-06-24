from dagster import op, In

@op(ins={"file_info_list": In(list)})
def insert_processed_info(context, file_info_list):
    import psycopg2

    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=localhost")
    cur = conn.cursor()

    for file_info in file_info_list:
        cur.execute("""
            INSERT INTO processed_files (file_name, file_source, process_date, execution_time, nb_lines)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (file_name) DO NOTHING
        """, (
            file_info["file_name"],
            file_info["file_source"],
            file_info["process_date"],
            file_info["execution_time"],
            file_info["nb_lines"]
        ))

    conn.commit()
    cur.close()
    conn.close()
