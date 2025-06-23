import os
import re
import psycopg2
from datetime import datetime
import json  # Add this import at the top


# PostgreSQL connection params
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "fix_db")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")

FIX_LOG_FILE = "fix_logs.txt"

# Match FIX lines and extract timestamp, source, and raw FIX payload
LINE_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[(?P<system>[^\]]+)\] .* : (?P<fix>8=.*)"
)

def parse_fix_message(fix_string: str):
    print ({tag: value for tag, value in (field.split("=", 1) for field in fix_string.strip().split("|") if "=" in field)})
    return {tag: value for tag, value in (field.split("=", 1) for field in fix_string.strip().split("|") if "=" in field)}

def ensure_table_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fix_messages (
            id SERIAL PRIMARY KEY,
            log_time TIMESTAMP,
            system TEXT,
            msg_type TEXT,
            raw_fix TEXT,
            fix_tags JSONB
        );
    """)


def insert_fix_message(cursor, log_time, system, msg_type, raw_fix, fix_tags):
    cursor.execute("""
        INSERT INTO fix_messages (log_time, system, msg_type, raw_fix, fix_tags)
        VALUES (%s, %s, %s, %s, %s);
    """, (log_time, system, msg_type, raw_fix, json.dumps(fix_tags)))  # Convert fix_tags to JSON string

def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
    ensure_table_exists(cursor)

    with open(FIX_LOG_FILE, "r") as f:
        for line in f:
            match = LINE_PATTERN.match(line)
            if not match:
                continue

            ts = datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
            system = match.group("system")
            fix_raw = match.group("fix")
            fix_tags = parse_fix_message(fix_raw)
            msg_type = fix_tags.get("35", "UNKNOWN")
            insert_fix_message(cursor, ts, system, msg_type, fix_raw, fix_tags)
            print(ts, system, msg_type, fix_raw, fix_tags)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
