import os
from pathlib import Path
import psycopg2


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = PROJECT_DIR / "data"
DEFAULT_SQL_FILE = PROJECT_DIR / "postgres" / "create_mock_data_table.sql"


def execute_sql_file(cursor, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        sql = file.read()

    cursor.execute(sql)


def load_csv_files(cursor, data_dir):
    data_dir = Path(data_dir)
    csv_files = sorted(data_dir.glob("*.csv"), key=lambda path: path.name)
    if not csv_files:
        raise FileNotFoundError(f"CSV files not found in {data_dir}")

    copy_query = """
        COPY mock_data
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE, NULL '')
    """

    for csv_file in csv_files:
        with csv_file.open("r", encoding="utf-8", newline="") as file:
            cursor.copy_expert(copy_query, file)
        print(f"Loaded {csv_file.name}")

    return len(csv_files)


def main():
    conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "db"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "12345")
    )
    cursor = conn.cursor()

    sql_file = os.getenv("SQL_FILE", str(DEFAULT_SQL_FILE))
    data_dir = os.getenv("DATA_DIR", str(DEFAULT_DATA_DIR))

    execute_sql_file(cursor, sql_file)
    conn.commit()
    print("Mock data table created.")

    loaded_files = load_csv_files(cursor, data_dir)
    cursor.execute("SELECT COUNT(*) FROM mock_data")
    rows_count = cursor.fetchone()[0]
    conn.commit()
    print(f"Loaded {rows_count} rows from {loaded_files} CSV files")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
