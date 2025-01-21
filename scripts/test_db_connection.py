# test_db_connection.py
from db_utils import get_db_connection

def test_connection():
    conn = get_db_connection()
    cur = conn.cursor()
    # A simple query to check the version
    cur.execute("SELECT version();")
    result = cur.fetchone()
    print("PostgreSQL version:", result[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    test_connection()
