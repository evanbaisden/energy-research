import psycopg2

def get_db_connection():
    """Return a new database connection."""
    conn = psycopg2.connect(
        dbname="energy-data",
        user="postgres",
        password="@1Evanb55",
        host="localhost",
        port="5433"
    )
    return conn
