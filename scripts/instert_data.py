import random
import datetime
from db_utils import get_db_connection

def generate_synthetic_data(num_rows=100):
    """Generate synthetic data for demonstration."""
    categories = ["Oil", "Gas", "Electricity", "Renewables"]
    data = []
    start_date = datetime.date(2024, 1, 1)
    for i in range(num_rows):
        category = random.choice(categories)
        value = random.randint(50, 300)
        date = start_date + datetime.timedelta(days=i)
        data.append((category, value, date))
    return data

def insert_synthetic_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Make sure the table exists (create if not)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_table_synthetic (
            id SERIAL PRIMARY KEY,
            category VARCHAR(50),
            value INT,
            reading_date DATE
        );
    """)

    query = """
    INSERT INTO test_table_synthetic (category, value, reading_date)
    VALUES (%s, %s, %s);
    """

    synthetic_rows = generate_synthetic_data(50)
    for row in synthetic_rows:
        cursor.execute(query, row)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insert_synthetic_data()
