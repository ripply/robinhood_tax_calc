import csv
import uuid
from datetime import datetime
from .database import establish_connection

def create_table(cursor):
    """Create a new table for transactions."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            activity_date TEXT,
            process_date TEXT,
            settle_date TEXT,
            instrument TEXT,
            description TEXT,
            trans_code TEXT,
            quantity TEXT,
            price REAL,
            amount REAL
        )
    """)

def create_splits_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS splits (
            date TEXT,
            instrument TEXT,
            description TEXT,
            trans_code TEXT,
            from_factor REAL,
            to_factor REAL
        )
    """)

def convert_date_format(date_str):
    """Convert date format from MM/DD/YY to ISO 8601."""
    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
    return date_obj.isoformat()

def convert_money_format(money_str):
    """Convert a money format string to a float."""
    if not money_str:  # if money_str is empty, return 0
        return 0.0
    if '(' in money_str and ')' in money_str:  # Negative number
        result = float(money_str.replace('$', '').replace('(', '').replace(')', '').replace(',', ''))
        return -result
    else:  # Positive number
        result = float(money_str.replace('$', '').replace(',', ''))
        return result


def insert_into_db(cursor, row):
    """Insert a row into the transactions table."""
    cursor.execute("""
        INSERT INTO transactions (activity_date, process_date, settle_date, instrument, description, trans_code, quantity, price, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)

def insert_splits_into_db(cursor, row):
    """Insert a row into the splits table."""
    cursor.execute("""
        INSERT INTO splits (date, instrument, description, trans_code, from_factor, to_factor)
        VALUES (?, ?, ?, ?, ?, ?)
    """, row)

def read_csv_and_insert_into_db(cursor, csv_file):
    """Read a CSV file and insert the data into an SQLite database."""
    create_table(cursor)
    inserts = 0

    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip the header row
        for row in reader:
            if not any(field.strip() for field in row):
                break
            # Convert date columns
            for i in range(3):
                row[i] = convert_date_format(row[i])
            # Convert price and amount columns
            row[7] = convert_money_format(row[7])
            row[8] = convert_money_format(row[8])
            insert_into_db(cursor, row)
            inserts += 1

    print(f'Inserted {inserts} rows')

def read_csv_and_insert_splits_into_db(cursor, csv_file):
    """Read splits CSV file and insert the data into an SQLite database."""
    create_splits_table(cursor)
    inserts = 0

    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip the header row
        for row in reader:
            if not any(field.strip() for field in row):
                break
            # Convert date columns
            row[0] = convert_date_format(row[0])
            insert_into_splits_db(cursor, row)
            inserts += 1

    print(f'Inserted splits {inserts} rows')


if __name__ == "__main__":
    conn, cursor = establish_connection('transactions.sqlite')
    read_csv_and_insert_into_db(cursor, './data/transactions.csv')
    read_csv_and_insert_splits_into_db(cursor, './data/splits.csv')
    conn.commit()
    conn.close()
