import sqlite3

def establish_connection(db_file):
    """Establish and return a database connection."""
    conn = sqlite3.connect(db_file)
    return conn, conn.cursor()
