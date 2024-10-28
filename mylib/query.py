import sqlite3
from tabulate import tabulate

def create_table():
    """Create the Nutrition table if it doesn't already exist"""
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Nutrition (
            ID INTEGER PRIMARY KEY,
            cancer TEXT,
            diabetes TEXT,
            heart_disease TEXT,
            EGGSFREQ INTEGER,
            GREENSALADFREQ INTEGER,
            FRIESFREQ INTEGER,
            MILKFREQ INTEGER,
            SODAFREQ INTEGER,
            COFFEEFREQ INTEGER,
            CAKESFREQ INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print("Table created successfully.")


def insert_data():
    """Insert sample data into the Nutrition table"""
    print('Inserting Data')
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    # Inserting sample rows into the Nutrition table
    cursor.execute('''
        INSERT INTO Nutrition (
                   ID, cancer, diabetes, heart_disease, 
                   EGGSFREQ, GREENSALADFREQ, FRIESFREQ, 
                   MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ)
        VALUES (1, "Yes", "No", "No", 2, 5, 3, 4, 1, 2, 3)
    ''')
    cursor.execute('''
        INSERT INTO Nutrition (
                   ID, cancer, diabetes, heart_disease, 
                   EGGSFREQ, GREENSALADFREQ, FRIESFREQ, 
                   MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ)
        VALUES (2, "No", "Yes", "Yes", 4, 3, 2, 5, 2, 4, 1)
    ''')
    conn.commit()
    conn.close()
    print("Sample data inserted successfully.")


def read_data():
    """Read and query the database for the top 5 rows of the Nutrition table"""
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Nutrition LIMIT 5")
    rows = cursor.fetchall()
    print("Top 5 rows of the Nutrition table:")
    for row in rows:
        print(row)
    conn.close()
    print()


def update_data():
    """Update a record in the Nutrition table"""
    print('Updating ID 1 to 6 Eggs')
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE Nutrition
        SET EGGSFREQ = 6, cancer = "No"
        WHERE ID = 1
    ''')
    conn.commit()
    conn.close()
    print("Record updated successfully.")


def delete_data():
    """Delete a record from the Nutrition table"""
    print('Deleting Data')
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM Nutrition
        WHERE ID = 2
    ''')
    conn.commit()
    conn.close()
    print("Record deleted successfully.")


def run_crud_operations():
    """Run all CRUD operations to demonstrate their functionality"""
    create_table()
    read_data()

    insert_data()
    read_data()

    update_data()
    read_data()

    delete_data()
    read_data()

def query_frequent_soda():
    """Retrieve and print all individuals who frequently consume soda"""
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT ID, SODAFREQ, EGGSFREQ, FRIESFREQ
        FROM Nutrition
        WHERE SODAFREQ > 3
        LIMIT 5
    ''')

    rows = cursor.fetchall()
    headers = ["ID", "SODAFREQ", "EGGSFREQ", "FRIESFREQ"]
    print(tabulate(rows, headers, tablefmt="grid"))

    conn.commit()
    conn.close()
    print("Frequent soda drinkers queried successfully.\n")



def query_heart_disease():
    """Retrieve and print individuals with heart disease and their food consumption"""
    conn = sqlite3.connect("Nutrition.db")
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT ID, EGGSFREQ, GREENSALADFREQ, FRIESFREQ, SODAFREQ
        FROM Nutrition
        WHERE heart_disease = "Yes"
        LIMIT 5
    ''')
    
    rows = cursor.fetchall()
    headers = ["ID", "EGGSFREQ", "GREENSALADFREQ", "FRIESFREQ", "SODAFREQ"]
    print(tabulate(rows, headers, tablefmt="grid"))

    conn.commit()
    conn.close()
    print("Heart disease queried successfully.\n")