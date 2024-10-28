import sqlite3
import os
import pandas as pd

# load the csv file and insert into a new sqlite3 database
def load(dataset="data/Nutrition.csv"):
    """Transforms and Loads data into the local SQLite3 database"""

    # prints the full working directory and path
    print(f"Current working directory: {os.getcwd()}")
    df = pd.read_csv(dataset)

    # connect to the SQLite database
    conn = sqlite3.connect('Nutrition.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS Nutrition")

    # create table
    c.execute('''
        CREATE TABLE Nutrition (
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

    # insert
    data_to_insert = df.values.tolist()
    c.executemany('''
        INSERT INTO Nutrition (
            ID, cancer, diabetes, heart_disease, 
                  EGGSFREQ, GREENSALADFREQ, FRIESFREQ, 
                  MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_to_insert)

    conn.commit()
    conn.close()

    return "Nutrition.db"