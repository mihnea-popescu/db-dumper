import pyodbc
import pandas as pd
import os
import hashlib
import csv

"""
This script dumps as CSVs some tables from a database and obfuscates (hashes) some columns in order to
hide their content. This can be useful for building ML Datasets.

WARNING: Make sure to make a backup of the database, do not attempt it on production or any important databases!
"""

# Make sure to convert MySQL DATE columns to TIMESTAMP columns, otherwise pd.read_sql_query will throw an error if it finds a NULL column of type DATE
CONNECTION_DETAILS = 'DRIVER={MySQL ODBC 8.0 Unicode Driver};UID=username;Password=pass;Server=localhost;Database=main_db;Port=3306;String Types=Unicode'

# Each key of the the tables dictionary is a table that is going to be dumped
# And each value in the tables dictionary is a list of the columns that are going to be obfuscated (hashed).
tables = {
    'candidates': ['email', 'phone', 'name', 'password'],
    'cities': [],
    'counties': [],
    'companies': ['name', 'address', 'description'],
    'users': ['email', 'phone', 'password'] # email, phone and password column values in the users table are going to be hashed in the final CSV file.
}

def main():
    conn = pyodbc.connect(CONNECTION_DETAILS)
    print('Connected to database, starting dump')

    # clear folder of previous CSV file
    if not os.path.exists('db_export'):
        os.makedirs('db_export')
    
    for file in os.listdir('db_export'):
        os.remove(os.getcwd() + "/db_export/" + file)
        
    # process tables
    for table in tables:
        print('------------------')
        print('Processing table: {}'.format(table))

        query = "SELECT * FROM {}".format(table)
        df = pd.read_sql_query(query, conn)
        
        
        # Obfuscate (hash) columns
        for column in tables[table]:
            df[column] = df[column].astype(str)
            df[column] = df[column].apply(
                lambda x:
                    hashlib.sha3_256(x.encode()).hexdigest()
            )
        
        # Export to CSV
        df.to_csv(os.getcwd() + "/db_export/" + table + ".csv", index = False, escapechar='\\')
    
    
if __name__ == "__main__":
    main()