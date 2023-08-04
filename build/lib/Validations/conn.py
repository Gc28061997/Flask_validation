import pyodbc
server = 'sql-server-validation.database.windows.net'
database = 'db_validate'
username = 'sql_server'
password = 'valid@123'

connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(connection_string)


cursor = conn.cursor()



# Execute an SQL query
cursor.execute('SELECT * FROM User_Data')


rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.close()
conn.close()

