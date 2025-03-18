import pyodbc
import pandas as pd

# DATOS DE CONEXION



try:
    connection = pyodbc.connect('DRIVER={SQL SERVER};SERVER=10.1.21.237\SQLEXPRESS;DATABASE=CheckPostingDB;UID=db_user;PWD=Or4cl3#1')
    print("Conexion Exitosa")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM CHECKS;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
except Exception as ex:
    print(ex)









