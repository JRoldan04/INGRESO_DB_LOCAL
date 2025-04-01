import pyodbc
import pandas as pd
from hdbcli import dbapi
from datetime import datetime




# DATOS DE CONEXION

db_ip = "10.1.21.236"
db_ips = ["10.1.21.236\SQLEXPRESS"]
db_user = "db_user"
db_password = "Or4cl3#1"
database_name = "CheckPostingDB"
caps = "RYDPISCWSCAPS"
fecha_carga = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

for ip in db_ips:
    try:
        connection_string = f"DRIVER={{SQL Server}};SERVER={ip};DATABASE={database_name};UID={db_user};PWD={db_password}"
        
        conn = pyodbc.connect(connection_string, timeout=5)
        print(f"Concentado Exitosamente a {ip}")

        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME")
        row = cursor.fetchone()
        print(f"Version de SQL Server en {ip}:{row[0]}")

        cursor.close()
        conn.close

    
    except Exception as e:
        print(f"No se pudo conectar a {ip}:{e}")

        df = pd.read_sql("@@SERVERNAME", conn)
        print(df)
        

    try :
        hana_conn = dbapi.connect(
            address="548b50f4-1fe9-4725-baea-e9f96bb4f092.hana.prod-us10.hanacloud.ondemand.com",
            port = 443,
            user="DBADMIN",
            password="Analitica2022"
        )
        hana_cursor = hana_conn.cursor()
        sql_command = 'INSERT INTO "COLSUBSIDIO_HDI"."T_SIMP_IP_CAPS" (IP , DATA_BASE_NAME, CAPS , USUARIO , PASSWORD , FECHA_CARGA) VALUES (?,?,?,?,?,?)'
        #'INSERT INTO "HDI_DB"."SPFLIGHT::T_SIMP_IP_CAPS" (MANDT, CARRID, PLANETYPE, SNUMBER) VALUES (?,?,?,?)'
        valores = (db_ip,database_name,caps,db_user,db_password,fecha_carga)
        hana_cursor.execute(sql_command,valores)

        # Obtener los resultados
        #rows = hana_cursor.fetchall()

    # Imprimir los registros obtenidos
        #for row in rows:
           # print(row)


        hana_conn.commit()
        print("Registro Insertado Correctamente")

      
        hana_cursor.close()
        hana_conn.close()
        
        
    except dbapi.Error as er:
        print("Conexion Fallida, Existing")
        print(er)
        exit()

    print("Conexion Satisfactoria")

       
          














