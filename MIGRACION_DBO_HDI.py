import pyodbc
import pandas as pd
from hdbcli import dbapi
from datetime import datetime
from sqlalchemy import text

# CAPS con la misma configuración excepto el HOST

servidores = [
    {"host": "10.1.21.236\SQLEXPRESS", "database": "CheckPostingDB", "user": "db_user", "password": "Or4cl3#1"}
    #{"host": "10.1.21.237\SQLEXPRESS", "database": "CheckPostingDB", "user": "db_user", "password": "Or4cl3#1"}
]

for srv in servidores:
    try:
        conn_sql = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={srv['host']};"
            f"DATABASE={srv['database']};"
            f"UID={srv['user']};"
            f"PWD={srv['password']}"
        )
   #     cursor_sql = conn_sql.cursor()
   #    cursor_sql.execute(" SELECT RVCID, PCWSID, FCRID, FCRINVNUMBER, MICROSCHKNUM, INVOICETYPE, INVOICELETTER, MICROSBSNZDATE, FCRBSNZDATE, ITEMINDEX, ITEMTYPE, ITEMOBJNUM, ITEMQTY, ITEMTOTAL, ITEMTAXTOTAL, ITEMTAXRATE, ITEMSTATUS, EXTRAFIELD1, EXTRAFIELD2, EXTRAFIELD3, EXTRAFIELD4, EXTRAFIELD5, EXTRAFIELD6, EXTRAFIELD7, EXTRAFIELD8, PROPERTYID, STOREID, FISCALREF FROM FCR_INVOICE_DETAIL_BP " )  # Consulta de prueba
   #    registro = cursor_sql.fetchall()  #OBTENER TODOS LOS  REGISTROS
   #    if registro:

   #        df = pd.DataFrame.from_records(registro)
   #        #print("Cantidad de filas:", len(registro))
   #      # Mostrar TODOS los registros sin cortar la salida
   #        #print(df.to_string(index=False))  # Muestra todo el DataFrame sin truncar filas ni columnas
   #    else:
   #        print(f"⚠️ No hay registros en {srv['host']}.")

    except Exception as e:
        print(f"❌ Error conectando a {srv['host']}: {e}")
        
        #if not registro:
            #print(f"⚠️ No hay registros en {srv['host']}.")
        #else:
            #df = pd.DataFrame.from_records(registro, columns=columnas)
           # print(f"✅ Conectado a {srv['host']}, Total de registros: {len(df)}")
           #print(df.head())  # Muestra solo los primeros 5 registros
       
        #print("tipo de registro:", type(registro))
        #print("contenido de registro:", registro)
        #registro = tuple("NULL" if x is None else x for x in registro)
        #print("Tipo de result:", type(registro))
        #print("Cantidad de filas:", len(registro))
        #print("Primeros 5 registros:", registro[:5])  # Muestra solo los primeros 5 registros
        #for i, row in enumerate(registro):
             #if row is None:
                #print(f"⚠️ Fila {i+1} es None.")
        #else:
            #print(f"Fila {i+1}: {row}")
        
        #registro = registro.where(pd.notnull(registro), None)
        #print(f"Conectado a {srv['host']}, Total de registros: {cursor_sql.fetchall()[0]}")
        #print(registro[1])
        
    except Exception as e:
        print(f"Error conectando a {srv['host']}: {e}")

 # CONEXION A HDI
        

    try:
       
       conn_hana= dbapi.connect(
       address="548b50f4-1fe9-4725-baea-e9f96bb4f092.hana.prod-us10.hanacloud.ondemand.com",
       port= 443,
       user="DBADMIN",
       password="Analitica2022"
       )
       cursor_hana = conn_hana.cursor()
       cursor_hana.execute('SELECT COUNT(*) FROM "COLSUBSIDIO_HDI"."T_SIMP_IP_CAPS"')  # Consulta de prueba
       print(f"Conectado a  HDI, Total de registros: {cursor_hana.fetchone()[0]}")
    except Exception as e:
       print(f"Error conectando a ['host']: {e}")   
        
      # query = "SELECT RVCID, PCWSID, FCRID, FCRInvNumber, MicrosChkNum, InvoiceType, InvoiceLetter, MicrosBsnzDate, FCRBsnzDate, ItemIndex, ItemType, ItemObjNum, ItemQty, ItemTotal, ItemTaxTotal, ItemTaxRate, ItemStatus, ExtraField1, ExtraField2, ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, PropertyID, StoreID, FiscalRef  FROM FCR_INVOICE_DETAIL_BP"
       #df = pd.read_sql(query, conn_sql)  
       #print(df.head()) 

try:
    cursor_dbo = conn_sql.cursor()
    query_select = """
    SELECT RVCID, PCWSID, FCRID, FCRInvNumber, MicrosChkNum, InvoiceType, InvoiceLetter, 
           MicrosBsnzDate, FCRBsnzDate, ItemIndex, ItemType, ItemObjNum, ItemQty, 
           ItemTotal, ItemTaxTotal, ItemTaxRate, ItemStatus, ExtraField1, ExtraField2, 
           ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, 
           PropertyID, StoreID, FiscalRef
    FROM dbo.FCR_INVOICE_DETAIL_BP
    """
    
    cursor_dbo.execute(query_select)
    data = cursor_dbo.fetchall()
    columns = [desc[0] for desc in cursor_dbo.description]

  
#   for i, row in enumerate(data[:5]):  # Solo las primeras 5 filas
#    print(f"Registro {i+1}: {row}, Tipo: {type(row)}")
#    print(f"Longitud del registro {i+1}: {len(row)}")
#
#    df = pd.DataFrame([list(row) for row in data], columns=columns)
    df = pd.DataFrame([list(row) for row in data], columns=columns)


    print(df)

    for i, row in df.iterrows():
     print(f"Registro {i+1}: FCRInvNumber = {row['FCRInvNumber']}, ItemIndex = {row['ItemIndex']}")

    merge_sql = f"""
    MERGE INTO COLSUBSIDIO_HDI.T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP AS target
    USING (SELECT '{row['FCRInvNumber']}' AS FCRInvNumber, '{row['ItemIndex']}' AS ItemIndex FROM DUMMY) AS source
    ON (target.FCRInvNumber = source.FCRInvNumber AND target.ItemIndex = source.ItemIndex)
    
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(columns)}) 
        VALUES ({", ".join(["?" for _ in columns])})
    """

    # 📌 Manejo de valores `None` (evitar errores con `NULL`)
    values = tuple(None if pd.isna(row[col]) else row[col] for col in columns)

    # 📌 Ejecutar la consulta
    cursor_hana.execute(merge_sql, values)

    # 📌 Confirmar cambios en SAP HANA
    conn_hana.commit()
    print("✅ MERGE completado en SAP HANA.")
    

except Exception as e:
    print(f"❌ Error: {e}")



#   for _, row in df.iterrows():
#       merge_sql = """
#   MERGE INTO COLSUBSIDIO_HDI.T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP AS target
#   USING (SELECT 'santiago' AS FCRInvNumber, 222222 AS ItemIndex FROM DUMMY ) AS source
#   ON (target.FCRInvNumber = source.FCRInvNumber AND target.ItemIndex = source.ItemIndex)
#   WHEN NOT MATCHED THEN
#       INSERT ({})
#       VALUES ({})
#   """.format(
#       ", ".join(columns),  # Columnas de la tabla destino
#       ", ".join(["?" for _ in columns])  # Placeholders para valores
#   )
#
#   # Extraer los valores en el orden correcto
#   values = (row['FCRInvNumber'], row['ItemIndex']) + tuple(row[col] for col in columns)
#
#   # Ejecutar la consulta con parámetros
#   cursor_hana.execute(merge_sql, values)
#
# Confirmar cambios en SAP HANA
#   conn_hana.commit()
#   print("✅ MERGE completado en SAP HANA.")
except Exception as e:
    print(f"❌ Error: {e}")





    