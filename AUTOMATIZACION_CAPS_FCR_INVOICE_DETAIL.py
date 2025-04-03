import pyodbc
import pandas as pd
from hdbcli import dbapi

# Conexión a SAP HANA para obtener la lista de servidores
try:
    conn_hana = dbapi.connect(
        address="548b50f4-1fe9-4725-baea-e9f96bb4f092.hana.prod-us10.hanacloud.ondemand.com",
        port=443,
        user="DBADMIN",
        password="Analitica2022"
    )
    cursor_hana = conn_hana.cursor()
    
    # Leer servidores desde SAP HANA
    cursor_hana.execute('SELECT SERVER, DATABASE, USUARIO, PASSWORD FROM "COLSUBSIDIO_HDI"."T_SIMP_IP_CAPS"')
    servidores = cursor_hana.fetchall()
    print(f"🔍 {len(servidores)} Servidores encontrados en SAP HANA.")
    
    # Truncar tabla destino antes de la migración
    cursor_hana.execute('TRUNCATE TABLE  "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP"')
    print("✅ Truncate completado en T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP.")
    
except Exception as e:
    print(f"❌ Error conectando a SAP HANA: {e}")
    exit()

query_select = """
    SELECT RVCID, PCWSID, FCRID, FCRInvNumber, MicrosChkNum, InvoiceType, InvoiceLetter, 
           MicrosBsnzDate, FCRBsnzDate, ItemIndex, ItemType, ItemObjNum, ItemQty, 
           ItemTotal, ItemTaxTotal, ItemTaxRate, ItemStatus, ExtraField1, ExtraField2, 
           ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, 
           PropertyID, StoreID, FiscalRef
    FROM dbo.FCR_INVOICE_DETAIL_BP
"""

for SERVER, DATABASE, USUARIO, PASSWORD in servidores:
    try:
        print(f"🔍 Conectando a {SERVER}...")
        conn_sql = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USUARIO};"
            f"PWD={PASSWORD}"
        )
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(query_select)
        data = cursor_sql.fetchall()
        columns = [desc[0] for desc in cursor_sql.description]

        if not data:
            print(f"⚠️ No hay registros en {SERVER}.")
        else:
            df = pd.DataFrame.from_records(data, columns=columns)
            print(f"✅ {len(df)} registros migrados de {SERVER}.")

            insert_sql = f"""
            INSERT INTO "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP" ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in columns])})
            """
            
            values = [tuple(None if pd.isna(row[col]) else row[col] for col in columns) for _, row in df.iterrows()]
            cursor_hana.executemany(insert_sql, values)
            conn_hana.commit()
            print("✅ Migración completada en T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP.")
        
        conn_sql.close()
    except Exception as e:
        print(f"❌ Error conectando a {SERVER}: {e}")

# Cerrar la conexión a SAP HANA
conn_hana.close()
