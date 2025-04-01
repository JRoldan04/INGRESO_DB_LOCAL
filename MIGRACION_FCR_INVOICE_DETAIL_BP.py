import pyodbc
import pandas as pd
from hdbcli import dbapi

# Conexión a SAP HANA
try:
    conn_hana = dbapi.connect(
        address="548b50f4-1fe9-4725-baea-e9f96bb4f092.hana.prod-us10.hanacloud.ondemand.com",
        port=443,
        user="DBADMIN",
        password="Analitica2022"
    )
    cursor_hana = conn_hana.cursor()
    cursor_hana.execute('TRUNCATE TABLE  "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP"')
    #print(f"Conectado a HDI, Total de registros: {cursor_hana.fetchone()[0]}")
    print("✅ Truncate completado en T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP.")

except Exception as e:
    print(f"Error conectando a SAP HANA: {e}")

# Lista de servidores SQL Server
servidores = [
    {"host": "10.1.21.236\\SQLEXPRESS", "database": "CheckPostingDB", "user": "db_user", "password": "Or4cl3#1"},
    {"host": "10.1.21.237\\SQLEXPRESS", "database": "CheckPostingDB", "user": "db_user", "password": "Or4cl3#1"}
]

query_select = """
    SELECT RVCID, PCWSID, FCRID, FCRInvNumber, MicrosChkNum, InvoiceType, InvoiceLetter, 
           MicrosBsnzDate, FCRBsnzDate, ItemIndex, ItemType, ItemObjNum, ItemQty, 
           ItemTotal, ItemTaxTotal, ItemTaxRate, ItemStatus, ExtraField1, ExtraField2, 
           ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, 
           PropertyID, StoreID, FiscalRef
    FROM dbo.FCR_INVOICE_DETAIL_BP
"""

for srv in servidores:
    try:
        print(f"🔍 Conectando a {srv['host']}...")

        conn_sql = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={srv['host']};"
            f"DATABASE={srv['database']};"
            f"UID={srv['user']};"
            f"PWD={srv['password']}"
        )
        cursor_sql = conn_sql.cursor()

        cursor_sql.execute(query_select)
        data = cursor_sql.fetchall()
        columns = [desc[0] for desc in cursor_sql.description]

        if not data:
            print(f"⚠️ No hay registros en {srv['host']}.")
        else:
            df = pd.DataFrame.from_records(data, columns=columns)
            print(f"✅ {len(df)} registros migrados de {srv['host']}.")

            # Optimización con executemany para mejorar el rendimiento
            insert_sql = f"""
            INSERT INTO COLSUBSIDIO_HDI.T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in columns])})
            """

            values = [tuple(None if pd.isna(row[col]) else row[col] for col in columns) for _, row in df.iterrows()]
            cursor_hana.executemany(insert_sql, values)

            conn_hana.commit()
            print("✅ Migracion completada en T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP.")

        conn_sql.close()

    except Exception as e:
        print(f"❌ Error conectando a {srv['host']}: {e}")

# Cerrar la conexión a SAP HANA
conn_hana.close()