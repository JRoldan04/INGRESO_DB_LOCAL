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
    cursor_hana.execute('TRUNCATE TABLE  "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DATA"')
    print("✅ Truncate completado en T_SIMP_CAPS_FCR_INVOICE_DATA.")
    
except Exception as e:
    print(f"❌ Error conectando a SAP HANA: {e}")
    exit()

query_select = """
    SELECT PCWSID, FCRInvNumber, MicrosChkNum, InvoiceType, InSARMode, CustomerID, InvoiceStatus,
           MicrosBsnzDate, FCRBsnzDate, Subtotal1, Subtotal2, Subtotal3, Subtotal4, Subtotal5, 
           Subtotal6, Subtotal7, Subtotal8, Subtotal9, Subtotal10, Subtotal11, Subtotal12, 
           TaxTtl1, TaxTtl2, TaxTtl3, TaxTtl4, TaxTtl5, TaxTtl6, TaxTtl7, TaxTtl8, ExtraField1, 
           ExtraField2, ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, 
           PropertyID, FCRID, StoreID, SerialID, DocumentType, DataType, SysDocID, FiscalRef, Subtotal13, 
           Subtotal14, ExtraField9, ExtraField10, ExtraField11, ExtraField12, ExtraField13, ExtraField14, 
           ExtraField15, ExtraField16, ExtraDate1, ExtraDate2, ExtraDate3, ExtraDate4, ExtraDate5, 
           PurgeStatus, HierStrucID, RevenueCenterID, Guid, ReplicationDest, ReplicationStatus, 
           ReplicationError, CPPurgeDate, RepPurgeDate, UpdateTime, TaxTtl9, TaxTtl10, TaxTtl11, TaxTtl12, 
           TaxTtl13, TaxTtl14, TaxTtl15, TaxTtl16, CheckGuid, FCRInvoiceDataID, FCRJSONEXP 
    FROM dbo.FCR_INVOICE_DATA
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

         # Extraer el valor de configuración específico para este ID
        cursor_hana.execute(f'SELECT NOMBRE_CAPS FROM "COLSUBSIDIO_HDI"."T_SIMP_IP_CAPS" WHERE SERVER = ?', (SERVER,))
        resultado = cursor_hana.fetchone()
        
        CONFIG_VALUE = resultado[0] if resultado else "ValorPorDefecto"
        print(f"📌 Valor de configuración para ID {SERVER}: {CONFIG_VALUE}")

        if not data:
            print(f"⚠️ No hay registros en {SERVER}.")
        else:
            df = pd.DataFrame.from_records(data, columns=columns)
            print(f"✅ {len(df)} registros migrados de {SERVER}.")

            columns.append("HOTEL")

            insert_sql = f"""
            INSERT INTO "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DATA" ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in columns])})
            """
            
            values = [tuple(None if pd.isna(row[col]) else row[col] for col in columns [:-1]) + (CONFIG_VALUE,) for _, row in df.iterrows()]
            cursor_hana.executemany(insert_sql, values)
            conn_hana.commit()
            print("✅ Migración completada en T_SIMP_CAPS_FCR_INVOICE_DATA.")
        
        conn_sql.close()
    except Exception as e:
        print(f"❌ Error conectando a {SERVER}: {e}")

# Cerrar la conexión a SAP HANA
conn_hana.close()
