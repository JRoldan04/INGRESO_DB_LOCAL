import pyodbc
import pandas as pd
from hdbcli import dbapi
#PRUEBA CORRECTA PERO DE A REGISTRO 31 MARZO
# Conexión a SAP HANA
try:
    conn_hana = dbapi.connect(
        address="548b50f4-1fe9-4725-baea-e9f96bb4f092.hana.prod-us10.hanacloud.ondemand.com",
        port=443,
        user="DBADMIN",
        password="Analitica2022"
    )
    cursor_hana = conn_hana.cursor()
    cursor_hana.execute('SELECT COUNT(*) FROM "COLSUBSIDIO_HDI"."T_SIMP_IP_CAPS"')  # Consulta de prueba
    print(f"Conectado a HDI, Total de registros: {cursor_hana.fetchone()[0]}")

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
            print(f"✅ {len(df)} registros recuperados de {srv['host']}.")

            # ✅ Recorrer cada fila y ejecutar MERGE
            for i, row in df.iterrows():
                print(f"Procesando registro {i+1}: RVCID = {row['RVCID']}, PCWSID = {row['PCWSID']}, FCRID = {row['FCRID']}")

                merge_sql = f"""
                MERGE INTO COLSUBSIDIO_HDI.T_SIMP_CAPS_FCR_INVOICE_DETAIL_BP AS target
                USING (SELECT 
                '{row['RVCID']}' AS RVCID,
                '{row['PCWSID']}' AS PCWSID, 
                '{row['FCRID']}' AS FCRID,
                '{row['FCRInvNumber']}' AS FCRInvNumber, 
                '{row['ItemIndex']}' AS ItemIndex, 
                '{row['MicrosChkNum']}' AS MicrosChkNum, 
                '{row['InvoiceType']}' AS InvoiceType, 
                '{row['InvoiceLetter']}' AS InvoiceLetter, 
                '{row['MicrosBsnzDate']}' AS MicrosBsnzDate, 
                '{row['FCRBsnzDate']}' AS FCRBsnzDate, 
                '{row['ItemType']}' AS ItemType, 
                '{row['ItemObjNum']}' AS ItemObjNum, 
                '{row['ItemQty']}' AS ItemQty, 
                '{row['ItemTotal']}' AS ItemTotal, 
                '{row['ItemTaxTotal']}' AS ItemTaxTotal, 
                '{row['ItemTaxRate']}' AS ItemTaxRate, 
                '{row['ItemStatus']}' AS ItemStatus, 
                '{row['ExtraField1']}' AS ExtraField1, 
                '{row['ExtraField2']}' AS ExtraField2, 
                '{row['ExtraField3']}' AS ExtraField3, 
                '{row['ExtraField4']}' AS ExtraField4, 
                '{row['ExtraField5']}' AS ExtraField5, 
                '{row['ExtraField6']}' AS ExtraField6, 
                '{row['ExtraField7']}' AS ExtraField7, 
                '{row['ExtraField8']}' AS ExtraField8, 
                '{row['PropertyID']}' AS PropertyID, 
                '{row['StoreID']}' AS StoreID, 
                '{row['FiscalRef']}' AS FiscalRef FROM DUMMY LIMIT 1) AS source
                ON (target.RVCID = source.RVCID 
                   AND target.PCWSID = source.PCWSID 
                   AND target.FCRID = source.FCRID 
                   AND target.FCRINVNUMBER = source.FCRInvNumber 
                   AND target.MICROSCHKNUM = source.MicrosChkNum 
                   AND target.INVOICETYPE = source.InvoiceType 
                   AND target.INVOICELETTER = source.InvoiceLetter 
                   AND target.MICROSBSNZDATE = source.MicrosBsnzDate 
                   AND target.FCRBSNZDATE = source.FCRBsnzDate
                   AND target.ITEMINDEX = source.ItemIndex
                   AND target.ITEMTYPE = source.ItemType
                   AND target.ITEMOBJNUM = source.ItemObjNum
                   AND target.ITEMQTY = source.ItemQty
                   AND target.ITEMTOTAL = source.ItemTotal
                   AND target.ITEMTAXTOTAL = source.ItemTaxTotal
                   AND target.ITEMTAXRATE = source.ItemTaxRate
                   AND target.ITEMSTATUS = source.ItemStatus
                   AND target.EXTRAFIELD1 = source.ExtraField1
                   AND target.EXTRAFIELD2 = source.ExtraField2
                   AND target.EXTRAFIELD3 = source.ExtraField3
                   AND target.EXTRAFIELD4 = source.ExtraField4
                   AND target.EXTRAFIELD5 = source.ExtraField5
                   AND target.EXTRAFIELD6 = source.ExtraField6
                   AND target.EXTRAFIELD7 = source.ExtraField7
                   AND target.EXTRAFIELD8 = source.ExtraField8
                   AND target.PROPERTYID = source.PropertyID
                   AND target.STOREID = source.StoreID
                   AND target.FISCALREF = source.FiscalRef)
                
                WHEN NOT MATCHED THEN
                    INSERT ({", ".join(columns)}) 
                    VALUES ({", ".join(["?" for _ in columns])})
                """

                # 📌 Manejo de valores `None`
                values = tuple(None if pd.isna(row[col]) else row[col] for col in columns)

                # 📌 Ejecutar la consulta para cada fila
                cursor_hana.execute(merge_sql, values)

            # 📌 Confirmar cambios en SAP HANA
            conn_hana.commit()
            print("✅ MERGE completado en SAP HANA.")

        conn_sql.close()

    except Exception as e:
        print(f"❌ Error conectando a {srv['host']}: {e}")

# Cerrar la conexión a SAP HANA
conn_hana.close()
