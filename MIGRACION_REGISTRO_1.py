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
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute("SELECT PCWSID, FCRInvNumber, MicrosChkNum, InvoiceType, InSARMode, CustomerID, InvoiceStatus, MicrosBsnzDate, FCRBsnzDate, Subtotal1, Subtotal2, Subtotal3, Subtotal4, Subtotal5, Subtotal6, Subtotal7, Subtotal8, Subtotal9, Subtotal10, Subtotal11, Subtotal12, TaxTtl1, TaxTtl2, TaxTtl3, TaxTtl4, TaxTtl5, TaxTtl6, TaxTtl7, TaxTtl8, ExtraField1, ExtraField2, ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, PropertyID, FCRID, StoreID, SerialID, DocumentType, DataType, SysDocID, FiscalRef, Subtotal13, Subtotal14, ExtraField9, ExtraField10, ExtraField11, ExtraField12, ExtraField13, ExtraField14, ExtraField15, ExtraField16, ExtraDate1, ExtraDate2, ExtraDate3, ExtraDate4, ExtraDate5, PurgeStatus, HierStrucID, RevenueCenterID, Guid, ReplicationDest, ReplicationStatus, ReplicationError, CPPurgeDate, RepPurgeDate, UpdateTime, TaxTtl9, TaxTtl10, TaxTtl11, TaxTtl12, TaxTtl13, TaxTtl14, TaxTtl15, TaxTtl16, CheckGuid, FCRInvoiceDataID, FCRJSONEXP FROM FCR_INVOICE_DATA  WHERE FCRInvNumber = 1613049" )  # Consulta de prueba
        registro = cursor_sql.fetchone()  #OBTENER UN REGISTRO
        registro = tuple("NULL" if x is None else x for x in registro)
        #registro = registro.where(pd.notnull(registro), None)
        print(f"Conectado a {srv['host']}, Total de registros: {cursor_sql.fetchone()[0]}")
        print(registro[1])
        
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


#try:        
       # query = "SELECT RVCID, PCWSID, FCRID, FCRInvNumber, MicrosChkNum, InvoiceType, InvoiceLetter, MicrosBsnzDate, FCRBsnzDate, ItemIndex, ItemType, ItemObjNum, ItemQty, ItemTotal, ItemTaxTotal, ItemTaxRate, ItemStatus, ExtraField1, ExtraField2, ExtraField3, ExtraField4, ExtraField5, ExtraField6, ExtraField7, ExtraField8, PropertyID, StoreID, FiscalRef  FROM FCR_INVOICE_DETAIL_BP"
        #df = pd.read_sql(query, conn_sql)  
        #print(df.head()) 
try: 
     if registro:
        cursor_hana = conn_hana.cursor()
        query_hana = ('INSERT INTO "COLSUBSIDIO_HDI"."T_SIMP_CAPS_FCR_INVOICE_DATA"  (PCWSID, FCRINVNUMBER, MICROSCHKNUM, INVOICETYPE, INSARMODE, CUSTOMERID, INVOICESTATUS, MICROSBSNZDATE, FCRBSNZDATE, SUBTOTAL1, SUBTOTAL2, SUBTOTAL3, SUBTOTAL4, SUBTOTAL5, SUBTOTAL6, SUBTOTAL7, SUBTOTAL8, SUBTOTAL9, SUBTOTAL10, SUBTOTAL11, SUBTOTAL12, TAXTTL1, TAXTTL2, TAXTTL3, TAXTTL4, TAXTTL5, TAXTTL6, TAXTTL7, TAXTTL8, EXTRAFIELD1, EXTRAFIELD2, EXTRAFIELD3, EXTRAFIELD4, EXTRAFIELD5, EXTRAFIELD6, EXTRAFIELD7, EXTRAFIELD8, PROPERTYID, FCRID, STOREID, SERIALID, DOCUMENTTYPE, DATATYPE, SYSDOCID, FISCALREF, SUBTOTAL13, SUBTOTAL14, EXTRAFIELD9, EXTRAFIELD10, EXTRAFIELD11, EXTRAFIELD12, EXTRAFIELD13, EXTRAFIELD14, EXTRAFIELD15, EXTRAFIELD16, EXTRADATE1, EXTRADATE2, EXTRADATE3, EXTRADATE4, EXTRADATE5, PURGESTATUS, HIERSTRUCID, REVENUECENTERID, GUID, REPLICATIONDEST, REPLICATIONSTATUS, REPLICATIONERROR, CPPURGEDATE, REPPURGEDATE, UPDATETIME, TAXTTL9, TAXTTL10, TAXTTL11, TAXTTL12, TAXTTL13, TAXTTL14, TAXTTL15, TAXTTL16, CHECKGUID, FCRINVOICEDATAID, FCRJSONEXP) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ')  # Consulta de prueba
        cursor_hana.execute(query_hana, registro )
        print(f"Conectado a  HDI, Total de registros: {cursor_hana.fetchone()[0]}")
        conn_hana.commit()
        print("Registro insertado en SAP HANA:", registro)




except  Exception as e:               
        print("Error al cargar el DataFrame:", e)
#except Exception as e:
        #print("Error al cargar el DataFrame:", e)
                
        
        conn_sql.close()
        conn_hana.close()

        