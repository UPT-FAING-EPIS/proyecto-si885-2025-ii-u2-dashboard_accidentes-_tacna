import pyodbc

try:
    conn = pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:incidentes.database.windows.net,1433;"
        "Database=incidentes;"
        "Uid=Luzkalid;"
        "Pwd=Tacna123;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    print("✅ Conexión exitosa a Azure SQL")
    conn.close()
except Exception as e:
    print("❌ Error de conexión:", e)
