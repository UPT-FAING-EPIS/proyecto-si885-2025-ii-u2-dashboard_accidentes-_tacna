import os
import pyodbc

def conectar_sql():
    """
    Crea la conexión a la base de datos Azure SQL Server.
    Usa las variables de entorno SQL_SERVER, SQL_USER, SQL_PASS.
    """
    try:
        # Verificar que las variables de entorno existan
        server = os.environ.get('SQL_SERVER')
        user = os.environ.get('SQL_USER')
        password = os.environ.get('SQL_PASS')
        
        if not all([server, user, password]):
            raise ValueError("Faltan variables de entorno: SQL_SERVER, SQL_USER, SQL_PASS")
        
        # Cadena de conexión para Azure SQL Database
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE=incidentestacna;"
            f"UID={user};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        
        print(f"Conectando a: {server}")
        conn = pyodbc.connect(connection_string)
        print("✅ Conexión exitosa a Azure SQL Database")
        return conn
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        # Listar drivers disponibles para debug
        print("Drivers ODBC disponibles:")
        for driver in pyodbc.drivers():
            print(f"  - {driver}")
        raise
