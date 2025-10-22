import os
import pyodbc

def conectar_sql():
    """
    Crea la conexión a la base de datos Azure SQL Server.
    Usa las variables de entorno SQL_SERVER, SQL_USER, SQL_PASS.
    """
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['SQL_SERVER']};"
        f"DATABASE=incidentestacna;"
        f"UID={os.environ['SQL_USER']};"
        f"PWD={os.environ['SQL_PASS']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return conn
