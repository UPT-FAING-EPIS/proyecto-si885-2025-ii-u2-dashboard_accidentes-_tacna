import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    try:
        conn = pyodbc.connect(
            f"Driver={{{os.getenv('AZURE_SQL_DRIVER')}}};"
            f"Server=tcp:{os.getenv('AZURE_SQL_SERVER')},1433;"
            f"Database={os.getenv('AZURE_SQL_DATABASE')};"
            f"Uid={os.getenv('AZURE_SQL_USERNAME')};"
            f"Pwd={os.getenv('AZURE_SQL_PASSWORD')};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        print("✅ Conectado exitosamente a Azure SQL Database.")
        return conn
    except Exception as e:
        print("❌ Error al conectar con Azure SQL:", e)
        raise
