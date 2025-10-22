import os
import pandas as pd
import pyodbc

def guardar_csv(rows, filename="noticias_tacna_incidentes.csv"):
    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"✅ CSV guardado: {filename}")
    return df

def cargar_sql(df):
    if df.empty:
        print("⚠️ No hay datos para cargar en SQL")
        return

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
        cursor = conn.cursor()
        cursor.fast_executemany = True  # ⚡ optimiza inserts masivos

        insert_query = """
            INSERT INTO IncidentesTacna (medio, url, ubicacion, lat, lon, snippet)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        # 🚨 Insertar fila por fila con try/except
        for i, row in df.iterrows():
            try:
                cursor.execute(insert_query, 
                               row.get("medio"), 
                               row.get("url"), 
                               row.get("ubicacion"), 
                               row.get("lat"), 
                               row.get("lon"), 
                               row.get("snippet"))
            except Exception as e:
                print(f"❌ Error en fila {i}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Datos cargados en Azure SQL")

    except Exception as e:
        print("❌ Error general en carga SQL:", e)
