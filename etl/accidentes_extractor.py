import re
import pandas as pd
from datetime import datetime
from utils.db import conectar_sql

# Palabras clave para detectar accidentes
ACCIDENTE_KEYWORDS = [
    "accidente", "choque", "colisión", "atropello", "volcadura",
    "despiste", "arrollado", "impacto", "camioneta", "motocicleta",
    "herido", "fallecido", "muerto", "ambulancia", "auto", "vehículo"
]

def contiene_accidente(texto):
    texto = texto.lower()
    return any(k in texto for k in ACCIDENTE_KEYWORDS)

def tipo_accidente(texto):
    texto = texto.lower()
    if "choque" in texto or "colisión" in texto:
        return "Choque"
    if "atropello" in texto or "arrollado" in texto:
        return "Atropello"
    if "volcadura" in texto or "despiste" in texto:
        return "Volcadura"
    if "motocicleta" in texto or "moto" in texto:
        return "Motocicleta"
    if "camioneta" in texto or "camión" in texto:
        return "Vehículo pesado"
    return "Otro"

def extraer_numeros(texto):
    texto = texto.lower()
    fallecidos = re.search(r"(\d+)\s+(fallecid|muert)", texto)
    heridos = re.search(r"(\d+)\s+(herid)", texto)
    return (
        int(fallecidos.group(1)) if fallecidos else 0,
        int(heridos.group(1)) if heridos else 0
    )

def extraer_accidentes():
    conn = conectar_sql()
    df = pd.read_sql("SELECT id, titulo, fuente, fecha_publicacion FROM noticias", conn)
    cursor = conn.cursor()
    total = 0

    for _, row in df.iterrows():
        titulo = row.titulo.lower()
        if contiene_accidente(titulo):
            tipo = tipo_accidente(titulo)
            fallecidos, heridos = extraer_numeros(titulo)

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM accidentes WHERE noticia_id = ?)
                INSERT INTO accidentes (
                    noticia_id, fecha_accidente, tipo_accidente,
                    num_fallecidos, num_heridos
                ) VALUES (?, ?, ?, ?, ?)
            """, (row.id, row.id, row.fecha_publicacion, tipo, fallecidos, heridos))
            total += 1

    conn.commit()
    conn.close()
    print(f"🚗 {total} accidentes detectados y registrados correctamente.")

if __name__ == "__main__":
    print("🚨 Extrayendo accidentes desde noticias...")
    extraer_accidentes()
