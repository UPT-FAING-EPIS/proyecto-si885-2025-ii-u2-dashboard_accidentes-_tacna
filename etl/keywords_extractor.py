import re
import pandas as pd
from utils.db import conectar_sql

# Palabras que se ignoran (muy comunes)
STOPWORDS = {
    "el", "la", "los", "las", "de", "del", "en", "y", "a", "por", "con",
    "para", "se", "que", "un", "una", "al", "su", "es", "lo", "como",
    "fue", "más", "sus", "ya", "este", "esta", "hay", "entre", "sin"
}

def limpiar_texto(texto):
    """Limpia texto de símbolos y stopwords."""
    texto = texto.lower()
    texto = re.sub(r"[^a-záéíóúüñ\s]", "", texto)
    palabras = [p for p in texto.split() if p not in STOPWORDS and len(p) > 3]
    return palabras

def extraer_keywords():
    """Extrae palabras clave desde la tabla noticias y las guarda en keywords."""
    conn = conectar_sql()
    df = pd.read_sql("SELECT id, titulo FROM noticias", conn)

    cursor = conn.cursor()
    total = 0

    for _, row in df.iterrows():
        palabras = limpiar_texto(row.titulo)
        for palabra in set(palabras):  # set para evitar repetidos
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM keywords 
                    WHERE noticia_id = ? AND palabra_clave = ?
                )
                INSERT INTO keywords (noticia_id, palabra_clave)
                VALUES (?, ?)
            """, (row.id, palabra, row.id, palabra))
            total += 1

    conn.commit()
    conn.close()
    print(f"🧠 {total} palabras clave extraídas y guardadas correctamente.")

if __name__ == "__main__":
    print("🧩 Extrayendo palabras clave...")
    extraer_keywords()
