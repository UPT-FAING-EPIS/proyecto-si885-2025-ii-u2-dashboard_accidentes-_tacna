import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
from utils.db import conectar_sql

def obtener_noticias(url, categoria, fuente):
    """Extrae título, enlace y fecha desde una sección de noticias."""
    noticias = []
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        for art in soup.select("article"):
            titulo_tag = art.find("a")
            if not titulo_tag: 
                continue

            titulo = titulo_tag.get_text(strip=True)
            enlace = titulo_tag.get("href")

            if not enlace.startswith("http"):
                enlace = "https://radiouno.pe" + enlace  # fallback

            fecha = date.today()

            noticias.append({
                "titulo": titulo,
                "url": enlace,
                "fuente": fuente["nombre"],
                "categoria": categoria,
                "fecha_publicacion": fecha,
                "fecha_extraccion": date.today(),
                "ciudad": fuente["region"]
            })
    except Exception as e:
        print(f"⚠️ Error al scrapear {url}: {e}")
    return noticias


def guardar_medios(conn, fuente):
    """Inserta el medio si no existe en la tabla medios."""
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM medios WHERE nombre = ?)
        INSERT INTO medios (nombre, tipo, region, url_principal)
        VALUES (?, ?, ?, ?)
    """, (fuente["nombre"], fuente["nombre"], fuente["tipo"], fuente["region"], fuente["url_principal"]))
    conn.commit()


def guardar_noticias(conn, noticias):
    """Inserta las noticias en la base de datos, evitando duplicados."""
    cursor = conn.cursor()
    for n in noticias:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM noticias WHERE titulo = ? AND fuente = ?)
            INSERT INTO noticias (titulo, url, fuente, categoria, fecha_publicacion, fecha_extraccion, ciudad)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            n["titulo"], n["fuente"],
            n["titulo"], n["url"], n["fuente"], n["categoria"],
            n["fecha_publicacion"], n["fecha_extraccion"], n["ciudad"]
        ))
    conn.commit()


def ejecutar_scraper():
    """Carga fuentes desde JSON y guarda noticias en Azure SQL."""
    with open("etl/fuentes.json", "r", encoding="utf-8") as f:
        fuentes = json.load(f)

    conn = conectar_sql()
    total = 0

    for key, fuente in fuentes.items():
        print(f"📰 Extrayendo noticias de {fuente['nombre']}...")
        guardar_medios(conn, fuente)

        for categoria, url in fuente["secciones"].items():
            noticias = obtener_noticias(url, categoria, fuente)
            guardar_noticias(conn, noticias)
            total += len(noticias)
            print(f"✅ {len(noticias)} noticias guardadas de {categoria}")

    conn.close()
    print(f"🎯 Total de noticias procesadas: {total}")


if __name__ == "__main__":
    ejecutar_scraper()
