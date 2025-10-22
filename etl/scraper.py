import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
from utils.db import conectar_sql

def obtener_noticias(url, categoria, fuente, configuracion=None):
    """Extrae título, enlace y fecha desde una sección de noticias."""
    noticias = []
    
    # Usar configuración por defecto si no se proporciona
    if not configuracion:
        configuracion = {
            "timeout_segundos": 15,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    
    try:
        headers = {'User-Agent': configuracion.get("user_agent", "")}
        timeout = configuracion.get("timeout_segundos", 15)
        
        res = requests.get(url, timeout=timeout, headers=headers)
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
        config = json.load(f)

    # Obtener configuración y fuentes
    fuentes = config["fuentes"]
    configuracion = config.get("configuracion", {})
    
    conn = conectar_sql()
    total = 0

    for key, fuente in fuentes.items():
        # Solo procesar fuentes activas
        if not fuente.get("activo", True):
            print(f"⏭️ Saltando {fuente['nombre']} (desactivada)")
            continue
            
        print(f"📰 Extrayendo noticias de {fuente['nombre']}...")
        guardar_medios(conn, fuente)

        for categoria, url in fuente["secciones"].items():
            noticias = obtener_noticias(url, categoria, fuente, configuracion)
            guardar_noticias(conn, noticias)
            total += len(noticias)
            print(f"✅ {len(noticias)} noticias guardadas de {categoria}")
            
            # Aplicar delay entre requests si está configurado
            import time
            delay = configuracion.get("delay_entre_requests", 0)
            if delay > 0:
                time.sleep(delay)

    conn.close()
    print(f"🎯 Total de noticias procesadas: {total}")


if __name__ == "__main__":
    ejecutar_scraper()
