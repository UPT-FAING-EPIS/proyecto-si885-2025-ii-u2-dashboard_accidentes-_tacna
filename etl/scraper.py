"""(Updated etl/scraper.py) Replace problematic URL joining and DB insertion logic, add error logging and real DB counting per batch.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
from urllib.parse import urljoin
from utils.db import conectar_sql

def obtener_noticias(url, categoria, fuente, configuracion=None):
    """Extrae título, enlace y fecha desde una sección de noticias.
    Usa url_principal de la fuente para construir URLs relativas y soporta configuración.
    """
    noticias = []

    # Usar configuración por defecto si no se proporciona
    if not configuracion:
        configuracion = {
            "timeout_segundos": 15,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    try:
        headers = {'User-Agent': configuracion.get("user_agent", "") }
        timeout = configuracion.get("timeout_segundos", 15)

        res = requests.get(url, timeout=timeout, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        for art in soup.select("article"):
            titulo_tag = art.find("a")
            if not titulo_tag:
                continue

            titulo = titulo_tag.get_text(strip=True)
            enlace = titulo_tag.get("href") or ""

            # Construir URL absoluta usando url_principal de la fuente
            if enlace and not enlace.startswith("http"):
                enlace = urljoin(fuente.get("url_principal", ""), enlace)

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
    """Inserta el medio si no existe en la tabla medios.
    Maneja errores y no rompe el flujo si la inserción falla.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM medios WHERE nombre = ?)
            INSERT INTO medios (nombre, tipo, region, url_principal)
            VALUES (?, ?, ?, ?)
        """, (fuente["nombre"], fuente["tipo"], fuente["region"], fuente["url_principal"]))
        conn.commit()
    except Exception as e:
        print(f"❌ Error al insertar/actualizar medio '{fuente.get('nombre')}': {e}")

def guardar_noticias(conn, noticias):
    """Inserta las noticias en la base de datos, evitando duplicados.
    Devuelve el número de filas nuevas insertadas en la BD para este lote.
    Registra errores por noticia.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM noticias")
        before = cursor.fetchone()[0] if cursor.fetchone() is None else cursor.fetchone()[0]
    except Exception:
        # Si la consulta falla (tabla no existe), asumimos 0
        before = 0

    errores = 0
    for n in noticias:
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM noticias WHERE titulo = ? AND fuente = ?)
                INSERT INTO noticias (titulo, url, fuente, categoria, fecha_publicacion, fecha_extraccion, ciudad)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                n["titulo"], n["url"], n["fuente"], n["categoria"],
                n["fecha_publicacion"], n["fecha_extraccion"], n["ciudad"]
            ))
        except Exception as e:
            errores += 1
            print(f"❌ Error al insertar noticia '{n['titulo'][:80]}...': {e}")
    conn.commit()

    try:
        cursor.execute("SELECT COUNT(*) FROM noticias")
        after = cursor.fetchone()[0]
    except Exception:
        after = before

    insertadas = max(0, after - before)
    print(f"✅ Intentadas: {len(noticias)}, nuevas insertadas en BD: {insertadas}, errores: {errores}")
    return insertadas

def ejecutar_scraper():
    """Carga fuentes desde JSON y guarda noticias en Azure SQL.
    Retorna el número total de noticias nuevas insertadas en la BD.
    """
    with open("etl/fuentes.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    # Obtener configuración y fuentes
    fuentes = config["fuentes"]
    configuracion = config.get("configuracion", {})

    conn = conectar_sql()
    total_nuevas = 0

    for key, fuente in fuentes.items():
        # Solo procesar fuentes activas
        if not fuente.get("activo", True):
            print(f"⏭️ Saltando {fuente['nombre']} (desactivada)")
            continue

        print(f"📰 Extrayendo noticias de {fuente['nombre']}...")
        guardar_medios(conn, fuente)

        for categoria, url in fuente["secciones"].items():
            noticias = obtener_noticias(url, categoria, fuente, configuracion)
            nuevas = guardar_noticias(conn, noticias)
            total_nuevas += nuevas
            print(f"✅ {len(noticias)} noticias extraídas de {categoria}, {nuevas} nuevas insertadas en BD")

            # Aplicar delay entre requests si está configurado
            import time
            delay = configuracion.get("delay_entre_requests", 0)
            if delay > 0:
                time.sleep(delay)

    # Mostrar conteo real en BD
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM noticias")
        total_en_bd = cursor.fetchone()[0]
        print(f"🎯 Total de noticias procesadas (nuevas insertadas): {total_nuevas}")
        print(f"📊 Total de noticias reales en DB después del scraper: {total_en_bd}")
    except Exception as e:
        print(f"⚠️ No se pudo obtener COUNT(*) de noticias: {e}")

    conn.close()
    return total_nuevas

if __name__ == "__main__":
    ejecutar_scraper()