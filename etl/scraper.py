import requests
from bs4 import BeautifulSoup
from datetime import date
import json
from urllib.parse import urljoin
from utils.db import conectar_sql
import time


def obtener_noticias(url, categoria, fuente, configuracion=None):
    """Extrae título, enlace y fecha desde una sección de noticias.
    Usa url_principal de la fuente para construir URLs relativas y soporta configuración.
    """
    noticias = []

    if not configuracion:
        configuracion = {
            "timeout_segundos": 15,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    try:
        headers = {"User-Agent": configuracion.get("user_agent", "")}
        timeout = configuracion.get("timeout_segundos", 15)

        res = requests.get(url, timeout=timeout, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        # Selector genérico: article -> a. Puedes adaptar por fuente si hace falta.
        for art in soup.select("article"):
            titulo_tag = art.find("a")
            if not titulo_tag:
                continue

            titulo = titulo_tag.get_text(strip=True)
            enlace = titulo_tag.get("href") or ""

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
                "ciudad": fuente.get("region", "Tacna")
            })
    except Exception as e:
        print(f"⚠️ Error al scrapear {url}: {e}")
    return noticias


def guardar_medios(conn, fuente):
    """Inserta el medio si no existe en la tabla medios.
    Comprueba con SELECT primero y luego INSERT si hace falta.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM medios WHERE nombre = ?", (fuente["nombre"],))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO medios (nombre, tipo, region, url_principal) VALUES (?, ?, ?, ?)",
                (fuente["nombre"], fuente["tipo"], fuente.get("region", ""), fuente.get("url_principal", ""))
            )
            conn.commit()
    except Exception as e:
        print(f"❌ Error al insertar/actualizar medio '{fuente.get('nombre')}': {e}")


def guardar_noticias(conn, noticias):
    """Inserta las noticias en la base de datos, evitando duplicados.
    Comprueba existencia por URL (preferible) o por título (fallback) y muestra logs detallados.
    Devuelve cuántas filas nuevas fueron insertadas.
    """
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM noticias")
        row = cursor.fetchone()
        before = row[0] if row else 0
    except Exception:
        before = 0

    errores = 0
    nuevas = 0
    for n in noticias:
        try:
            titulo_check = (n.get("titulo") or "").strip()
            url_check = (n.get("url") or "").strip()

            existe = False
            indicador = None

            if url_check:
                cursor.execute("SELECT 1 FROM noticias WHERE url = ?", (url_check,))
                existe = cursor.fetchone() is not None
                indicador = "url"
            else:
                # Normalizar: recortar y comparar en minúsculas
                cursor.execute("SELECT 1 FROM noticias WHERE LOWER(RTRIM(LTRIM(titulo))) = ?", (titulo_check.lower(),))
                existe = cursor.fetchone() is not None
                indicador = "titulo"

            if existe:
                print(f"⏭️ Saltada (ya existe por {indicador}): '{titulo_check[:100]}' - {url_check}")
            else:
                cursor.execute(
                    "INSERT INTO noticias (titulo, url, fuente, categoria, fecha_publicacion, fecha_extraccion, ciudad) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        n.get("titulo"), n.get("url"), n.get("fuente"), n.get("categoria"),
                        n.get("fecha_publicacion"), n.get("fecha_extraccion"), n.get("ciudad")
                    )
                )
                nuevas += 1
                print(f"➕ Insertada: '{titulo_check[:100]}' - {url_check}")
        except Exception as e:
            errores += 1
            print(f"❌ Error al insertar noticia '{(n.get('titulo') or '')[:80]}...': {e}")

    try:
        conn.commit()
    except Exception as e:
        print(f"❌ Error al commitear cambios en noticias: {e}")

    try:
        cursor.execute("SELECT COUNT(*) FROM noticias")
        row = cursor.fetchone()
        after = row[0] if row else before
    except Exception:
        after = before

    insertadas_report = max(0, after - before)
    print(f"✅ Intentadas: {len(noticias)}, nuevas insertadas en BD: {insertadas_report}, errores: {errores}")
    return insertadas_report


def ejecutar_scraper():
    """Carga fuentes desde JSON y guarda noticias en Azure SQL.
    Retorna el número total de noticias nuevas insertadas en la BD.
    """
    with open("etl/fuentes.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    fuentes = config.get("fuentes", {})
    configuracion = config.get("configuracion", {})

    conn = conectar_sql()
    total_nuevas = 0

    for key, fuente in fuentes.items():
        if not fuente.get("activo", True):
            print(f"⏭️ Saltando {fuente.get('nombre')} (desactivada)")
            continue

        print(f"📰 Extrayendo noticias de {fuente.get('nombre')}...")
        guardar_medios(conn, fuente)

        for categoria, url in fuente.get("secciones", {}).items():
            noticias = obtener_noticias(url, categoria, fuente, configuracion)
            nuevas = guardar_noticias(conn, noticias)
            total_nuevas += nuevas
            print(f"✅ {len(noticias)} noticias extraídas de {categoria}, {nuevas} nuevas insertadas en BD")

            delay = configuracion.get("delay_entre_requests", 0)
            if delay > 0:
                time.sleep(delay)

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