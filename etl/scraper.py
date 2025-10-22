"""(Updated etl/scraper.py) Replace problematic URL joining and DB insertion logic, add error logging and real DB counting per batch.\n"""\n\nimport requests\nfrom bs4 import BeautifulSoup\nfrom datetime import datetime, date\nimport json\nfrom urllib.parse import urljoin\nfrom utils.db import conectar_sql\n\ndef obtener_noticias(url, categoria, fuente, configuracion=None):\n    """Extrae título, enlace y fecha desde una sección de noticias.\n    Usa url_principal de la fuente para construir URLs relativas y soporta configuración.\n    """\n    noticias = []\n\n    # Usar configuración por defecto si no se proporciona\n    if not configuracion:\n        configuracion = {\n            "timeout_segundos": 15,\n            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"\n        }\n\n    try:\n        headers = {'User-Agent': configuracion.get("user_agent", "") }\n        timeout = configuracion.get("timeout_segundos", 15)\n\n        res = requests.get(url, timeout=timeout, headers=headers)\n        res.raise_for_status()\n        soup = BeautifulSoup(res.text, "html.parser")\n\n        for art in soup.select("article"):\n            titulo_tag = art.find("a")\n            if not titulo_tag:\n                continue\n\n            titulo = titulo_tag.get_text(strip=True)\n            enlace = titulo_tag.get("href") or ""\n\n            # Construir URL absoluta usando url_principal de la fuente\n            if enlace and not enlace.startswith("http"):\n                enlace = urljoin(fuente.get("url_principal", ""), enlace)\n\n            fecha = date.today()\n\n            noticias.append({\n                "titulo": titulo,\n                "url": enlace,\n                "fuente": fuente["nombre"],\n                "categoria": categoria,\n                "fecha_publicacion": fecha,\n                "fecha_extraccion": date.today(),\n                "ciudad": fuente["region"]\n            })\n    except Exception as e:\n        print(f"⚠️ Error al scrapear {url}: {e}")\n    return noticias\n\ndef guardar_medios(conn, fuente):\n    """Inserta el medio si no existe en la tabla medios.\n    Maneja errores y no rompe el flujo si la inserción falla.\n    """\n    try:\n        cursor = conn.cursor()\n        cursor.execute("""\n            IF NOT EXISTS (SELECT 1 FROM medios WHERE nombre = ?)\n            INSERT INTO medios (nombre, tipo, region, url_principal)\n            VALUES (?, ?, ?, ?)\n        """, (fuente["nombre"], fuente["tipo"], fuente["region"], fuente["url_principal"]))\n        conn.commit()\n    except Exception as e:\n        print(f"❌ Error al insertar/actualizar medio '{fuente.get('nombre')}': {e}")\n\ndef guardar_noticias(conn, noticias):\n    """Inserta las noticias en la base de datos, evitando duplicados.\n    Devuelve el número de filas nuevas insertadas en la BD para este lote.\n    Registra errores por noticia.\n    """\n    cursor = conn.cursor()\n    try:\n        cursor.execute("SELECT COUNT(*) FROM noticias")\n        before = cursor.fetchone()[0] if cursor.fetchone() is None else cursor.fetchone()[0]\n    except Exception:\n        # Si la consulta falla (tabla no existe), asumimos 0\n        before = 0\n\n    errores = 0\n    for n in noticias:\n        try:\n            cursor.execute("""\n                IF NOT EXISTS (SELECT 1 FROM noticias WHERE titulo = ? AND fuente = ?)\n                INSERT INTO noticias (titulo, url, fuente, categoria, fecha_publicacion, fecha_extraccion, ciudad)\n                VALUES (?, ?, ?, ?, ?, ?, ?)\n            """, (\n                n["titulo"], n["url"], n["fuente"], n["categoria"],\n                n["fecha_publicacion"], n["fecha_extraccion"], n["ciudad"]\n            ))\n        except Exception as e:\n            errores += 1\n            print(f"❌ Error al insertar noticia '{n['titulo'][:80]}...': {e}")\n    conn.commit()\n\n    try:\n        cursor.execute("SELECT COUNT(*) FROM noticias")\n        after = cursor.fetchone()[0]\n    except Exception:\n        after = before\n\n    insertadas = max(0, after - before)\n    print(f"✅ Intentadas: {len(noticias)}, nuevas insertadas en BD: {insertadas}, errores: {errores}")\n    return insertadas\n\ndef ejecutar_scraper():\n    """Carga fuentes desde JSON y guarda noticias en Azure SQL.\n    Retorna el número total de noticias nuevas insertadas en la BD.\n    """\n    with open("etl/fuentes.json", "r", encoding="utf-8") as f:\n        config = json.load(f)\n\n    # Obtener configuración y fuentes\n    fuentes = config["fuentes"]\n    configuracion = config.get("configuracion", {})\n\n    conn = conectar_sql()\n    total_nuevas = 0\n\n    for key, fuente in fuentes.items():\n        # Solo procesar fuentes activas\n        if not fuente.get("activo", True):\n            print(f"⏭️ Saltando {fuente['nombre']} (desactivada)")\n            continue\n\n        print(f"📰 Extrayendo noticias de {fuente['nombre']}...")\n        guardar_medios(conn, fuente)\n\n        for categoria, url in fuente["secciones"].items():\n            noticias = obtener_noticias(url, categoria, fuente, configuracion)\n            nuevas = guardar_noticias(conn, noticias)\n            total_nuevas += nuevas\n            print(f"✅ {len(noticias)} noticias extraídas de {categoria}, {nuevas} nuevas insertadas en BD")\n\n            # Aplicar delay entre requests si está configurado\n            import time\n            delay = configuracion.get("delay_entre_requests", 0)\n            if delay > 0:\n                time.sleep(delay)\n\n    # Mostrar conteo real en BD\n    try:\n        cursor = conn.cursor()\n        cursor.execute("SELECT COUNT(*) FROM noticias")\n        total_en_bd = cursor.fetchone()[0]\n        print(f"🎯 Total de noticias procesadas (nuevas insertadas): {total_nuevas}")\n        print(f"📊 Total de noticias reales en DB después del scraper: {total_en_bd}")\n    except Exception as e:\n        print(f"⚠️ No se pudo obtener COUNT(*) de noticias: {e}")\n\n    conn.close()\n    return total_nuevas\n\nif __name__ == "__main__":\n    ejecutar_scraper()\n
# (Reemplazar las funciones obtener_noticias, guardar_medios, guardar_noticias y la impresión final)
from urllib.parse import urljoin

def obtener_noticias(url, categoria, fuente, configuracion=None):
    """Extrae título, enlace y fecha desde una sección de noticias."""
    noticias = []
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
            enlace = titulo_tag.get("href") or ""
            if enlace and not enlace.startswith("http"):
                # usar url_principal de la fuente para armar la URL absoluta
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
    """Inserta el medio si no existe en la tabla medios."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM medios WHERE nombre = ?)
            INSERT INTO medios (nombre, tipo, region, url_principal)
            VALUES (?, ?, ?, ?)
        """, (fuente["nombre"], fuente["nombre"], fuente["tipo"], fuente["region"], fuente["url_principal"]))
        conn.commit()
    except Exception as e:
        print(f"❌ Error al insertar/actualizar medio '{fuente.get('nombre')}': {e}")

def guardar_noticias(conn, noticias):
    """Inserta las noticias en la base de datos, evitando duplicados.
       Se registra cualquier error por noticia y al final muestra el total real en BD."""
    cursor = conn.cursor()
    insertadas = 0
    errores = 0
    for n in noticias:
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM noticias WHERE titulo = ? AND fuente = ?)
                INSERT INTO noticias (titulo, url, fuente, categoria, fecha_publicacion, fecha_extraccion, ciudad)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                n["titulo"], n["fuente"],
                n["titulo"], n["url"], n["fuente"], n["categoria"],
                n["fecha_publicacion"], n["fecha_extraccion"], n["ciudad"]
            ))
            insertadas += 1
        except Exception as e:
            errores += 1
            print(f"❌ Error al insertar noticia '{n['titulo'][:80]}...': {e}")
    conn.commit()
    print(f"✅ Intentadas: {len(noticias)}, ejecuciones INSERT intentadas: {insertadas}, errores: {errores}")

# Al final de ejecutar_scraper, antes de conn.close(), añade:
# cursor = conn.cursor()
# try:
#     cursor.execute("SELECT COUNT(*) FROM noticias")
#     total_in_db = cursor.fetchone()[0]
#     print(f"📊 Total de noticias reales en DB después del scraper: {total_in_db}")
# except Exception as e:
#     print(f"⚠️ No se pudo obtener COUNT(*) de noticias: {e}")