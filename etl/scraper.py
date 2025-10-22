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