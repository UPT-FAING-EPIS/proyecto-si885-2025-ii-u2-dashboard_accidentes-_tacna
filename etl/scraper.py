# etl/scraper.py
import requests
from bs4 import BeautifulSoup
import re, time

# Secciones base
SECTIONS = {
    "RPP": "https://rpp.pe/peru/tacna",
    "El Comercio": "https://elcomercio.pe/peru/tacna/",
    "La Republica": "https://larepublica.pe",
    "Peru21": "https://peru21.pe/peru/tacna/",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_links(section_url, pages=5, limit=50):
    """
    Extrae enlaces de noticias recorriendo varias páginas.
    pages = número de páginas a recorrer
    limit = máximo de links por sección
    """
    links = []
    for page in range(1, pages + 1):
        url = f"{section_url}?page={page}" if "larepublica" in section_url else section_url
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "tacna" in href.lower() and ("noticia" in href.lower() or href.startswith("https")):
                    if not href.startswith("http"):
                        href = requests.compat.urljoin(section_url, href)
                    if href not in links:
                        links.append(href)
                if len(links) >= limit:
                    break
        except Exception as e:
            print(f"Error en {url}: {e}")
        time.sleep(1)
    return links


def extract_text(url):
    """
    Extrae el cuerpo de una noticia (texto en <p>)
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        parts = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        return " ".join(parts)
    except Exception as e:
        print(f"Error al extraer texto de {url}: {e}")
        return ""
