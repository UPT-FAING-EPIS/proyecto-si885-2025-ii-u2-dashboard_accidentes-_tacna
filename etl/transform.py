import re, time
import spacy
from geopy.geocoders import Nominatim

# palabras clave
KEYWORDS = ["accidente", "choque", "muerto", "herido",
            "asalto", "robo", "homicidio", "asesinato", "violencia", "disparo"]

nlp = spacy.load("es_core_news_sm")
geolocator = Nominatim(user_agent="scraper_tacna")

def extract_locations(text):
    doc = nlp(text)
    locs = [ent.text for ent in doc.ents if ent.label_ in ("LOC","GPE")]
    extra = re.findall(r"(Panamericana Sur|Costanera|Tacna-Tarata|km\s*\d+)", text, flags=re.IGNORECASE)
    return list(set(locs + extra))

def geocode_location(loc):
    try:
        time.sleep(1)  # respetar límites API
        g = geolocator.geocode(f"{loc}, Tacna, Peru")
        if g:
            return g.latitude, g.longitude
    except:
        return None
    return None

def transformar_noticia(medio, url, texto):
    resultados = []
    if any(k in texto.lower() for k in KEYWORDS):
        locs = extract_locations(texto)
        for l in locs:
            coords = geocode_location(l)
            if coords:
                resultados.append({
                    "medio": medio,
                    "url": url,
                    "ubicacion": l,
                    "lat": coords[0],
                    "lon": coords[1],
                    "snippet": texto[:200]
                })
    return resultados
