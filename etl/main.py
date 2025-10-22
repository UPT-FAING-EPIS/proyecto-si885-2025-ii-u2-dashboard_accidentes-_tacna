# etl/main.py
from etl.scraper import SECTIONS, get_links, extract_text
from etl.transform import transformar_noticia
from etl.load import guardar_csv, cargar_sql

def run_etl():
    rows = []
    for medio, url in SECTIONS.items():
        print(f"--- Buscando en {medio} ---")
        links = get_links(url, pages=20, limit=200)  # 🔥 más páginas
        for link in links:
            texto = extract_text(link)
            resultados = transformar_noticia(medio, link, texto)
            rows.extend(resultados)

    df = guardar_csv(rows)
    cargar_sql(df)   # 🚀 subimos a Azure también
    print("ETL terminado. Registros:", len(df))

if __name__ == "__main__":
    run_etl()
