import os
from scraper import ejecutar_scraper
from keywords_extractor import extraer_keywords
from accidentes_extractor import extraer_accidentes

if __name__ == "__main__":
    print("🚀 Iniciando proceso ETL completo...")
    ejecutar_scraper()
    extraer_keywords()
    extraer_accidentes()
    print("✅ Proceso ETL completado y datos cargados en Azure SQL.")
