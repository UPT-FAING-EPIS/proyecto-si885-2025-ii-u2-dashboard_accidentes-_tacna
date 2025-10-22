# Diagrama Entidad-Relación - Sistema Incidentes Tacna

```mermaid
erDiagram
    medios ||--o{ noticias : publica
    noticias ||--o{ accidentes : reporta
    noticias ||--o{ keywords : contiene
    
    medios {
        int id PK
        nvarchar nombre
        nvarchar tipo
        nvarchar region
        nvarchar url_principal
    }
    
    noticias {
        int id PK
        nvarchar titulo
        nvarchar url
        nvarchar fuente
        nvarchar categoria
        date fecha_publicacion
        date fecha_extraccion
        nvarchar ciudad
        nvarchar sentimiento
    }
    
    accidentes {
        int id PK
        int noticia_id FK
        date fecha_accidente
        nvarchar hora_aproximada
        nvarchar dia_semana
        nvarchar distrito
        nvarchar tipo_via
        nvarchar tipo_accidente
        nvarchar causa_probable
        int num_vehiculos_involucrados
        nvarchar tipo_vehiculo
        nvarchar condicion_clima
        int num_fallecidos
        int num_heridos
        nvarchar danos_materiales
        bit intervencion_bomberos
        nvarchar coordenadas
        int tiempo_publicacion
    }
    
    keywords {
        int id PK
        int noticia_id FK
        nvarchar palabra_clave
    }
```

## Vistas y Procedimientos
- **vw_accidentes_por_distrito**
- **vw_noticias_accidentes**
- **vw_estadisticas_tipo_accidente**
- **sp_buscar_por_keyword**
- **sp_resumen_mensual**
