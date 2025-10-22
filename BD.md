# Documentación de Base de Datos - Incidentes Tacna

## 📊 Diagramas

➡️ **[Diagrama ER Completo y Diccionario](diagrams/database-er-diagram.md)**

## 🗄️ Tablas Principales
- **medios**
- **noticias**
- **accidentes**
- **keywords**

### Estadísticas Actuales
- **Medios:** 0             
- **Noticias:** 0           
- **Accidentes:** 0         
- **Keywords:** 0           

(4 rows affected)

## 🔍 Características
- ✅ Relaciones con claves foráneas (DELETE CASCADE)
- ✅ Índices optimizados
- ✅ Vistas analíticas
- ✅ Procedimientos almacenados listos para uso
- ✅ Compatible con Azure SQL Server

## 🔧 Vistas y Procedimientos
| Tipo | Nombre |
|------|---------|
| Vista | vw_accidentes_por_distrito |
| Vista | vw_noticias_accidentes |
| Vista | vw_estadisticas_tipo_accidente |
| SP | sp_buscar_por_keyword |
| SP | sp_resumen_mensual |

---
*Base de datos actualizada el $(date)* 
