![C:\\Users\\EPIS\\Documents\\upt.png][image1]

**UNIVERSIDAD PRIVADA DE TACNA**

**FACULTAD DE INGENIERÍA**

**Escuela Profesional de Ingeniería de Sistemas**

**Proyecto Dashboard de Accidentes de Tránsito en Tacna**

Curso: *Inteligencia de Negocios*

Docente: Mag. Patrick Cuadros Quiroga

Integrantes:

***SEBASTIAN NICOLAS FUENTES AVALOS		(2022073902)***  
***MAYRA FERNANDA CHIRE RAMOS			(2021072620)***  
***GABRIELA LUZKALID GUTIERREZ MAMANI 	(2022074263)***

**Tacna – Perú**  
***2025***

# **Proyecto Dashboard de Accidentes de Tránsito en Tacna**

# **Informe de Factibilidad**

# 

# **Versión *{1.0}***

| CONTROL DE VERSIONES |  |  |  |  |  |
| :---: | :---: | :---: | :---: | :---: | ----- |
| Versión | Hecha por | Revisada por | Aprobada por | Fecha | Motivo |
| 1.0 | MPV | ELV | ARV | 10/10/2020 | Versión Original |

**INDICE GENERAL**

[1\.	Descripción del Proyecto	3](#descripción-del-proyecto)

[2\.	Riesgos	3](#riesgos)

[3\.	Análisis de la Situación actual	3](#análisis-de-la-situación-actual)

[4\.	Estudio de Factibilidad	3](#estudio-de-factibilidad)

[4.1	Factibilidad Técnica	4](#factibilidad-técnica)

[4.2	Factibilidad económica	4](#factibilidad-económica)

[4.3	Factibilidad Operativa	4](#factibilidad-operativa)

[4.4	Factibilidad Legal	4](#factibilidad-legal)

[4.5	Factibilidad Social	5](#factibilidad-social)

[4.6	Factibilidad Ambiental	5](#factibilidad-ambiental)

[5\.	Análisis Financiero	5](#análisis-financiero)

[6\.	Conclusiones	5](#conclusiones)

**Informe de Factibilidad**

1. Descripción del Proyecto  
   1. Nombre del proyecto

		Dashboard de Accidentes de Tránsito en Tacna

2. Duración del proyecto

		3 meses

3. Descripción 

*El proyecto consiste en el desarrollo de un dashboard web interactivo para el monitoreo y análisis de los accidentes de tránsito en la ciudad de Tacna. Se implementarán técnicas de web scraping para la recopilación automatizada de información desde fuentes noticiosas digitales locales.*

*La importancia del proyecto radica en la necesidad de contar con información sistematizada y actualizada sobre la accidentalidad vial en Tacna, considerando que actualmente esta información se encuentra dispersa en diferentes medios de comunicación sin un análisis integral. El contexto actual muestra un crecimiento del parque automotor en la región de Tacna y un incremento en la preocupación ciudadana por la seguridad vial, especialmente en zonas urbanas con alta densidad de tráfico.*

*Este dashboard permitirá a autoridades locales, investigadores en seguridad vial y ciudadanos acceder a estadísticas consolidadas que faciliten la toma de decisiones informadas para implementar medidas preventivas y mejorar la infraestructura vial en los puntos más críticos de la ciudad.*

1.4 Objetivos

       1.4.1 Objetivo general

Desarrollar un dashboard interactivo para la visualización y análisis de accidentes de tránsito en la ciudad de Tacna, consolidando la información obtenida de fuentes noticiosas locales y presentándola en gráficos y mapas dinámicos que faciliten la interpretación de los datos.

        1.4.2 Objetivos Específicos

* Recopilar y organizar información de accidentes de tránsito en la ciudad de Tacna a partir de fuentes noticiosas digitales locales mediante técnicas de web scraping.


* Visualizar la distribución geográfica de los accidentes en un mapa interactivo que permita identificar las zonas con mayor incidencia.


* Presentar el recuento de accidentes por ubicación mediante gráficos que permitan comparar la frecuencia de eventos en distintos distritos o zonas.


* Analizar las fuentes de información utilizadas mostrando el recuento de noticias por medio de comunicación a través de gráficos de pie.

2. Riesgos

   Los principales riesgos que podrían afectar el éxito del proyecto incluyen aspectos técnicos, operacionales y de cronograma que requieren atención especial durante la implementación.

* Cambios en la estructura de sitios web de noticias: Los sitios web fuente pueden modificar su estructura HTML sin previo aviso, lo que afectaría directamente el funcionamiento del web scraper. Este riesgo tiene una probabilidad media pero un impacto alto, ya que podría interrumpir completamente la recolección de datos.  
* Bloqueo por parte de sitios web por scraping excesivo: Los portales noticiosos podrían implementar medidas anti-bot o bloquear el acceso si detectan actividad automatizada excesiva, especialmente si no se respetan los delays apropiados entre peticiones.  
* Insuficiencia de datos históricos para análisis temporal: Las fuentes noticiosas locales podrían no mantener archivos históricos suficientes, limitando la capacidad de generar análisis de tendencias a largo plazo y reduciendo el valor analítico del dashboard.  
* Problemas de conectividad y estabilidad del hosting: Fallos en la conexión a internet o problemas con el servidor de hosting podrían interrumpir la actualización automática de datos, afectando la confiabilidad del sistema.  
* Calidad inconsistente de la información noticiosa: La variabilidad en el detalle, precisión y formato de las noticias sobre accidentes podría generar datos incompletos o inconsistentes, afectando la calidad de los análisis y visualizaciones.  
* Limitaciones de rendimiento en Power BI Desktop: Si el volumen de datos crece más de lo esperado, Power BI Desktop podría presentar limitaciones de rendimiento, requiriendo migración a Power BI Service con costos adicionales.

3. Análisis de la Situación actual  
   1. Planteamiento del problema

La ciudad de Tacna ha experimentado un crecimiento significativo en su parque automotor, pasando de 95,000 vehículos en 2015 a más de 180,000 en 2024, según el Ministerio de Transportes y Comunicaciones. Este incremento del 89% ha superado el crecimiento de la infraestructura vial, generando congestión en arterias principales y un aumento del 25% en accidentes de tránsito reportados entre 2019 y 2023\.  
Actualmente, la información sobre accidentes se encuentra dispersa en múltiples fuentes sin coordinación: medios locales como Tacna21, El Peruano Regional y Radio Uno publican noticias de manera fragmentada, la Policía Nacional maneja estadísticas internas de acceso limitado, y la Municipalidad carece de un sistema integrado de monitoreo. Esta fragmentación impide identificar patrones temporales, geográficos y causales en los accidentes, resultando en decisiones reactivas por parte de las autoridades en lugar de medidas preventivas basadas en evidencia.  
La problemática se agrava por la falta de transparencia en el acceso a información consolidada, limitando la capacidad de ciudadanos, investigadores y autoridades para realizar análisis integral de la seguridad vial. Las intersecciones críticas como Avenida Bolognesi con Avenida Collpa y los accesos al Complejo Fronterizo concentran la mayor incidencia de siniestros, pero sin datos sistematizados es imposible implementar intervenciones focalizadas y efectivas.  
La necesidad de un sistema automatizado de monitoreo se vuelve crítica considerando que las proyecciones indican que el parque automotor podría superar los 250,000 vehículos para 2030, intensificando los riesgos viales existentes si no se implementan medidas preventivas fundamentadas en datos precisos y análisis sistemático.

2. Consideraciones de hardware y software

*{Hardware y software posibles para la implementación, se analizara lo que existe y es alcanzable, se evaluara que tecnología se puede utilizar en el proyecto.}*

4. Estudio de Factibilidad  
   1. Factibilidad Técnica

*El estudio de viabilidad técnica se enfoca en los recursos tecnológicos necesarios para el desarrollo del proyecto.*

*Hardware:*

* *Computadoras personales con procesadores de gama media (Intel i5 o Ryzen 5\) y mínimo 8 GB de RAM para desarrollo y pruebas.*  
* *Servidor en la nube (Azure, AWS o Google Cloud) para almacenamiento de datos y despliegue del dashboard.*  
* *Conectividad estable a internet de al menos 50 Mbps.*

*Software:*

* *Lenguaje de programación: Python (para web scraping).*  
* *Base de datos: SQLite o PostgreSQL para almacenar los datos extraídos.*  
* *Herramienta de visualización: Power BI (Desktop gratuito y posibilidad de migración a Power BI Service).*  
* *Navegadores compatibles: Google Chrome y Microsoft Edge.*  
* *Sistema Operativo: Windows 10/11 o Linux para servidores.*  
* *Infraestructura de red: acceso a internet con routers estables y disponibilidad de un dominio en caso de publicación web.*  
* *La tecnología requerida es accesible, ampliamente utilizada y compatible con los requerimientos del proyecto, lo que garantiza su viabilidad técnica.*

  2. Factibilidad Económica

     1. Costos Generales 

*Estos costos incluyen gastos operativos básicos necesarios para la implementación del sistema.*

| *Material* | *Cantidad* | *Precio Unitario* | *Total* |
| :---- | ----- | ----- | ----- |
| *Paquete de Hojas Bond* | *1* | *S/ 6.00* | *S/ 6.00* |
| *Lápices* | *3* | *S/ 1.60* | *S/ 4.80* |
| ***TOTAL*** |   |   | ***S/ 10.80*** |

*Tabla 01: En Costos Generales se detallan las utilidades con sus cantidades y precios haciendo un total de S/10.80*

2. Costos operativos durante el desarrollo 

| *Concepto* | *Duración* | *Costo Mensual* | *Costo Final* |
| :---- | ----- | ----- | ----- |
| *Internet*  | *3 meses* | *S/ 90* | *S/ 270* |
| *Energía eléctrica* | *3 meses* | *S/ 70* | *S/ 210* |
| ***TOTAL*** |   |   | ***S/ 480*** |

   3. Costos del ambiente

   *Se evalúan costos asociados al entorno tecnológico necesario para la implementación del sistema.*

   

   

   

|  *CONCEPTO* | *DURACIÓN* | *COSTO EN $* | *COSTO EN S/.* |
| :---- | :---- | :---- | :---- |
| *Azure Database for MySQL Flexible Server (azurerm\_mysql\_flexible\_server.mysql)* | *12 meses* | *$104.19* | *S/. 387.61* |
| *Licencia de PowerBl* | *12 meses* | *Gratuito* | *Gratuito* |
| ***Total*** |   | ***$104.19*** | ***S/. 387.61*** |

   

   

   

   

   

   

   

 


      4. Costos de personal

   

   *Estos costos corresponden al recurso humano necesario para desarrollar e implementar el sistema de gestión. Incluirán roles como:*

    

      *●  	Desarrolladores de software para construir y mantener el sistema.*

      *●  	Tester o QA para asegurar la calidad del sistema.*

      *●  	Diseñador UI/UX para crear una interfaz fácil de usar para el cliente y los empleados.* 

      

| *ROL* | *DURACIÓN DEL PROYECTO* | *COSTO POR MES* | *COSTO FINAL* |
| :---- | :---- | :---- | :---- |
| *DevOps* | *3 meses* | *S/. 1200* | *S/. 3600* |
| *Analista de datos* | *3 meses* | *S/. 1200* | *S/. 3600* |
| *Director de proyecto* | *3 meses* | *S/. 1300* | *S/. 3900* |
| ***Total*** |   |   | ***S/. 11100*** |

   

   

   

   

   

   

   

   

   

   

   

      5. Costos totales del desarrollo del sistema 

   

 


   

   

   

   

| CONCEPTO | COSTO SOLES |
| :---- | :---- |
| Costos Generales | S/ 10.80 |
| Costos Operativos | S/. 480 |
| Costos de Ambiente | S/. 387.61 |
| Costos de Personal | S/. 11200 |
| Total | S/. 11,100 |

   3. Factibilidad Operativa

*El dashboard generará un alto valor para usuarios internos y externos, brindando información clara y accesible para la toma de decisiones.*

*Beneficios principales:*

* *Facilita la consulta de datos en tiempo real sobre accidentes.*  
* *Mejora la planificación de la seguridad vial al identificar puntos críticos.*  
* *Promueve la transparencia y acceso a la información para la ciudadanía.*  
* *Reduce la dependencia de reportes manuales dispersos.*

*Capacidad de operación y mantenimiento:*

* *El cliente (Municipalidad o institución interesada) podrá mantener el sistema con personal mínimo capacitado en manejo de Power BI.*  
* *Actualizaciones automáticas mediante scripts de scraping programados.*

*Impacto en usuarios:*

* *Autoridades locales: apoyo en la planificación de medidas preventivas.*  
* *Investigadores y académicos: acceso a datos sistematizados para estudios.*  
* *Ciudadanía: acceso a información clara que promueve conciencia vial.*

*Interesados:*

* *Municipalidad Provincial de Tacna.*  
* *Policía Nacional del Perú.*  
* *Universidades locales (investigadores en seguridad vial).*  
* *Medios de comunicación*  
* *Ciudadanía en general.*

  4. Factibilidad Legal

*El proyecto cumple con la normativa vigente en Perú y no presenta conflictos legales:*

* *No recopila información privada ni confidencial, ya que el scraping se aplicará únicamente sobre fuentes públicas y noticiosas.*  
* *Cumple con la Ley de Protección de Datos Personales (Ley N.° 29733), dado que los datos se limitan a estadísticas de accidentes y no incluyen información sensible de personas.*  
* *Respeta derechos de autor al no reproducir contenido noticioso íntegro, sino extraer datos estructurados para análisis.*  
* *Compatible con las regulaciones municipales y nacionales en materia de seguridad vial y transparencia de datos.*

  5. Factibilidad Social

*El proyecto tiene un impacto positivo en la sociedad, ya que:*

* *Promueve la conciencia ciudadana sobre la importancia de la seguridad vial.*  
* *Favorece la transparencia en la información pública, acercando a la población datos que antes eran dispersos o de difícil acceso.*  
*   
* *Contribuye al fortalecimiento de la cultura de prevención en movilidad urbana.*  
* *Apoya a la comunidad académica con datos confiables para investigaciones sociales y urbanísticas.*  
* *No se identifican impactos negativos en el ámbito cultural o social.*

  6. Factibilidad Ambiental

*El proyecto no genera impacto ambiental directo, ya que se basa en el desarrollo de un sistema digital.*

* *No implica uso de infraestructura física significativa que incremente la huella de carbono.*  
* *Contribuye indirectamente al medio ambiente al **reducir la congestión vehicular** mediante la identificación de zonas críticas de accidentalidad, lo cual puede inspirar intervenciones que mejoren la movilidad y disminuyan emisiones contaminantes.*  
    
5. Análisis Financiero

   1. Justificación de la Inversión

*5.1.1 Beneficios* del Proyecto

*Beneficios tangibles:*

* *Disponibilidad de información consolidada y actualizada.*

  * *Reducción de costos en la recopilación manual de datos.*

    * *Disminución en el tiempo de análisis para la elaboración de reportes.*

      * *Optimización de recursos municipales en seguridad vial.*

        

  *Beneficios intangibles:*

      * *Mayor transparencia en la gestión de información pública.*

      * *Incremento en la confianza ciudadana hacia las autoridades locales.*

      * *Fomento de la conciencia y educación vial en la población.*

      * *Soporte a la comunidad académica en investigaciones.*

      * *Generación de ventaja competitiva para la ciudad en términos de innovación y modernización de la gestión urbana.*

5.1.2 Criterios de Inversión

		![][image2]

*5.1.2.1 Relación Beneficio/Costo (B/C)*

*La relación Beneficio/Costo (B/C) del proyecto es de 1.05 Este ratio compara el valor presente de los beneficios con el valor presente de los costos. Un B/C mayor que 1, como en este caso, indica que los beneficios superan ampliamente los costos, lo que hace que el proyecto sea económicamente viable y atractivo para su ejecución.*

                    *5.1.2.2 Valor Actual Neto (VAN)*  
*El VAN es de S/* 11,645.38*. Esto significa que después de descontar los flujos de efectivo futuros a una tasa de descuento del 10%, el valor presente neto de los ingresos esperados del proyecto es positivo. Un VAN positivo indica que el proyecto generará más valor del que cuesta, por lo tanto, es financieramente viable y debería ser considerado para su implementación.*

*5.1.2.3 Tasa Interna de Retorno (TIR)*

*La Tasa Interna de Retorno (TIR) del proyecto es de 13%, lo cual supera significativamente la tasa de descuento utilizada del 10%. Esta tasa refleja una alta rentabilidad sobre la inversión realizada. Una TIR mayor que la tasa de oportunidad indica que el proyecto no solo es viable, sino que ofrece un retorno atractivo con respecto al riesgo asumido.*

6. Conclusiones

*El proyecto de implementación de un dashboard de monitoreo de accidentes de tránsito en Tacna es técnica y operativamente viable, ya que requiere hardware y software accesibles, de bajo costo y con tecnologías ampliamente disponibles en el mercado.*

*Desde el punto de vista económico y financiero, los resultados del análisis muestran un VAN positivo (S/ 11,645.38), una TIR del 13% y una relación B/C mayor a 1 (1.05), lo que confirma que los beneficios del sistema superan los costos, garantizando rentabilidad y sostenibilidad de la inversión.*

*El sistema propuesto no solo genera beneficios tangibles, como reducción de costos y tiempos en la gestión de datos, sino también beneficios intangibles, como mayor transparencia, apoyo a la investigación académica y fortalecimiento de la confianza ciudadana, contribuyendo a la prevención de accidentes y mejora de la seguridad vial en Tacna.*

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,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>