# Guión de Sustentación: Arquitectura Medallion, Data Mesh y Gobierno de Datos
## Maestría en Ingeniería de Datos - Proyecto Final (Grupo G2 - Sección 1 / Código g102)

Este guión contiene el discurso, los puntos clave y los argumentos técnicos para sustentar de manera exitosa tu proyecto final de Maestría ante el jurado, alineado de forma exacta slide a slide con la plantilla oficial de UTEC Posgrado (**Why - What - How - When**).

---

## PARTE 1: PRESENTACIÓN EJECUTIVA (CDO y Stakeholders del Negocio)
*Enfoque: Directo, sin texto denso, destacando el valor económico y operativo.*

### 🛝 Slide 1: Portada
* **Texto visual:** CURSO | INGENIERÍA DE DATOS | Proyecto Final - Grupo G2
* **Discurso del Expositor:**
  > "Buenas noches, estimados miembros del jurado y patrocinadores del negocio. Hoy les presentaremos nuestro proyecto final para el curso de Ingeniería de Datos. Nuestra propuesta implementa una arquitectura descentralizada de Data Mesh y Data Products para resolver un desafío logístico real mediante cómputo en la nube con Databricks."

### 🛝 Slide 2: Contexto Organizacional
* **Texto visual:** Compañía Minera Los Andes S.A. | RUBRO: Minería polimetálica
* **Discurso del Expositor:**
  > "Nuestra organización de estudio es la Compañía Minera Los Andes S.A., una empresa minera que opera a tajo abierto. Un pilar de nuestra continuidad operativa es el modelo de inventarios en consignación, donde materiales muy costosos y críticos para el tajo y la planta concentradora pertenecen físicamente a los proveedores en nuestros almacenes hasta que los consumimos."

### 🛝 Slide 3: Ejemplos - Datasets por rubro
* **Texto visual:** Tabla comparativa destacando el rubro Logística (MB51_*.csv, maestro_materiales.csv)
* **Discurso del Expositor:**
  > "En la industria de datos, cada rubro opera con transacciones muy específicas. A diferencia del comercio minorista o la salud, en la minería la continuidad depende de la logística pesada. Para este proyecto final, hemos procesado la data transaccional real de movimientos de stock de SAP (archivos MB51) y la cruzamos con un catálogo maestro de materiales diseñado por nuestro equipo para representar fielmente el negocio."

### 🛝 Slide 4: WHY -- ¿Por qué estamos haciendo esto?
* **Texto visual:** Objetivos, necesidades, dolor en mina e impacto financiero
* **Discurso del Expositor:**
  > "La justificación de este proyecto responde a tres dolores reales en la operación minera:
  > Primero, el riesgo de desabastecimiento: si nos quedamos sin repuestos críticos, detener la planta concentradora cuesta millones de dólares en lucro cesante.
  > Segundo, el exceso de inventario preventivo que inmoviliza capital de trabajo de forma ineficiente.
  > Y tercero, la lentitud en la facturación mensual por diferencias entre SAP y los proveedores.
  > La solución mediante un Data Product permite automatizar y dar visibilidad predictiva a la reposición de stock, generando eficiencia y ahorrando costos de compras de emergencia."

### 🛝 Slide 5: WHAT -- Qué estamos construyendo?
* **Texto visual:** Los 3 Data Products, consumidores, SLOs y Dashboard final
* **Discurso del Expositor:**
  > "Construimos un portal que publica 3 Data Products con propósitos y consumidores de negocio muy claros:
  > 1. Alertas de Reabastecimiento Crítico (DP1) para los planeadores de mantenimiento.
  > 2. Análisis de Consumo y Costos en USD (DP2) para la gerencia de contratos.
  > 3. Reporte de Conciliación de Pagos Mensuales (DP3) para Finanzas y proveedores.
  > Estos productos garantizan niveles de servicio (SLOs) de actualización diaria, latencias de consulta interactiva menores a 2.5 segundos, y se consumen visualmente desde un Dashboard con filtros ágiles por proveedor."

### 🛝 Slide 6: HOW -- Cómo lo estamos implementando?
* **Texto visual:** Pipeline Medallion, Unity Catalog, validaciones de calidad y contratos YAML
* **Discurso del Expositor:**
  > "Técnicamente, implementamos una arquitectura Medallion en Databricks:
  > * En la capa Bronze, normalizamos las columnas crudas de SAP para evitar caracteres especiales inválidos en Delta Lake.
  > * En la capa Silver, aplicamos lógica resiliente con `try_cast` y `try_to_date` para neutralizar datos corruptos, enviándolos automáticamente a una tabla de Cuarentena, y aplicamos enmascaramiento dinámico de usuarios responsables (PII) mediante Unity Catalog.
  > * En la capa Gold, publicamos los Data Products gobernados bajo contratos de datos en formato YAML."

### 🛝 Slide 7: WHEN -- Cuál es el estado y próximos pasos?
* **Texto visual:** Mínimo Viable (MVP), Databricks Workflows, desafíos y plan
* **Discurso del Expositor:**
  > "Actualmente, el pipeline completo de Bronze a Gold está operativo al 100\% y orquestado mediante un Job de Databricks Workflows diario a las 01:00 AM. Nuestro plan a corto plazo consiste en integrar las alertas de reorden directamente con el canal logístico de Microsoft Teams y enrolar progresivamente a nuevos proveedores mineros en la capa de consumo."

### 🛝 Slide 8: Recomendaciones Finales
* **Texto visual:** Enfoque en valor, lenguaje Data Mesh, trazabilidad y lección de diseño
* **Discurso del Expositor:**
  > "Para finalizar esta sección, nuestras recomendaciones se centran en anteponer siempre el valor del negocio a la complejidad de la infraestructura. Hemos adoptado el lenguaje común de Data Mesh (dominios, contratos de datos y dueños operativos de la data), demostrando que el objetivo principal no es solo procesar grandes volúmenes de datos, sino crear productos de datos limpios, útiles y altamente gobernados que mejoren la toma de decisiones."

### 🛝 Slide 9: Cierre (Preguntas o comentarios)
* **Texto visual:** ¡Gracias!
* **Discurso del Expositor:**
  > "Muchas gracias por su atención. Quedamos a su disposición para cualquier pregunta técnica o de negocio sobre nuestra solución."

---

## PARTE 2: SUSTENTACIÓN TÉCNICA (Ronda de Preguntas del Jurado)

* **Pregunta 1: ¿Por qué usaron vistas estándar en lugar de vistas materializadas en la capa Gold?**
  * *Respuesta:* "Por diseño de arquitectura propusimos vistas materializadas para pre-calcular los agregados y optimizar consultas. Sin embargo, para la demostración en el clúster interactivo de la clase (`DSAI's Cluster`), cambiamos a vistas lógicas estándar. La razón es que la interfaz interactiva de Databricks restringe el cómputo DBSQL Pro/Serverless necesario para crear vistas materializadas. En un entorno de producción real, activaríamos una SQL Warehouse Serverless para materializarlas físicamente."
* **Pregunta 2: ¿Cómo maneja la arquitectura la data corrupta o malformada de SAP?**
  * *Respuesta:* "Mediante el uso de `try_cast` y `try_to_date` en PySpark en la capa Silver. Si un registro tiene un texto de fecha dañado o campos desalineados, Spark retorna `NULL` de manera silenciosa en lugar de tumbar la tarea del Workflow. Posteriormente, la regla de calidad evalúa si hay nulos y desvía esos registros erróneos directamente a la tabla `silver_movimientos_cuarentena` para su auditoría, dejando pasar solo los registros limpios a producción."
* **Pregunta 3: ¿Cómo aseguraron la protección de datos personales (PII)?**
  * *Respuesta:* "El campo del usuario responsable de almacén (`User Name`) es un dato sensible. Implementamos una UDF SQL en Unity Catalog en la capa Silver. Esta función evalúa dinámicamente el grupo de seguridad de la sesión con `is_member('utec-admin')`. Si un analista general consulta la tabla, el motor de Databricks aplica la función `mask()` y le oculta la información en tiempo de ejecución de manera transparente."
* **Pregunta 4: ¿Por qué mockearon los SLAs de auditoría de consultas?**
  * *Respuesta:* "El catálogo de tablas del sistema de Databricks (`system.query.history`) requiere permisos de Account Administrator en Unity Catalog, los cuales están deshabilitados para las cuentas de estudiantes en el workspace compartido del curso por razones de privacidad general. Para cumplir con el diseño, mockeamos la estructura con una vista que simula el rendimiento y volumen de consultas reales esperados para nuestros Data Products."
