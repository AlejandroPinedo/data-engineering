# Pipeline de Ingesta y Control de Inventario en Consignación Minera

Este repositorio contiene el código del avance del proyecto integrador para el curso de **Ingeniería de Datos** de la Maestría en Ciencia de Datos e Inteligencia Artificial (**UTEC**).

## 1. Integrantes del Grupo
* [Integrante 1 - Nombre y Apellidos]
* [Integrante 2 - Nombre y Apellidos]
* [Integrante 3 - Nombre y Apellidos]
* [Integrante 4 - Nombre y Apellidos]
* [Integrante 5 - Nombre y Apellidos]

---

## 2. Contexto y Problema de Negocio

En las grandes operaciones mineras en Perú, la disponibilidad de herramientas, consumibles de seguridad (EPP) y repuestos mecánicos críticos es vital para no detener la producción. Detener una planta concentradora o un frente de minado por la falta de un repuesto crítico puede generar pérdidas financieras masivas (entre **$10,000 y $50,000 USD por hora**).

Para optimizar el capital de trabajo, la mina opera bajo un esquema de **inventario en consignación**: los materiales se almacenan físicamente en la mina, pero siguen perteneciendo legalmente a proveedores clave (como Ferreyros, Komatsu, 3M) hasta que la mina los retira para usarlos, momento en el cual se efectúa la compra automática.

### El Problema
Actualmente, los niveles de stock en consignación se gestionan de forma fragmentada en SAP. Los reportes de stock son exportados manualmente por el área de logística de la mina y se envían a los proveedores por correo de forma semanal o quincenal. Este desfase de tiempo genera:
* **Desabastecimiento (Stockouts):** El proveedor no se entera a tiempo de que el stock cayó por debajo de los límites de seguridad, provocando demoras de abastecimiento y paradas operativas no programadas.
* **Sobrestock preventivo:** Para evitar multas, los proveedores inflan los niveles de stock, saturando el espacio físico del almacén minero.
* **Falta de trazabilidad y automatización:** Carga administrativa manual ineficiente.

### La Solución
Implementar un pipeline de datos diario y automático que extraiga los reportes de inventario de SAP en local y los cargue de manera inmutable al contenedor `raw` en la nube (**Azure Data Lake Storage Gen2**), utilizando **Apache Airflow** como orquestador. 

Esto sienta las bases para estructurar las capas **Bronze, Silver y Gold** (Arquitectura Medallion) con el fin de compartir un Datamart de stock en tiempo real y emitir alertas proactivas de reabastecimiento directamente a los proveedores.

---

## 3. Arquitectura del Pipeline

El flujo de datos sigue un diseño conceptual desacoplado para garantizar escalabilidad, seguridad e inmutabilidad:

```
+------------------------+      +---------------------------------+      +------------------------+
|   Servidor SAP Mina    |      |          Apache Airflow         |      |       Azure Cloud      |
| (Simulador Directorio) |      |           (Orquestador)         |      |    (Data Lake ADLS)    |
|                        |      |                                 |      |                        |
|  +------------------+  |      |  +---------------------------+  |      |  +------------------+  |
|  | Carpeta de Datos |  | ---> |  |  FileSensor               |  |      |  | Contenedor: raw  |  |
|  |  (datos/input/)  |  |      |  |  (Monitorea CSV de SAP)   |  |      |  +------------------+  |
|  +------------------+  |      |  +---------------------------+  |      |            ^           |
+------------------------+      |                |                |      |            |           |
                                |                v                |      |            |           |
                                |  +---------------------------+  |      |            |           |
                                |  | PythonOperator (WasbHook) |  |------+------------+           |
                                |  | (Carga a Azure Storage)   |  | (Ingesta segura a raw/)
                                |  +---------------------------+  |                                
                                |                |                |                                
                                |                v                |                                
                                |  +---------------------------+  |                                
                                |  | PythonOperator (Archive)  |  |                                
                                |  | (Mueve local a /archive)  |  |                                
                                |  +---------------------------+  |                                
                                +---------------------------------+                                
```

---

## 4. Estructura del Proyecto

```text
├── dags/
│   └── inventario_consignacion_dag.py     # DAG oficial de Airflow para el pipeline de almacén
├── datos_simulados/
│   ├── archive/                           # Repositorio local de archivos de SAP ya procesados
│   └── input/                             # Directorio de entrada donde caen los archivos crudos de SAP
├── generar_inventario_consignacion.py     # Script de Python que simula exportación de stock de SAP
├── Propuesta_Avance_Proyecto.ipynb        # Notebook con la propuesta formal del proyecto
└── README.md                              # Documentación del proyecto (este archivo)
```

---

## 5. Instrucciones de Despliegue Local

### Paso 1: Crear Rama de Trabajo (Git Flow)
Crea una rama de trabajo para el desarrollo del feature aislada de la rama principal:
```bash
git checkout -b feature/consignment-inventory-ingestion
```

### Paso 2: Ejecutar el Generador de Datos
Simula la exportación diaria de datos de SAP ejecutando el script:
```bash
python3 generar_inventario_consignacion.py
```
Esto creará el archivo CSV estructurado bajo `datos_simulados/input/sap_consignacion_YYYYMMDD_HHMMSS.csv`.

### Paso 3: Configurar Conexiones en Apache Airflow (UI)
Ingresa al panel administrativo de Airflow (`http://localhost:8080`), navega a **Admin -> Connections** y configura:

1. **`fs_default` (File Connection):**
   * **Conn Type:** File (path)
   * **Host:** `/opt/airflow/dags/datos_simulados/input` (o la ruta absoluta de la carpeta input montada en tu entorno Docker).

2. **`wasb_default` (Azure Blob Storage Connection):**
   * **Conn Type:** Azure Blob Storage
   * **Extra:** `{"connection_string": "<Tu cadena de conexión de azure_connection.txt>"}`

### Paso 4: Encender el DAG
Activa el DAG `telco_mina_consignacion_ingestion_dag` en la consola de Airflow. El sensor detectará el archivo CSV en la carpeta local, lo cargará al contenedor `raw` en la carpeta `inventario_consignacion/` de Azure Blob Storage y, tras la subida exitosa, trasladará el archivo local procesado a `datos_simulados/archive/`.

---

## 6. Procesamiento Medallion (Fase Siguiente)

* **Bronze (Capa de Ingesta):** Consolidación histórica 1:1 de los CSVs cargados en Azure, agregando metadatos técnicos de control (`ingested_at` y `file_source_name`).
* **Silver (Capa de Calidad):** Deduplicación de registros basados en `id_material` + `fecha_actualizacion`, casteo a tipos correctos (Double, Integer, Timestamp), limpieza de nulos en `proveedor_dueno` y desvío automático de registros corruptos (ej. cantidades en consignación negativas) a una tabla de **Cuarentena** para auditoría.
* **Gold (Capa de Negocio/Proveedores):** Creación de un Datamart estructurado (`gold_reordenamiento_inventario`) que filtre materiales cuyo nivel de stock actual esté por debajo del mínimo de seguridad (`cantidad_consignada < cantidad_minima_seguridad`) y consolide el volumen necesario a reponer por cada proveedor (`Ferreyros`, `Komatsu`, `3M`), sirviendo como backend para tableros de PowerBI y notificaciones por correo automáticas.
