from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta
import os
import shutil

# Rutas locales dentro del entorno Airflow
INPUT_DIR = '/opt/airflow/dags/datos_simulados/input'
ARCHIVE_DIR = '/opt/airflow/dags/datos_simulados/archive'
CONTAINER_NAME = 'raw'
BLOB_FOLDER = 'inventario_consignacion'

def upload_sap_data_to_azure(**kwargs):
    """
    Escanea la bandeja de entrada local en búsqueda de exportaciones de SAP,
    las sube al contenedor raw de Azure Storage, y traslada los archivos procesados
    al archivo histórico local.
    """
    azure_hook = WasbHook(wasb_conn_id='wasb_default')
    
    if not os.path.exists(INPUT_DIR):
        print(f"[ALERTA] Directorio de entrada no existe: {INPUT_DIR}")
        return
        
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Filtrar archivos con prefijo 'sap_consignacion_' y extensión '.csv'
    files = [f for f in os.listdir(INPUT_DIR) if f.startswith('sap_consignacion_') and f.endswith('.csv')]
    
    if not files:
        print("No se encontraron nuevos archivos de inventario SAP para procesar.")
        return
        
    print(f"Se encontraron {len(files)} archivo(s) para procesar.")
    
    for file_name in files:
        local_path = os.path.join(INPUT_DIR, file_name)
        blob_path = f"{BLOB_FOLDER}/{file_name}"
        
        try:
            print(f"Iniciando subida de '{file_name}' a Azure ADLS en '{blob_path}'...")
            
            # Cargar archivo a Azure Blob Storage
            azure_hook.load_file(
                file_path=local_path,
                container_name=CONTAINER_NAME,
                blob_name=blob_path,
                overwrite=True
            )
            print(f"[EXITO CLOUD] Archivo '{file_name}' subido correctamente a Azure.")
            
            # Mover a la carpeta de archivo histórico local para liberar la bandeja de entrada
            archive_path = os.path.join(ARCHIVE_DIR, file_name)
            shutil.move(local_path, archive_path)
            print(f"[EXITO LOCAL] Archivo '{file_name}' movido a histórico local: {archive_path}")
            
        except Exception as e:
            print(f"[ERROR] Error procesando el archivo '{file_name}': {str(e)}")
            raise e

default_args = {
    'owner': 'mining-logistics-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'telco_mina_consignacion_ingestion_dag',
    default_args=default_args,
    description='DAG para la ingesta de inventario de materiales en consignación minera desde SAP',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. FileSensor: Monitorea si hay archivos CSV con patrón 'sap_consignacion_*.csv'
    wait_for_sap_file = FileSensor(
        task_id='esperar_csv_sap_consignacion',
        filepath='sap_consignacion_*.csv',
        fs_conn_id='fs_default',
        poke_interval=30,      # Tiempo entre chequeos (en segundos)
        timeout=300,            # Tiempo de espera máximo (5 minutos)
        mode='poke'
    )

    # 2. PythonOperator: Sube los archivos a Azure ADLS y los archiva localmente
    upload_and_archive = PythonOperator(
        task_id='subir_a_azure_y_archivar_consignacion',
        python_callable=upload_sap_data_to_azure
    )

    wait_for_sap_file >> upload_and_archive
