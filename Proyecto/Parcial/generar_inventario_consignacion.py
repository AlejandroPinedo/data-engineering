import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Directorios de la simulación
INPUT_DIR = "datos_simulados/input"
ARCHIVE_DIR = "datos_simulados/archive"
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Catálogo de materiales consignados de minería
materiales_catalogo = [
    {"id_material": "MAT-001", "descripcion": "Broca de Perforacion 12 Pulgadas Tritonica", "categoria": "Herramientas de Perforacion", "proveedor": "Ferreyros S.A.", "costo": 1250.00},
    {"id_material": "MAT-002", "descripcion": "Filtro de Aire Camion Minero CAT 797F", "categoria": "Repuestos Motor", "proveedor": "Ferreyros S.A.", "costo": 380.50},
    {"id_material": "MAT-003", "descripcion": "Filtro de Aceite Hidraulico Komatsu 930E", "categoria": "Repuestos Hidraulicos", "proveedor": "Komatsu Mitsui", "costo": 420.00},
    {"id_material": "MAT-004", "descripcion": "Respirador de Doble Via 3M Serie 6200", "categoria": "Seguridad e Higiene (EPP)", "proveedor": "3M Peru S.A.", "costo": 45.00},
    {"id_material": "MAT-005", "descripcion": "Cartucho para Gases Acidose Inorganicos 3M", "categoria": "Seguridad e Higiene (EPP)", "proveedor": "3M Peru S.A.", "costo": 18.20},
    {"id_material": "MAT-006", "descripcion": "Lubricante Multigrado Mobil Delvac 15W-40 (Balde)", "categoria": "Lubricantes y Grasas", "proveedor": "Mobil Distribuidora", "costo": 180.00},
    {"id_material": "MAT-007", "descripcion": "Sensor de Temperatura Motor Cummins QSK60", "categoria": "Sensores y Electricos", "proveedor": "Komatsu Mitsui", "costo": 290.00},
    {"id_material": "MAT-008", "descripcion": "Zapatas de Oruga Tractor CAT D11T", "categoria": "Repuestos Tren de Rodaje", "proveedor": "Ferreyros S.A.", "costo": 4500.00}
]

data = {
    "id_material": [],
    "descripcion": [],
    "categoria": [],
    "proveedor_dueno": [],
    "cantidad_consignada": [],
    "cantidad_minima_seguridad": [],
    "unidad_medida": [],
    "costo_unitario_usd": [],
    "ubicacion_almacen": [],
    "fecha_actualizacion": []
}

current_time = datetime.now()

# Generar 30 registros diarios de stock en consignación
for i in range(30):
    mat = random.choice(materiales_catalogo)
    
    # Simular niveles de stock. Ocasionalmente forzar estado de desabastecimiento (por debajo del mínimo)
    cant_minima = random.randint(10, 50)
    is_under_stock = random.random() < 0.20 # 20% de probabilidad de alerta
    
    if is_under_stock:
        cant_actual = random.randint(0, int(cant_minima * 0.8)) # Menor al stock mínimo
    else:
        cant_actual = random.randint(cant_minima, cant_minima * 3)
        
    data["id_material"].append(mat["id_material"])
    data["descripcion"].append(mat["descripcion"])
    data["categoria"].append(mat["categoria"])
    data["proveedor_dueno"].append(mat["proveedor"])
    data["cantidad_consignada"].append(cant_actual)
    data["cantidad_minima_seguridad"].append(cant_minima)
    data["unidad_medida"].append("Unidades" if mat["id_material"] != "MAT-006" else "Baldes")
    data["costo_unitario_usd"].append(mat["costo"])
    data["ubicacion_almacen"].append(f"ALM-{random.choice(['A', 'B', 'C'])}{random.randint(1, 10)}")
    data["fecha_actualizacion"].append(current_time.strftime('%Y-%m-%d %H:%M:%S'))

df = pd.DataFrame(data)

# Introducir anomalías ficticias para la capa de procesamiento posterior (Medallion):
# 1. Un registro duplicado
df.loc[len(df)] = df.iloc[0]

# 2. Un registro con Proveedor Nulo (suciedad de SAP)
df.loc[len(df)] = [
    "MAT-099", "Filtro Generico Cabina CAT", "Repuestos Motor", None, 5, 10, "Unidades", 150.00, "ALM-Z9", current_time.strftime('%Y-%m-%d %H:%M:%S')
]

# 3. Un registro con cantidad negativa (error de ingreso)
df.loc[len(df)] = [
    "MAT-002", "Filtro de Aire Camion Minero CAT 797F", "Repuestos Motor", "Ferreyros S.A.", -5, 15, "Unidades", 380.50, "ALM-A2", current_time.strftime('%Y-%m-%d %H:%M:%S')
]

filename = f"sap_consignacion_{current_time.strftime('%Y%m%d_%H%M%S')}.csv"
filepath = os.path.join(INPUT_DIR, filename)
df.to_csv(filepath, index=False)

print(f"[EXITO] Archivo de inventario de consignación SAP simulado creado en: {filepath}")
print(df.tail(4))
