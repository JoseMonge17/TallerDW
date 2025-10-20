import pandas as pd
import pyodbc
from datetime import datetime

# ========================
# CONFIGURACIÓN
# ========================
EXCEL_PATH = r"D:\VsCode\TallerDW\TipoCambioUSD.xlsx"  # Ruta del archivo Excel
SHEET_NAME = "Sheet1"  # Nombre de la hoja
CONNECTION_STRING = (
    r"DRIVER={ODBC Driver 18 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=DW_Sales;"
    r"Trusted_Connection=yes;"
    r"TrustServerCertificate=yes;"
)

# ========================
# LEER EL EXCEL
# ========================
print("Leyendo archivo Excel...")
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# Normalizar nombres de columnas
df.columns = [c.strip().lower() for c in df.columns]

if 'fecha' not in df.columns or 'tipocambio_usd_crc' not in df.columns:
    raise Exception("El archivo debe tener las columnas 'Fecha' y 'TipoCambio_USD_CRC'.")

# ========================
# LIMPIAR DATOS
# ========================
print("Limpiando datos...")

# Convertir fecha
df['fecha_limpia'] = pd.to_datetime(df['fecha'], errors='coerce')

# Limpiar valores de tipo de cambio
df['tipo_cambio'] = (
    df['tipocambio_usd_crc']
    .astype(str)
    .str.replace(',', '.', regex=False)
    .astype(float)
)

# Eliminar filas inválidas
valid_rows = df.dropna(subset=['fecha_limpia', 'tipo_cambio'])
invalid_rows = df[df['fecha_limpia'].isna() | df['tipo_cambio'].isna()]

print(f"Filas válidas: {len(valid_rows)} | Filas inválidas: {len(invalid_rows)}")

# ========================
# CONECTAR A SQL SERVER
# ========================
print("Conectando a SQL Server...")
conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()


# ========================
# INSERTAR DATOS NUEVOS
# ========================
print("Insertando nuevos registros...")

insert_query = """
INSERT INTO dbo.DIM_Time (
    [date], [day], [month], [year],
    [tipoCambio], [numberMonth], [quarter]
)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

rows_to_insert = []
for _, row in valid_rows.iterrows():
    fecha = row['fecha_limpia']
    tipo_cambio = row['tipo_cambio']
    rows_to_insert.append((
        fecha.date(),
        fecha.day,
        fecha.strftime("%B"),
        fecha.year,
        tipo_cambio,
        fecha.month,
        (fecha.month - 1) // 3 + 1
    ))

cursor.executemany(insert_query, rows_to_insert)
conn.commit()

print(f"{len(rows_to_insert)} filas insertadas correctamente en DIM_Time.")

# ========================
# MOSTRAR FILAS INVALIDAS
# ========================
if len(invalid_rows) > 0:
    print("\n Filas que no se pudieron procesar:")
    print(invalid_rows)

# ========================
# 7CERRAR CONEXIÓN
# ========================
cursor.close()
conn.close()
print("\nProceso ETL completado exitosamente.")
