import pandas as pd
import json
import pyodbc
from datetime import datetime

# ========================
# CONFIGURACIÓN
# ========================
JSON_PATH = r"C:\Users\djmon\OneDrive\Documentos\DW_Dummy\ventas_resumen_2024_2025.json"
CONNECTION_STRING = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=DW_Sales;"
    r"Trusted_Connection=yes;"
    r"TrustServerCertificate=yes;"
)

# ========================
# LEER EL ARCHIVO JSON
# ========================
print("Leyendo archivo JSON...")
with open(JSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

rows = []
for periodo in data:
    anio = periodo.get("anio")
    mes = periodo.get("mes")
    for venta in periodo.get("ventas", []):
        rows.append({
            "anio": anio,
            "mes": mes,
            "itemCode": venta.get("item"),
            "cantidad": venta.get("cantidad"),
            "precio": venta.get("precio")
        })

df = pd.DataFrame(rows)
print(f"{len(df)} registros cargados desde el JSON.")

# ========================
# TRANSFORMACIÓN
# ========================
print("Transformando datos...")

df["MontoTotalUSD"] = df["cantidad"].astype(float) * df["precio"].astype(float)
df["fecha_limpia"] = pd.to_datetime(df["anio"].astype(str) + "-" + df["mes"].astype(str) + "-01")

valid_rows = df.dropna(subset=["itemCode", "cantidad", "precio", "MontoTotalUSD"])
invalid_rows = df[df.isna().any(axis=1)]
print(f"Filas válidas: {len(valid_rows)} | Filas inválidas: {len(invalid_rows)}")

# ========================
# CONECTAR A SQL SERVER
# ========================
print("Conectando a SQL Server...")
conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()

# ========================
# OBTENER LLAVES DE DIMENSIONES
# ========================

# -- CLIENTE DUMMY
cursor.execute("SELECT idCustomer FROM dbo.DIM_Customer WHERE cardCode = 'DUMMY_JSON'")
row = cursor.fetchone()
if row:
    idCustomer = row[0]
else:
    cursor.execute("""
        INSERT INTO dbo.DIM_Customer (cardCode, cardName, cardType, country, zone)
        VALUES ('DUMMY_JSON', 'Cliente JSON', 'C', 'XXX', 'Desconocido')
    """)
    conn.commit()
    cursor.execute("SELECT idCustomer FROM dbo.DIM_Customer WHERE cardCode = 'DUMMY_JSON'")
    idCustomer = cursor.fetchone()[0]
print(f"idCustomer (Cliente JSON): {idCustomer}")

# -- SALESPERSON DUMMY
cursor.execute("SELECT idSalesPerson FROM dbo.DIM_SalesPerson WHERE slpCode = -1")
row = cursor.fetchone()
if row:
    idSalesPerson = row[0]
else:
    cursor.execute("""
        INSERT INTO dbo.DIM_SalesPerson (idSalesPerson, slpCode, slpName)
        VALUES (1, -1, 'Vendedor JSON')
    """)
    conn.commit()
    idSalesPerson = 1
print(f"idSalesPerson (Vendedor JSON): {idSalesPerson}")

# ========================
# PREPARAR INSERCIÓN A FACT_SALES
# ========================
insert_query = """
INSERT INTO dbo.FACT_Sales (
    idTime, idCustomer, idProduct, idSalesPerson,
    quantity, price, docTotal, doctTotalFC
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# ========================
# PROCESAR FILAS
# ========================
print("Insertando datos en FACT_Sales...")
filas_insertadas = 0

for _, row in valid_rows.iterrows():
    item_code = row["itemCode"]
    fecha = row["fecha_limpia"]

    # -- Verificar si existe el producto
    cursor.execute("SELECT idProduct FROM dbo.DIM_Product WHERE itemCode = ?", item_code)
    prod_row = cursor.fetchone()

    if not prod_row:
        cursor.execute("""
            INSERT INTO dbo.DIM_Product (itemCode, itemName, brand, onHand, avgPrice, cardCode, whsCode, whsName)
            VALUES (?, ?, 'Desconocido', 0, ?, 'DUMMY', 'WH01', 'Warehouse JSON')
        """, item_code, f"Producto {item_code}", float(row["precio"]))
        conn.commit()

        cursor.execute("SELECT idProduct FROM dbo.DIM_Product WHERE itemCode = ?", item_code)
        prod_row = cursor.fetchone()

    idProduct = prod_row[0]

    # -- Obtener idTime y tipo de cambio
    cursor.execute("""
        SELECT TOP 1 idTime, tipoCambio FROM dbo.DIM_Time
        WHERE YEAR([date]) = ? AND MONTH([date]) = ?
    """, fecha.year, fecha.month)
    time_row = cursor.fetchone()
    if not time_row:
        print(f"Fecha {fecha.strftime('%Y-%m')} no encontrada en DIM_Time. Saltando fila.")
        continue

    idTime = time_row[0]
    tipo_cambio = float(time_row[1])

    # ========================
    # Calcular montos finales
    # ========================
    cantidad = int(row["cantidad"])
    precio_usd = float(row["precio"])
    doc_total_fc = float(row["MontoTotalUSD"])         # USD
    doc_total_crc = doc_total_fc * tipo_cambio         # Convertido a colones

    cursor.execute(insert_query, (
        idTime, idCustomer, idProduct, idSalesPerson,
        cantidad, precio_usd, doc_total_crc, doc_total_fc
    ))
    filas_insertadas += 1

conn.commit()
print(f"{filas_insertadas} filas insertadas correctamente en FACT_Sales.")

# ========================
# MOSTRAR FILAS INVÁLIDAS
# ========================
if len(invalid_rows) > 0:
    print("\nFilas que no se pudieron procesar:")
    print(invalid_rows)

# ========================
# CERRAR CONEXIÓN
# ========================
cursor.close()
conn.close()
print("\nProceso ETL completado exitosamente para el JSON de ventas.")