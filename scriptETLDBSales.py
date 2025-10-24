import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# ========================
# CONFIGURACIÓN
# ========================
#CONNECTION_STRING_DB = "mssql+pyodbc://localhost/DB_Sales2?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server" #Configuracion Carlos
CONNECTION_STRING_DB = "mssql+pyodbc://localhost/DB_SALES?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server" #Configuracion JJ
CONNECTION_STRING_DW = "mssql+pyodbc://localhost/DW_Sales?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"


# ========================
# FUNCIONES DE CONEXIÓN
# ========================
def get_engine(connection_string):
    return create_engine(connection_string)

def execute_sql(engine, sql):
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

def read_sql_to_dataframe(connection, sql):
    """Lee datos SQL a DataFrame"""
    return pd.read_sql(sql, connection)

# ========================
# ACTUALIZACIÓN DE FECHAS A 2024
# ========================
def update_dates_to_2024():
    """Actualiza las fechas de ventas de 2020 a 2024"""
    print("Actualizando fechas a 2024...")
    try:
        engine_db = get_engine(CONNECTION_STRING_DB)
        
        execute_sql(engine_db, """
            UPDATE OINV SET 
                DocDate = DATEADD(YEAR, 4, DocDate),
                DocDueDate = DATEADD(YEAR, 4, DocDueDate)
        """)
        
        execute_sql(engine_db, """
            UPDATE ORIN SET 
                DocDate = DATEADD(YEAR, 4, DocDate),
                DocDueDate = DATEADD(YEAR, 4, DocDueDate)
        """)
        
        print("✓ Fechas actualizadas exitosamente")
    except Exception as e:
        print(f"✗ Error actualizando fechas: {e}")

# ========================
# ETL DIM_CUSTOMER
# ========================
def etl_dim_customer():
    #Carga datos de clientes a DIM_Customer
    print("Cargando DIM_Customer...")
    try:
        engine_db = get_engine(CONNECTION_STRING_DB)
        engine_dw = get_engine(CONNECTION_STRING_DW)
        
        # Deshabilitar FK constraints
        execute_sql(engine_dw, "ALTER TABLE FACT_Sales NOCHECK CONSTRAINT ALL")
        
        # Leer y cargar datos
        df_customers = read_sql_to_dataframe(engine_db, """
            SELECT 
                c.CardCode as cardCode,
                c.CardName as cardName,
                ISNULL(c.CardType, 'C') as cardType,
                ISNULL(c.Country, 'NA') as country,
                ISNULL(z.Name, 'Sin Zona') as zone
            FROM OCRD c
            LEFT JOIN ZONAS z ON c.U_Zona = z.Code
            WHERE c.CardType = 'C'
        """)
        
        df_customers.to_sql('DIM_Customer', engine_dw, if_exists='append', index=False)
        
        # Rehabilitar FK constraints
        execute_sql(engine_dw, "ALTER TABLE FACT_Sales CHECK CONSTRAINT ALL")
        
        print(f"✓ DIM_Customer cargado: {len(df_customers)} registros")
        return df_customers
        
    except Exception as e:
        print(f"✗ Error en ETL DIM_Customer: {e}")
        return None

# ========================
# ETL DIM_PRODDUCT
# ========================
def etl_dim_product():
    print("Cargando DIM_Product...")
    try:
        engine_db = get_engine(CONNECTION_STRING_DB)
        engine_dw = get_engine(CONNECTION_STRING_DW)
        
        execute_sql(engine_dw, "DELETE FROM DIM_Product")
        
        df_products = read_sql_to_dataframe(engine_db, """
            SELECT 
                p.ItemCode as itemCode,
                p.ItemName as itemName,
                ISNULL(m.Name, 'Sin Marca') as brand,
                CAST(p.OnHand AS INT) as onHand,
                CAST(ISNULL(AVG(w.AvgPrice), 0) AS DECIMAL(10,2)) as avgPrice,
                ISNULL(p.CardCode, 'NA') as cardCode
            FROM OITM p
            LEFT JOIN MARCAS m ON p.U_Marca = m.Code
            LEFT JOIN OITW w ON p.ItemCode = w.ItemCode
            GROUP BY p.ItemCode, p.ItemName, m.Name, p.OnHand, p.CardCode
        """)
        
        df_products.to_sql('DIM_Product', engine_dw, if_exists='append', index=False)
        print(f"✓ DIM_Product cargado: {len(df_products)} registros")
        return df_products
        
    except Exception as e:
        print(f"✗ Error en ETL DIM_Product: {e}")
        return None
    
# ========================
# ETL DIM_SALESPERSON
# ========================
def etl_dim_salesperson():
    print("Cargando DIM_SalesPerson...")
    try:
        engine_db = get_engine(CONNECTION_STRING_DB)
        engine_dw = get_engine(CONNECTION_STRING_DW)
        
        execute_sql(engine_dw, "DELETE FROM DIM_SalesPerson")
        
        df_salespersons = read_sql_to_dataframe(engine_db, """
            SELECT 
                SlpCode as slpCode,
                SlpName as slpName,
                Active as active,
                U_Gestor as gestor
            FROM OSLP
            WHERE Active = 'Y'
        """)
        
        df_salespersons.to_sql('DIM_SalesPerson', engine_dw, if_exists='append', index=False)
        print(f"✓ DIM_SalesPerson cargado: {len(df_salespersons)} registros")
        return df_salespersons
        
    except Exception as e:
        print(f"✗ Error en ETL DIM_SalesPerson: {e}")
        return None

# ========================
# ETL FACT_SALES
# ========================
def etl_fact_sales():
    print("Cargando FACT_Sales...")
    try:
        engine_db = get_engine(CONNECTION_STRING_DB)
        engine_dw = get_engine(CONNECTION_STRING_DW)

        # Mapeos de IDs
        df_customers_map = read_sql_to_dataframe(engine_dw, "SELECT idCustomer, cardCode FROM DIM_Customer")
        df_products_map = read_sql_to_dataframe(engine_dw, "SELECT idProduct, itemCode FROM DIM_Product")
        df_salespersons_map = read_sql_to_dataframe(engine_dw, "SELECT idSalesPerson, slpCode FROM DIM_SalesPerson")
        df_time_map = read_sql_to_dataframe(engine_dw, "SELECT idTime, date, tipoCambio FROM DIM_Time")

        customers_dict = dict(zip(df_customers_map['cardCode'], df_customers_map['idCustomer']))
        products_dict = dict(zip(df_products_map['itemCode'], df_products_map['idProduct']))
        salespersons_dict = dict(zip(df_salespersons_map['slpCode'], df_salespersons_map['idSalesPerson']))
        time_dict = dict(zip(df_time_map['date'], df_time_map['idTime']))
        cambio_dict = dict(zip(df_time_map['date'], df_time_map['tipoCambio']))

        # Facturas (OINV) y Notas de crédito (ORIN)
        df_invoices = read_sql_to_dataframe(engine_db, """
            SELECT i.DocEntry, i.DocDate, i.CardCode, i.SlpCode, i.DocTotal, i.DocTotalFC,
                    d.ItemCode, d.Quantity, d.Price, d.LineTotal
            FROM OINV i INNER JOIN INV1 d ON i.DocEntry = d.DocEntry
        """)

        df_credit_notes = read_sql_to_dataframe(engine_db, """
            SELECT r.DocEntry, r.DocDate, r.CardCode, r.SlpCode, r.DocTotal, r.DocTotalFC,
                    d.ItemCode, d.Quantity, d.Price, d.LineTotal
            FROM ORIN r INNER JOIN RIN1 d ON r.DocEntry = d.DocEntry
        """)

        # Crear los registros de las facturas
        df_invoices['multiplier'] = 1  # Para las facturas, multiplicamos por 1
        df_credit_notes['multiplier'] = -1  # Para las notas de crédito, multiplicamos por -1

        # Combinar facturas y notas de crédito
        df_sales = pd.concat([df_invoices, df_credit_notes])

        # Aplicar el multiplicador a las cantidades, LineTotal, DocTotal, DocTotalFC
        df_sales['Quantity'] *= df_sales['multiplier']
        df_sales['LineTotal'] *= df_sales['multiplier']
        df_sales['DocTotal'] *= df_sales['multiplier']
        df_sales['DocTotalFC'] *= df_sales['multiplier']

        # Mapear IDs
        df_sales['idCustomer'] = df_sales['CardCode'].map(customers_dict)
        df_sales['idProduct'] = df_sales['ItemCode'].map(products_dict)
        df_sales['idSalesPerson'] = df_sales['SlpCode'].map(salespersons_dict)
        df_sales['idTime'] = df_sales['DocDate'].map(time_dict)

        # Eliminar filas sin mapeo válido
        df_sales = df_sales.dropna(subset=['idCustomer', 'idProduct', 'idSalesPerson', 'idTime'])

        # Mapear tipo de cambio
        df_sales['tipoCambio'] = df_sales['DocDate'].map(cambio_dict)
        
        # Calcular docTotalFC (sin cambios, ya que es la cantidad en dólares)
        df_sales['docTotalFC'] = df_sales['Quantity'] * df_sales['Price']
        
        # Calcular docTotal en colones (Multiplicando docTotalFC por tipoCambio)
        df_sales['docTotal'] = df_sales['Quantity'] * df_sales['Price'] * df_sales['tipoCambio']

        # Ordenar por DocEntry
        df_sales = df_sales.sort_values(by='DocEntry', ascending=True)

        # Formato final
        df_final = df_sales[['idTime', 'idCustomer', 'idProduct', 'idSalesPerson',
                             'Quantity', 'Price', 'docTotal', 'docTotalFC']].rename(columns={
            'Quantity': 'quantity',
            'Price': 'price',
            'docTotal': 'docTotal',
            'docTotalFC': 'doctTotalFC'
        })

        # Limpiar e insertar
        execute_sql(engine_dw, "DELETE FROM FACT_Sales")
        df_final.to_sql('FACT_Sales', engine_dw, if_exists='append', index=False)

        print(f"FACT_Sales cargado: {len(df_final)} registros")
        return df_final
    except Exception as e:
        print(f"Error en ETL FACT_Sales: {e}")
        return None

# ========================
# FUNCIÓN PRINCIPAL
# ========================
def main():
    """Función principal que ejecuta todo el ETL"""
    print("🚀 INICIANDO ETL COMPLETO DB_Sales → DW_Sales")
    print("=" * 50)
    
    start_time = datetime.now()
    
    try:
        # 1. Actualizar fechas a 2024
        #update_dates_to_2024()
        
        # 2. Cargar dimensiones
        etl_dim_customer()
        etl_dim_product()
        etl_dim_salesperson()
        
        # 3. Cargar hechos
        etl_fact_sales()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("=" * 50)
        print(f"✅ ETL COMPLETADO EXITOSAMENTE")
        print(f"⏰ Duración total: {duration}")
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN ETL: {e}")

# ========================
# EJECUCIÓN
# ========================
if __name__ == "__main__":
    main()