import pandas as pd

# Leer el archivo CSV
df1 = pd.read_csv('SCRIPTS_USUARIO\\SANTIAGO\\EXTRACT_GASTOSPRES_2025002.csv', low_memory=False)

# Leer el archivo Excel (asegúrate de que el archivo contenga la hoja correcta, si tiene varias hojas)
df2 = pd.read_excel('SCRIPTS_USUARIO\\SANTIAGO\\RSA3_2025002_VERS.xlsx', sheet_name=0, engine='openpyxl')  # Usa sheet_name para especificar la hoja

# Seleccionar solo las primeras 4 columnas de ambos DataFrames
df1_selected = df1.iloc[:, :4]
df2_selected = df2.iloc[:, :4]

# Restablecer los índices (en caso de que sean diferentes)
df1_selected.reset_index(drop=True, inplace=True)
df2_selected.reset_index(drop=True, inplace=True)

# Verificar el número de columnas en df2_selected
print(f"Columnas en df2_selected: {df2_selected.shape[1]}")

# Verificar que df2_selected tenga 4 columnas antes de renombrar
if df2_selected.shape[1] == 4:
    # Asegurarse de que las columnas tengan los mismos nombres
    df1_selected.columns = ['Col1', 'Col2', 'Col3', 'Col4']  # Renombrar columnas de df1
    df2_selected.columns = ['Col1', 'Col2', 'Col3', 'Col4']  # Renombrar columnas de df2
else:
    print("El número de columnas en df2_selected no es 4, revisa los datos.")

# Comparar los DataFrames
res = df1_selected.compare(df2_selected)

# Imprimir el resultado de la comparación
print(res)
