import pandas as pd

# Ruta al archivo Excel
ruta_excel = "C:\\Users\\FernandoCuellar\\Desktop\\modeloscomercialesadh.xlsx"

# Orden de prioridad para grupos de dimensión
orden_dimensiones = {
    "TIEMPO": 1,
    "ORGANIZACIÓN": 2,
    "GEOGRAFIA": 3,
    "DOCUMENTO": 4,
    "OBRA": 5,
    "TRANSPORTE": 6,
    "CLIENTE": 7,
    "MATERIAL": 8,
    "INDICADORES": 9,
    "TÉCNICA": 10
}

# Leer todas las hojas
xls = pd.read_excel(ruta_excel, sheet_name=None)

# Excluir hoja de salida si ya existe
modelos_validos = {k: v for k, v in xls.items() if k.upper() != "MATRIZ CAMPOS MODELOS"}

# Recolector de campos
campo_info = {}

# Procesar cada hoja válida
for modelo, df in modelos_validos.items():
    # Eliminar columna residual si existe
    df = df.loc[:, ~df.columns.str.contains("Matriz Campos Modelos", case=False)]

    # Verificar columnas necesarias
    if "Campo Snowflake" not in df.columns or "Grupo Dimensión" not in df.columns:
        continue

    for _, row in df.iterrows():
        campo = str(row["Campo Snowflake"]).strip().upper()
        grupo = str(row["Grupo Dimensión"]).strip().upper()

        if campo not in campo_info:
            campo_info[campo] = {
                "Grupo Dimensión": grupo,
                modelo: campo
            }
        else:
            campo_info[campo]["Grupo Dimensión"] = campo_info[campo].get("Grupo Dimensión") or grupo
            campo_info[campo][modelo] = campo

# Crear DataFrame
df_resultado = pd.DataFrame([
    {"Campo Snowflake": campo, **info}
    for campo, info in campo_info.items()
])

# Agregar columna de orden
df_resultado["ORDER"] = df_resultado["Grupo Dimensión"].map(orden_dimensiones)

# Organizar columnas
modelos = sorted(modelos_validos.keys())
columnas_finales = ["ORDER", "Grupo Dimensión", "Campo Snowflake"] + modelos
df_resultado = df_resultado.reindex(columns=columnas_finales).fillna("").sort_values(by=["ORDER", "Campo Snowflake"])

# Guardar la hoja corregida
with pd.ExcelWriter(ruta_excel, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_resultado.to_excel(writer, sheet_name="Matriz Campos Modelos", index=False)