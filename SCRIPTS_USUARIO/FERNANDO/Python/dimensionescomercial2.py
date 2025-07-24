import pandas as pd

# Ruta al archivo
ruta_excel = "C:\\Users\\FernandoCuellar\\Desktop\\ModeloComercialREV.xlsx"

# Orden definido para Grupo de Dimensión
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

# Excluir hoja destino si ya existe
modelos_validos = {k: v for k, v in xls.items() if k.upper() != "MATRIZ CAMPOS MODELOS"}

# Recolector
campo_info = {}

# Recorrer cada hoja (modelo)
for modelo, df in modelos_validos.items():
    # Eliminar columna residual si existe
    df = df.loc[:, ~df.columns.str.contains("Matriz Campos Modelos", case=False)]

    # Validación básica
    if "Campo Snowflake" not in df.columns or "Grupo Dimensión" not in df.columns:
        continue

    for _, row in df.iterrows():
        campo = str(row["Campo Snowflake"]).strip().upper()
        grupo = str(row["Grupo Dimensión"]).strip().upper()
        descripcion = str(row.get("Descripción", "")).strip()
        tipo_dato = str(row.get("Tipo Dato", "")).strip()

        if campo not in campo_info:
            campo_info[campo] = {
                "Grupo Dimensión": grupo,
                "Descripción": descripcion,
                "Tipo Dato": tipo_dato,
                modelo: campo
            }
        else:
            campo_info[campo]["Grupo Dimensión"] = campo_info[campo].get("Grupo Dimensión") or grupo
            campo_info[campo]["Descripción"] = campo_info[campo].get("Descripción") or descripcion
            campo_info[campo]["Tipo Dato"] = campo_info[campo].get("Tipo Dato") or tipo_dato
            campo_info[campo][modelo] = campo

# Convertir a DataFrame
df_resultado = pd.DataFrame([
    {"Campo Snowflake": campo, **info}
    for campo, info in campo_info.items()
])

# Agregar ORDER
df_resultado["ORDER"] = df_resultado["Grupo Dimensión"].map(orden_dimensiones)

# Reordenar columnas
modelos = sorted(modelos_validos.keys())
columnas_finales = ["ORDER", "Grupo Dimensión", "Campo Snowflake", "Descripción", "Tipo Dato"] + modelos
df_resultado = df_resultado.reindex(columns=columnas_finales).fillna("").sort_values(by=["ORDER", "Campo Snowflake"])

# Guardar hoja
with pd.ExcelWriter(ruta_excel, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_resultado.to_excel(writer, sheet_name="Matriz Campos Modelos", index=False)
