import pandas as pd

# === CONFIGURACIÓN PERSONALIZABLE ===

# Ruta del archivo Excel (CAMBIAR esta línea por la ruta real)
ruta_excel = "C:\\Users\\FernandoCuellar\\Desktop\\ModeloComercialADHv3.xlsx"

orden_dimensiones = {
    "TIEMPO": 1,
    "ORGANIZACIÓN": 2,
    "GEOGRAFIA": 3,
    "DOCUMENTO BACKORDER": 4,
    "DOCUMENTO PEDIDOS": 4,
    "DOCUMENTO VENTAS Y PCP": 4,
    "OBRA": 5,
    "TRANSPORTE": 6,
    "CLIENTE": 7,
    "MATERIAL": 8,
    "INDICADORES": 9,
    "INDICADORES BACKORDER": 9,
    "INDICADORES PEDIDOS": 9,
    "INDICADORES VENTAS": 9,
    "INDICADORES VENTASPCP": 9,
    "TÉCNICA": 10
}

prefijos_personalizados = {
    "DOCUMENTO": {
        "Backorder": "BOR_",
        "Pedidos": "PED_",
        "Ventas": "VTA_",
        "VentasPCP": "VTA_"
    },
    "INDICADORES": {
        "Backorder": "BOR_",
        "Pedidos": "PED_",
        "Ventas": "VTA_",
        "VentasPCP": "PCP_"
    }
}

separar_grupo_indicadores_por_modelo = False  # True si quieres separar los indicadores por modelo

# === PROCESAMIENTO ===

xls = pd.read_excel(ruta_excel, sheet_name=None)
modelos_validos = {
    k: v for k, v in xls.items()
    if k.upper() not in ["MATRIZ CAMPOS MODELOS", "SELECTS"]
}

campo_info = {}

for modelo, df in modelos_validos.items():
    df = df.loc[:, ~df.columns.str.contains("Matriz Campos Modelos", case=False)]

    if "Campo Snowflake" not in df.columns or "Grupo Dimensión" not in df.columns:
        continue

    for _, row in df.iterrows():
        original_campo = str(row["Campo Snowflake"]).strip().upper()
        grupo = str(row["Grupo Dimensión"]).strip().upper()
        descripcion = str(row.get("Descripción", "")).strip()
        tipo_dato = str(row.get("Tipo Dato", "")).strip()

        grupo_final = grupo
        campo_final = original_campo

        if grupo in prefijos_personalizados:
            for modelo_key, prefijo in prefijos_personalizados[grupo].items():
                modelo_norm = modelo.upper().replace(" ", "").replace("_", "")
                modelo_key_norm = modelo_key.upper().replace(" ", "").replace("_", "")
                if modelo_key_norm in modelo_norm:
                    if grupo == "DOCUMENTO" and modelo_key_norm in ["VENTAS", "VENTASPCP"]:
                        grupo_final = "DOCUMENTO VENTAS Y PCP"
                    elif grupo == "INDICADORES":
                        grupo_final = (
                            f"{grupo} {modelo_key.upper()}"
                            if separar_grupo_indicadores_por_modelo
                            else "INDICADORES"
                        )
                    else:
                        grupo_final = f"{grupo} {modelo_key.upper()}"
                    campo_final = f"{prefijo}{original_campo}"
                    break

        if campo_final not in campo_info:
            campo_info[campo_final] = {
                "Grupo Dimensión": grupo_final,
                "Campo Snowflake sin prefijo": original_campo,
                "Campo PBI": campo_final,
                "Descripción": descripcion,
                "Tipo Dato": tipo_dato,
                modelo: campo_final
            }
        else:
            campo_info[campo_final]["Grupo Dimensión"] = campo_info[campo_final].get("Grupo Dimensión") or grupo_final
            campo_info[campo_final]["Campo Snowflake sin prefijo"] = campo_info[campo_final].get("Campo Snowflake sin prefijo") or original_campo
            campo_info[campo_final]["Campo PBI"] = campo_info[campo_final].get("Campo PBI") or campo_final
            campo_info[campo_final]["Descripción"] = campo_info[campo_final].get("Descripción") or descripcion
            campo_info[campo_final]["Tipo Dato"] = campo_info[campo_final].get("Tipo Dato") or tipo_dato
            campo_info[campo_final][modelo] = campo_final

# === MATRIZ DE CAMPOS ===

df_resultado = pd.DataFrame([
    {"Campo Snowflake": campo, **info}
    for campo, info in campo_info.items()
])

df_resultado["ORDER"] = df_resultado["Grupo Dimensión"].map(orden_dimensiones)

modelos = sorted(modelos_validos.keys())
columnas_finales = [
    "ORDER", "Grupo Dimensión", "Campo Snowflake sin prefijo",
    "Campo Snowflake", "Campo PBI", "Descripción", "Tipo Dato"
] + modelos

df_resultado = df_resultado.reindex(columns=columnas_finales).fillna("").sort_values(
    by=["ORDER", "Grupo Dimensión", "Campo Snowflake"]
)

# === SELECTS POR MODELO ===

selects = {}

for modelo in modelos:
    columnas = []
    for _, row in df_resultado.iterrows():
        campo_origen = row["Campo Snowflake sin prefijo"]
        campo_destino = row["Campo Snowflake"]
        if row[modelo] != "":
            columnas.append(f"{campo_origen} AS {campo_destino}")
        else:
            columnas.append(f"NULL AS {campo_destino}")
    selects[modelo] = f"SELECT\n" + ",\n".join(columnas) + f"\nFROM {modelo}"

df_selects = pd.DataFrame({
    "Modelo": list(selects.keys()),
    "SELECT SQL": list(selects.values())
})

# === GUARDADO ===

with pd.ExcelWriter(ruta_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_resultado.to_excel(writer, sheet_name="Matriz Campos Modelos", index=False)
    df_selects.to_excel(writer, sheet_name="Selects", index=False)

print("✅ Finalizado: se creó 'Matriz Campos Modelos' y 'Selects'.")