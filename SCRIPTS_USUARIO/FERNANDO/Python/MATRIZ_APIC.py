import pandas as pd
import unicodedata
from collections import defaultdict, Counter

# === Configura tu ruta ===
ruta_excel = "C:\\Users\\FernandoCuellar\\Desktop\\APIC.xlsx"

# === Utilidades ===
def norm_txt(x: str) -> str:
    """Normaliza texto: quita acentos, trim, colapsa espacios, mayúsculas."""
    if pd.isna(x):
        return ""
    x = str(x).strip()
    x = unicodedata.normalize("NFKD", x)
    x = "".join([c for c in x if not unicodedata.combining(c)])
    x = " ".join(x.split()).upper()
    return x

def pick_tipo_dato(tipos: list[str]) -> str:
    """Elige el tipo de dato más frecuente; si empate, concatena únicos alfabéticos."""
    tipos = [t for t in tipos if t]  # quita vacíos
    if not tipos:
        return ""
    cnt = Counter(tipos)
    maxc = max(cnt.values())
    candidatos = sorted([t for t, c in cnt.items() if c == maxc])
    if len(candidatos) == 1:
        return candidatos[0]
    return " | ".join(candidatos)

# Orden específico de grupos
orden_dimensiones = {
    "TIEMPO": 1,
    "ORGANIZACION": 2,
    "ABASTECIMIENTOS": 3,
    "MATERIAL": 4,
    "DOCUMENTO": 5,
    "DOCUMENTO BACKORDER": 6,
    "DOCUMENTO VENTAS Y PCP": 7,
    "INDICADORES": 8,
    "TECNICA": 9,
}

# Hojas objetivo (A, P, I, C)
TARGET_SHEETS = {"A", "P", "I", "C"}

# === Leer todas las hojas ===
xls = pd.read_excel(ruta_excel, sheet_name=None)

# Filtra solo A,P,I,C (case-insensitive)
modelos_validos = {}
for k, v in xls.items():
    key_up = k.strip().upper()
    if key_up in TARGET_SHEETS:
        modelos_validos[key_up] = v

if not modelos_validos:
    raise ValueError("No se encontraron hojas A, P, I o C en el archivo.")

# === Recolección ===
campo_grupos = defaultdict(Counter)
campo_por_modelo = defaultdict(dict)
campo_tipos = defaultdict(list)

for modelo, df in modelos_validos.items():
    df = df.loc[:, ~df.columns.str.contains("Matriz Campos Modelos", case=False, na=False)]

    cols = {c.upper().strip(): c for c in df.columns}
    col_campo = cols.get("CAMPO SNOWFLAKE") or cols.get("CAMPO_SNOWFLAKE") or cols.get("CAMPO")
    col_grupo = cols.get("GRUPO DIMENSIÓN") or cols.get("GRUPO DIMENSION") or cols.get("GRUPO_DIMENSION")
    col_activo = cols.get("ACTIVO")
    col_tipo   = cols.get("TIPO DATO") or cols.get("TIPO_DE_DATO") or cols.get("TIPO") or cols.get("DATA TYPE") or cols.get("TIPO DATOS")

    if not col_campo or not col_grupo:
        continue

    if col_activo in df.columns:
        df = df[df[col_activo].fillna(0).astype(str).str.strip().isin(["1", "TRUE", "SI", "SÍ"])]

    df["_CAMPO_"] = df[col_campo].apply(norm_txt)
    df["_GRUPO_"] = df[col_grupo].apply(norm_txt)
    tipos_norm = None
    if col_tipo in df.columns:
        tipos_norm = df[col_tipo].astype(str).str.strip().str.upper()

    df = df[df["_CAMPO_"] != ""]

    tmp_cols = ["_CAMPO_", "_GRUPO_"]
    if tipos_norm is not None:
        df = df.assign(_TIPO_=tipos_norm)
        tmp_cols.append("_TIPO_")
    df = df[tmp_cols].drop_duplicates()

    for _, row in df.iterrows():
        campo = row["_CAMPO_"]
        grupo = row["_GRUPO_"]
        if grupo:
            campo_grupos[campo][grupo] += 1
        campo_por_modelo[campo][modelo] = campo
        if "_TIPO_" in row and isinstance(row["_TIPO_"], str):
            campo_tipos[campo].append(row["_TIPO_"])

# Resolver grupo y tipo
campo_grupo_final = {}
campo_tipo_final = {}
for campo in campo_por_modelo.keys():
    counter = campo_grupos.get(campo, Counter())
    if counter:
        maxc = max(counter.values())
        candidatos = sorted([g for g, c in counter.items() if c == maxc])
        campo_grupo_final[campo] = candidatos[0]
    else:
        campo_grupo_final[campo] = ""
    campo_tipo_final[campo] = pick_tipo_dato(campo_tipos.get(campo, []))

# Construir DataFrame
modelos = ["A", "P", "I", "C"]
rows = []
for campo in sorted(campo_por_modelo.keys()):
    row = {
        "Grupo Dimensión": campo_grupo_final.get(campo, ""),
        "Campo Snowflake": campo,
        "Campo Power BI": "",
        "Tipo de Dato": campo_tipo_final.get(campo, ""),
    }
    for m in modelos:
        row[m] = campo_por_modelo[campo].get(m, "")
    rows.append(row)

df_resultado = pd.DataFrame(rows)

# Agregar columna ORDER
df_resultado["ORDER"] = df_resultado["Grupo Dimensión"].map(orden_dimensiones)

# Ordenar según ORDER y luego Campo
df_resultado = df_resultado.sort_values(
    by=["ORDER", "Campo Snowflake"], kind="mergesort"
).reset_index(drop=True)

# Columnas finales
df_resultado = df_resultado[["ORDER", "Grupo Dimensión", "Campo Snowflake", "Campo Power BI", "Tipo de Dato"] + modelos]

# Guardar
with pd.ExcelWriter(ruta_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_resultado.to_excel(writer, sheet_name="Matriz Campos Modelos", index=False)