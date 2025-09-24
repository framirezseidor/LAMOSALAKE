import os

def reemplazar_en_workflows_alteryx_solo_folder(ruta_carpeta, reemplazos):
    """
    Reemplaza strings en archivos .yxmd y .yxmc dentro de una sola carpeta (no recursivo).

    Parámetros:
    - ruta_carpeta: Ruta a la carpeta que contiene los workflows/macros.
    - reemplazos: Diccionario con {texto_viejo: texto_nuevo}.
    """
    extensiones_validas = ['.yxmd', '.yxmc']

    for nombre_archivo in os.listdir(ruta_carpeta):
        ruta_completa = os.path.join(ruta_carpeta, nombre_archivo)
        if not os.path.isfile(ruta_completa):
            continue  # Ignorar subcarpetas

        _, extension = os.path.splitext(nombre_archivo)
        if extension.lower() not in extensiones_validas:
            continue  # Solo yxmd o yxmc

        try:
            with open(ruta_completa, 'r', encoding='utf-8') as archivo:
                contenido = archivo.read()

            contenido_modificado = contenido
            for viejo, nuevo in reemplazos.items():
                contenido_modificado = contenido_modificado.replace(viejo, nuevo)

            if contenido != contenido_modificado:
                with open(ruta_completa, 'w', encoding='utf-8') as archivo:
                    archivo.write(contenido_modificado)
                print(f"✅ Modificado: {ruta_completa}")
            else:
                print(f"🔍 Sin cambios: {ruta_completa}")

        except Exception as e:
            print(f"❌ Error en {ruta_completa}: {e}")

reemplazos = {
    "809ff5f2-9593-492a-add8-1f9d378a7469": "75bc682d-475f-4bf7-80c0-22106ea478a1", #ODBC
    "e9810b1a-40a0-4448-a7e7-2c574f2eabc1": "38e5653b-5748-4f4b-9462-7140e0746369", #BULK
    "Snowflake BD — Snowflake": "Snowflake Lamosalake — Snowflake" #NOMBRE CONEXION
}

ruta = "C:\\Users\\Seidor\\SEIDOR ANALYTICS\\LAMOSA - General\\Proyecto BI S4H\\30 - Realización\\Alteryx\\Datos Maestros\\TRANSPORTE"

reemplazar_en_workflows_alteryx_solo_folder(ruta, reemplazos)
