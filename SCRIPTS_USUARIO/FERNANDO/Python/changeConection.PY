import os

def reemplazar_en_workflows_alteryx(ruta_carpeta, reemplazos):
    """
    Aplica múltiples find & replace a archivos .yxmd y .yxmc en una carpeta.
    
    Parámetros:
    - ruta_carpeta: Ruta de la carpeta donde están los workflows/macros.
    - reemplazos: Diccionario con pares {texto_viejo: texto_nuevo}.
    """
    extensiones_validas = ['.yxmd', '.yxmc']

    for root, _, archivos in os.walk(ruta_carpeta): #for archivo in os.listdir(ruta_carpeta): (caminando por subcarpetas) --for root, _, archivos in os.walk(ruta_carpeta):
        for archivo in archivos:
            ruta_completa = os.path.join(root, archivo)
            _, extension = os.path.splitext(archivo)

            if extension.lower() not in extensiones_validas:
                continue

            try:
                with open(ruta_completa, 'r', encoding='utf-8') as f:
                    contenido = f.read()

                contenido_modificado = contenido
                for viejo, nuevo in reemplazos.items():
                    contenido_modificado = contenido_modificado.replace(viejo, nuevo)

                if contenido != contenido_modificado:
                    with open(ruta_completa, 'w', encoding='utf-8') as f:
                        f.write(contenido_modificado)
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

ruta = "C:\\Users\\FernandoCuellar\\SEIDOR ANALYTICS\\LAMOSA - General\\Proyecto BI S4H\\30 - Realización\\Alteryx\\Celula 1\\Backorder\\PRUEBAS\\NEW CONECTION"

reemplazar_en_workflows_alteryx(ruta, reemplazos)
