import os

# Ruta donde están tus archivos
ruta = "C:\\Users\\FernandoCuellar\\SEIDOR ANALYTICS\\LAMOSA - General\\Proyecto BI S4H\\30 - Realización\\Alteryx\\Datos Maestros\\CLIENTE"

# Extensiones válidas
extensiones_validas = (".yxmd", ".yxmc")

# Recorremos los archivos de la ruta
for archivo in os.listdir(ruta):
    if archivo.endswith(extensiones_validas) and "DEV" in archivo:
        nuevo_nombre = archivo.replace("DEV", "")
        ruta_original = os.path.join(ruta, archivo)
        ruta_nueva = os.path.join(ruta, nuevo_nombre)
        
        # Renombrar archivo
        os.rename(ruta_original, ruta_nueva)
        print(f'Renombrado: {archivo} → {nuevo_nombre}')
