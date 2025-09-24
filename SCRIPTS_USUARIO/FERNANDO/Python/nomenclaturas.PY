import os

def agregar_prefijo(ruta_carpeta):
    # Listar todos los archivos en la ruta
    archivos = os.listdir(ruta_carpeta)
    
    for archivo in archivos:
        # Verificar que empiece con "DIM_CLI" y que no tenga ya el prefijo
        if archivo.startswith("DIM_CLI") and not archivo.startswith("MC_S4HDEV_"):
            ruta_vieja = os.path.join(ruta_carpeta, archivo)
            nuevo_nombre = "MC_S4HDEV_" + archivo
            ruta_nueva = os.path.join(ruta_carpeta, nuevo_nombre)
            
            # Renombrar el archivo
            os.rename(ruta_vieja, ruta_nueva)
            print(f"Renombrado: {archivo} -> {nuevo_nombre}")

# Ejemplo de uso:
ruta = "C:\\Users\\FernandoCuellar\\SEIDOR ANALYTICS\\LAMOSA - General\\Proyecto BI S4H\\30 - Realización\\Alteryx\\Datos Maestros\\CLIENTE"

agregar_prefijo(ruta)
