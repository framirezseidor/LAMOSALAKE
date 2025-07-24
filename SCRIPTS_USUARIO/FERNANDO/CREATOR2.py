import os

# Ruta donde están los archivos .sql
ruta = "C:\\Users\\FernandoCuellar\\Documents\\Lamosa Github\\lamosalake\\DIMENSIONES\\CLIENTE\\Tables"

# Iterar sobre todos los archivos .sql en la carpeta
for archivo in os.listdir(ruta):
    if archivo.endswith(".sql"):
        archivo_path = os.path.join(ruta, archivo)
        
        with open(archivo_path, "r", encoding="utf-8") as f:
            lineas = f.readlines()
        
        # Buscar el nombre de la dimensión desde la línea que lo dice
        for linea in lineas:
            if "pertenece a" in linea:
                dimension = linea.strip().split(" a ")[-1]
                break
        else:
            continue  # si no encontró la dimensión, salta ese archivo

        # Crear las nuevas líneas a agregar
        create_original = f'\nCREATE OR ALTER TABLE "PRE.P{dimension}" (\n\n);\n'
        create_prefijada = f'\nCREATE OR ALTER TABLE "CON.{dimension}" (\n\n);\n'

        # Reescribir archivo con todo
        with open(archivo_path, "w", encoding="utf-8") as f:
            f.write(f"Este archivo pertenece a {dimension}\n")
            f.write(create_original)
            f.write(create_prefijada)

print("✅ Archivos actualizados con bloques CREATE TABLE.")
