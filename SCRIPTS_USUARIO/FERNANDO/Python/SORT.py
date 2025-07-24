# Abrir archivo y leer las líneas
with open('C:\\Users\\FernandoCuellar\\Documents\\Lamosa GitHub\\lamosalake\\SCRIPTS_USUARIO\\FERNANDO\\Python\\tu_archivo.txt', 'r') as file:
    lines = file.readlines()

# Función para verificar si una línea termina en '_ID'
def termina_en_id(line):
    return line.strip().endswith('_ID')  # Asegúrate de eliminar espacios adicionales

# Separar las líneas en dos listas: una que termina en '_ID' y otra que no
lines_id = []
lines_no_id = []

for line in lines:
    if termina_en_id(line):
        lines_id.append(line)
    else:
        lines_no_id.append(line)

# Imprimir las líneas que terminan en '_ID' y las que no
print("Líneas que terminan en '_ID':")
for line in lines_id:
    print(line.strip())  # Solo para depuración

print("\nLíneas que NO terminan en '_ID':")
for line in lines_no_id:
    print(line.strip())  # Solo para depuración

# Combinar las listas: primero las que terminan en '_ID', luego las demás
lines_ordenadas = lines_id + lines_no_id

# Escribir el resultado ordenado en un nuevo archivo
with open('C:\\Users\\FernandoCuellar\\Documents\\Lamosa GitHub\\lamosalake\\SCRIPTS_USUARIO\\FERNANDO\\Python\\tu_archivo2.txt', 'w') as file:
    # Asegurarse de escribir las líneas con salto de línea
    for line in lines_ordenadas:
        file.write(line)

print("\nArchivo ordenado con éxito.")