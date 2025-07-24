import os

# Lista de nombres
nombres = [
    "SP_PRE_PDIM_CLI_CLIENTE",
    "SP_PRE_PDIM_CLI_GRUPOSOLICITANTE",
    "SP_PRE_PDIM_CLI_CLASIFICACIONCLIENTE",
    "SP_PRE_PDIM_CLI_GRUPOIMPUTCLIENTE",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES1",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES2",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES3",
    "SP_PRE_PDIM_CLI_RAMO",
    "SP_PRE_PDIM_CLI_COORDINADORCOMERCIAL",
    "SP_PRE_PDIM_CLI_ASESORCOMERCIAL",
    "SP_PRE_PDIM_CLI_EJECUTIVOCIS",
    "SP_PRE_PDIM_CLI_TIENDARECIBO",
    "SP_PRE_PDIM_CLI_DESTINATARIO",
    "SP_PRE_PDIM_CLI_SOLICITANTE",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES4",
    "SP_PRE_PDIM_CLI_GRUPOCLIENTES5"
]
# Ruta donde se crearán los archivos (ajústala a tu caso)
ruta = "C:\\Users\\FernandoCuellar\\Documents\\Lamosa Github\\lamosalake\\DIMENSIONES\\CLIENTE\\SPs"


# Asegúrate de que la carpeta exista
os.makedirs(ruta, exist_ok=True)

# Crear archivos en esa ruta
for nombre in nombres:
    with open(os.path.join(ruta, f"{nombre}.sql"), "w") as f:
        f.write(f"Este archivo pertenece a {nombre}")
