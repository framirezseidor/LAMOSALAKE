CREATE OR REPLACE TABLE PRE.PDIM_MAT_PEIDESTONALIZACION_REV(
PEIDESTONALIZACION_ID VARCHAR(70) PRIMARY KEY,
PEIDESTONALIZACION_TEXT VARCHAR(70),
SISORIGEN_ID VARCHAR(4),
MANDANTE VARCHAR(4),
FECHA_CARGA TIMESTAMP_TZ,
ZONA_HORARIA VARCHAR(10)
)