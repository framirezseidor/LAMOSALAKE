CREATE OR REPLACE TABLE CON.DIM_MAT_SEGMENTO_REV(
SEGMENTO_REV_ID VARCHAR(70) PRIMARY KEY,
SEGMENTO_REV_TEXT VARCHAR(70),
SISORIGEN_ID VARCHAR(4),
MANDANTE VARCHAR(4),
FECHA_CARGA TIMESTAMP_TZ,
ZONA_HORARIA VARCHAR(10)
)