CREATE OR alter VIEW  CON.VW_DIM_ORG_UENADHESIVOS AS

SELECT
UENADHESIVOS_ID,
UENADHESIVOS_TEXT,
SISORIGEN_ID,
MANDANTE,
FECHA_CARGA,
ZONA_HORARIA

FROM CON.DIM_ORG_UENADHESIVOS