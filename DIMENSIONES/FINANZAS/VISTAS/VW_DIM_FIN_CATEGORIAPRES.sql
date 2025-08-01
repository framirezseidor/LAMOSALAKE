CREATE OR REPLACE VIEW CON.VW_DIM_FIN_CATEGORIAPRES(
    CATEGORIAPRES_ID,
    CATEGORIAPRES_TEXT,
    SISORIGEN_ID,
    MANDANTE,
    FECHA_CARGA,
    ZONA_HORARIA
) AS

SELECT  CATEGORIAPRES_ID,
        CATEGORIAPRES_TEXT,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA
FROM CON.DIM_FIN_CATEGORIAPRES;