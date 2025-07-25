CREATE OR REPLACE TABLE CON.DIM_FIN_POSICIONPRES(
    POSICIONPRES_ID,
    POSICIONPRES,
    POSICIONPRES_TEXT,
    ENTIDADCP_ID,
    SISORIGEN_ID,
    MANDANTE,
    FECHA_CARGA,
    ZONA_HORARIA
) AS

SELECT  POSICIONPRES_ID,
        POSICIONPRES,
        POSICIONPRES_TEXT,
        ENTIDADCP_ID,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA
FROM PRE.PDIM_FIN_POSICIONPRES;