CREATE OR REPLACE TABLE CON.DIM_FIN_GASTOER(
    GASTOER_ID,
    GASTOER_TEXT
) AS

SELECT  GASTOER_ID,
        GASTOER_TEXT
FROM PRE.PDIM_FIN_GASTOER;