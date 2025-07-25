CREATE OR REPLACE TABLE PRE.PDIM_FIN_CUENTAMAYOR(
    CUENTAMAYOR_ID,
    CUENTAMAYOR,
    CUENTAMAYOR_TEXT,
    PLANCUENTAS_ID,
    SISORIGEN_ID,
    MANDANTE,
    FECHA_CARGA,
    ZONA_HORARIA
) AS

SELECT  CONCAT(CUENTA.KTOPL,'_',CUENTA.SAKNR) AS CUENTAMAYOR_ID,
        CUENTA.SAKNR AS CUENTAMAYOR,
        TEXTO.TXTLG AS CUENTAMAYOR_TEXT,
        CUENTA.KTOPL AS PLANCUENTAS_ID,
        CUENTA.SISORIGEN_ID,
        CUENTA.MANDANTE,
        CUENTA.FECHA_CARGA,
        CUENTA.ZONA_HORARIA
FROM RAW.SQ1_EXT_0GL_ACCOUNT_ATTR AS CUENTA

LEFT JOIN RAW.SQ1_EXT_0GL_ACCOUNT_TEXT AS TEXTO

ON CUENTA.SAKNR = TEXTO.SAKNR