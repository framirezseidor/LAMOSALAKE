CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_CLI_EJECUTIVOCIS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-24
 Creador:            Fernando Cuellar M
 Descripción:        SP que transforma datos desde la capa RAW a PRE para DIM_CLI_EJECUTIVOCIS
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM PRE.PDIM_CLI_EJECUTIVOCIS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PDIM_CLI_EJECUTIVOCIS
        (
            EJECUTIVOCIS_ID,
            FUNCION_INTERLOCUTOR,
            EJECUTIVOCIS_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            LTRIM(LIFNR, '0') AS EJECUTIVOCIS_ID,
            PARVW,
            TXTLG,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM (
            SELECT
                K.LIFNR,
                K.PARVW,
                T.TXTLG,
                K.SISORIGEN_ID,
                K.MANDANTE,
                ROW_NUMBER() OVER (
                    PARTITION BY K.LIFNR
                    ORDER BY T.TXTLG, K.MANDANTE
                ) AS rn
            FROM RAW.SQ1_TBL_KNVP K
            LEFT JOIN RAW.SQ1_EXT_0BPARTNER_TEXT T
                ON K.LIFNR = T.PARTNER
            WHERE K.PARVW = 'ZC'
        )
        QUALIFY rn = 1;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_CLI_EJECUTIVOCIS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PDIM_CLI_EJECUTIVOCIS','PRE.PDIM_CLI_EJECUTIVOCIS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 

 