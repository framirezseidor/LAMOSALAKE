CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MON_MONEDA()
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-08-19
 Creador:            Fidel Ramírez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_MON_MONEDA
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM PRE.PDIM_MON_MONEDA;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PDIM_MON_MONEDA
        (
            MONEDA_ID ,
            MONEDA_TEXTO ,
            MANDANTE ,
            SISORIGEN_ID ,
            FECHA_CARGA ,
            ZONA_HORARIA 
        )
       SELECT DISTINCT
            "TCURT~WAERS" AS MONEDA_ID,
            "TCURT~LTEXT" AS MONEDA_TEXTO,
            "TCURT~MANDT" AS MANDANTE,
            SISORIGEN_ID,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM RAW.SQ1_TBL_TCURT ;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MON_MONEDA;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
        SELECT COALESCE(:TEXTO,'EJECUCION CORRECTA') INTO :TEXTO;

        INSERT INTO LOGS.HISTORIAL_EJECUCIONES 
        VALUES('SP_PRE_PDIM_MON_MONEDA','PDIM_MON_MONEDA', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;