CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_MON_MONEDA()
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

        DELETE FROM CON.DIM_MON_MONEDA;

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

        INSERT INTO CON.DIM_MON_MONEDA
        (
            MONEDA_ID ,
            MONEDA_TEXTO ,
            MANDANTE ,
            SISORIGEN_ID ,
            FECHA_CARGA ,
            ZONA_HORARIA 
        )
       SELECT 
            MONEDA_ID ,
            MONEDA_TEXTO ,
            MANDANTE ,
            SISORIGEN_ID ,
            FECHA_CARGA ,
            ZONA_HORARIA 
        FROM PRE.PDIM_MON_MONEDA ;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_MON_MONEDA;

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
        VALUES('SP_CON_DIM_MON_MONEDA','DIM_MON_MONEDA', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );



    ---------------------------------------------------------------------------------
    -- STEP 5: CLONNING
    ---------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE MIRRORING.DIM_MON_LOC
    CLONE CON.DIM_MON_MONEDA;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_MON_LOC ON TABLE MIRRORING.DIM_MON_LOC;


    CREATE OR REPLACE TABLE MIRRORING.DIM_MON_MXN
    CLONE CON.DIM_MON_MONEDA;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_MON_MXN ON TABLE MIRRORING.DIM_MON_MXN;


    CREATE OR REPLACE TABLE MIRRORING.DIM_MON_USD
    CLONE CON.DIM_MON_MONEDA;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_MON_USD ON TABLE MIRRORING.DIM_MON_USD;


    CREATE OR REPLACE TABLE MIRRORING.DIM_MON_DOC
    CLONE CON.DIM_MON_MONEDA;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_MON_DOC ON TABLE MIRRORING.DIM_MON_DOC;


    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;