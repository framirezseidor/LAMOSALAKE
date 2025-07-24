CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_TRA_TRANSPORTISTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-21
 Creador:            Agustin Gutierrez
 Descripción:        SP que transforma datos desde la capa PRE a CON para Datos Maestros
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

        DELETE FROM CON.DIM_TRA_TRANSPORTISTA;

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

        INSERT INTO CON.DIM_TRA_TRANSPORTISTA
        (
            TRANSPORTISTA_ID,
            /*ELIMINADOS POR SOLICITUD DE DANIEL GORDILLO 23/06/2025
            CLIENTE_ID,
            REGION_ID,
            REGION_TEXT,
            CENTRO_ID,*/
            TRANSPORTISTA_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            LTRIM(TRANSPORTISTA_ID,'0') TRANSPORTISTA_ID,
            /*CLIENTE_ID,
            REGION_ID,
            REGION_TEXT,
            CENTRO_ID,*/
            TRANSPORTISTA_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM PRE.PDIM_TRA_TRANSPORTISTA;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_TRA_TRANSPORTISTA;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_DIM_TRA_TRANSPORTISTA','DIM_TRA_TRANSPORTISTA', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
        DELETE FROM MIRRORING.DIM_TRA_TRANSPORTISTA;

        INSERT INTO MIRRORING.DIM_TRA_TRANSPORTISTA
        (
            TRANSPORTISTA_ID,
            TRANSPORTISTA_TEXT,
            TRANSPORTISTA_ID_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            TRANSPORTISTA_ID,
            TRANSPORTISTA_TEXT,
            CONCAT(TRANSPORTISTA_ID, ' - ',TRANSPORTISTA_TEXT ) TRANSPORTISTA_ID_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM CON.DIM_TRA_TRANSPORTISTA;
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;

