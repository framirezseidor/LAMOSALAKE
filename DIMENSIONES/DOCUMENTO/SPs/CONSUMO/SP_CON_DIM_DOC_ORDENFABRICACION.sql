CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_DOC_ORDENFABRICACION()
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

        DELETE FROM CON.DIM_DOC_ORDENFABRICACION;

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

        INSERT INTO CON.DIM_DOC_ORDENFABRICACION
        (
            ORDENFABRICACION_ID,
            MATERIAL_ID,
            SOCIEDAD_ID,
            CENTRO_ID,
            SECTOR_ID,
            CLASEORDFAB_ID,
            TIPOORDFAB_ID,
            CLIENTE_ID,
            LOTE,
            ORDENFABRICACION_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            ORDENFABRICACION_ID,
            MATERIAL_ID,
            SOCIEDAD_ID,
            CENTRO_ID,
            SECTOR_ID,
            CLASEORDFAB_ID,
            TIPOORDFAB_ID,
            CLIENTE_ID,
            LOTE,
            ORDENFABRICACION_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM PRE.PDIM_DOC_ORDENFABRICACION;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_DOC_ORDENFABRICACION;

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
    VALUES ('SP_CON_DIM_DOC_ORDENFABRICACION','DIM_DOC_ORDENFABRICACION', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
        CREATE OR REPLACE TABLE MIRRORING.DIM_DOC_ORDENFABRICACION
        CLONE CON.DIM_DOC_ORDENFABRICACION;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_DOC_ORDENFABRICACION ON TABLE MIRRORING.DIM_DOC_ORDENFABRICACION;
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
