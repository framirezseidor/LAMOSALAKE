CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_MAT_TECNOLOGIA_REV()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-23
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_MAT_TECNOLOGIA_REV
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

        DELETE FROM CON.DIM_MAT_TECNOLOGIA_REV;

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

        INSERT INTO CON.DIM_MAT_TECNOLOGIA_REV
        (
            TECNOLOGIA_ID,
            TECNOLOGIA_TEXT,
            SISORIGEN_ID,
            MANDANTE ,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde PRE
            TECNOLOGIA_ID,
            TECNOLOGIA_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA 
        FROM PRE.PDIM_MAT_TECNOLOGIA_REV;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_MAT_TECNOLOGIA_REV;

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
    VALUES ('SP_CON_DIM_MAT_TECNOLOGIA_REV','CON.DIM_MAT_TECNOLOGIA_REV', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 