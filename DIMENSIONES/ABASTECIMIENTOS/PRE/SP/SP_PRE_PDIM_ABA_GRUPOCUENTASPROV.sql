CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_ABA_GRUPOCUENTASPROV()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-24
 Creador:            Juan Méndez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para DIM_ABA_GRUPOCUENTASPROV
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

        DELETE FROM PRE.PDIM_ABA_GRUPOCUENTASPROV;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PDIM_ABA_GRUPOCUENTASPROV
        (
            GRUPOCUENTASPROV_ID,
            GRUPOCUENTASPROV_TEXT,

            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            KTOKK AS GRUPOCUENTASPROV_ID,
            TXT30 AS GRUPOCUENTASPROV_TEXT,

            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA

        FROM RAW.SQ1_EXT_0VEN_ACCNT_GRP_TEXT
        WHERE SPRAS = 'S'
        ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_ABA_GRUPOCUENTASPROV;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PDIM_ABA_GRUPOCUENTASPROV','PRE.PDIM_ABA_GRUPOCUENTASPROV', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 