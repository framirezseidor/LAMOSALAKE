CREATE OR REPLACE PROCEDURE RAW.SP_RAW_SQ1_EXT_2LIS_02_SCL_DELETE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Esteban Méndez
 Descripción:        SP que borra datos de capa RAW para abastecimientos
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);
    FECHA_INICIO    VARCHAR(100);
    FECHA_FIN       VARCHAR(100);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA RAW
    ---------------------------------------------------------------------------------
    BEGIN

        SELECT R_INICIO 
        INTO :FECHA_INICIO 
        FROM RAW.PARAMETROS_EXTRACCION 
        WHERE ORDEN = '1' AND EXTRACTOR = '2LIS_02_SCL' AND NEGOCIO = '' AND PARAMETRO = 'PERIODO';

        SELECT R_FIN
        INTO :FECHA_FIN 
        FROM RAW.PARAMETROS_EXTRACCION 
        WHERE ORDEN = '1' AND EXTRACTOR = '2LIS_02_SCL' AND NEGOCIO = '' AND PARAMETRO = 'PERIODO';

        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE
        FROM RAW.SQ1_EXT_2LIS_02_SCL
        WHERE CONCAT(SUBSTR(BEDAT, 0, 4), SUBSTR(BEDAT, 6, 2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

        DELETE
        FROM SQ1_MOF_ZMM_IND_ADQUISICION;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: LOG
    ---------------------------------------------------------------------------------

    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_RAW_SQ1_EXT_2LIS_02_SCL_DELETE','RAW.SQ1_EXT_2LIS_02_SCL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 3: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 