CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_FIN_CUENTAMAYOR()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-15
 Creador:            Juan Pedreros
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_FIN_CUENTAMAYOR
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
        
        // Process TRUNCATE
        TRUNCATE TABLE CON.DIM_FIN_CUENTAMAYOR;
		
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
        
        // Process INSERT
        INSERT INTO CON.DIM_FIN_CUENTAMAYOR
        SELECT  
            CUENTAMAYOR_ID,
            CUENTAMAYOR,
            CUENTAMAYOR_TEXT,
            PLANCUENTAS_ID,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM PRE.PDIM_FIN_CUENTAMAYOR;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_FIN_CUENTAMAYOR;

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
    VALUES ('SP_CON_DIM_FIN_CUENTAMAYOR','CON.DIM_FIN_CUENTAMAYOR', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

        
    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
        DELETE FROM MIRRORING.DIM_FIN_CUENTAMAYOR;

        INSERT INTO MIRRORING.DIM_FIN_CUENTAMAYOR(
            CUENTAMAYOR_ID,
            CUENTAMAYOR,
            CUENTAMAYOR_TEXT,
            CUENTAMAYOR_ID_TEXT,
            PLANCUENTAS_ID,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )

        SELECT  CUENTAMAYOR_ID,
                CUENTAMAYOR,
                CUENTAMAYOR_TEXT,
                CONCAT(CUENTAMAYOR_ID,' - ',CUENTAMAYOR_TEXT) AS CUENTAMAYOR_ID_TEXT,
                PLANCUENTAS_ID,
                SISORIGEN_ID,
                MANDANTE,
                FECHA_CARGA,
                ZONA_HORARIA
        FROM CON.DIM_FIN_CUENTAMAYOR;

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;