CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FCT_CONTROL_PRESUPUESTAL_DELETE("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que Elimina datos de las tablas de capa RAW para Control Presupuestal
---------------------------------------------------------------------------------
*/

DECLARE

    ------------ VARIABLES DE ENTORNO ------------------
	    F_INICIO        TIMESTAMP_NTZ(9);
        F_FIN           TIMESTAMP_NTZ(9);
        T_EJECUCION     NUMBER(38,0);
        ROWS_INSERTED   NUMBER(38,0);
        TEXTO           VARCHAR(200);

BEGIN
    //Generacion del identificador del proceso
    SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
    
    BEGIN


        ---------------------------------------------------------
        ------------------- DELETE TABLAS -----------------------
        ---------------------------------------------------------
        // Inicia proceso DELETE 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Conteo de filas Elimandas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_31;
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_32;
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_33;
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_41;
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_42;
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0PU_IS_PS_43;

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_31
        WHERE CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN; 

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_32
        WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_33
        WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_41
        WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN; 

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_42
        WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN; 

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_43
        WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN; 
		
		
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
    VALUES ('PRE.SP_PRE_FCT_CONTROL_PRESUPUESTAL_DELETE','RAW.SQ1_EXT_0PU_IS_PS_3143', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas Elimandas: ', ROWS_INSERTED);

END;
$$;


CALL PRE.SP_PRE_FCT_CONTROL_PRESUPUESTAL_DELETE('197001','202612');