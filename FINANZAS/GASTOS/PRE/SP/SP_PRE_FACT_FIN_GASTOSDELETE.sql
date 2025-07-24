CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FACT_FIN_GASTOSDELETE("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que borra los datos de la tabla en RAW SQ1_EXT_0CO_OM_CCA_1
                        y SQ1_EXT_0CO_OM_CCA_9
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
        

        -- Conteo de filas borradas por tabla
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0CO_OM_CCA_1 AS CCA_1
        WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN AND CCA_1.TIPO='FULL';
        
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_0CO_OM_CCA_9 AS CCA_9
        WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN AND CCA_9.TIPO='FULL';


        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_0CO_OM_CCA_1
        WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN AND TIPO='FULL'; 

        DELETE FROM RAW.SQ1_EXT_0CO_OM_CCA_9
        WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN  AND TIPO='FULL';
		


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
    VALUES ('PRE.SP_PRE_FCT_GASTOSDELETE','RAW.SQ1_EXT_0CO_OM_CCA_1 y RAW.SQ1_EXT_0CO_OM_CCA_9', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );


    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas eliminadas: ', ROWS_INSERTED);

END;
$$;

CALL PRE.SP_PRE_FACT_FIN_GASTOSDELETE('2025001','2025005');