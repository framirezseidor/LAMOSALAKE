CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FCT_CARTERA_DELETE_FULL("ANIOMES" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que borra los datos de la tabla en RAW SQ1_EXT_ZBWFI_REP_ANTSALDET
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
        
        -- Conteo de filas borradas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET AS ANTSALDET
        WHERE CONCAT(SUBSTR(VALID_FROM,1,4),SUBSTR(VALID_FROM,6,2)) = :ANIOMES AND ANTSALDET.TIPO='FULL';

        // Process DELETE
        DELETE FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET AS ANTSALDET
        WHERE CONCAT(SUBSTR(VALID_FROM,1,4),SUBSTR(VALID_FROM,6,2)) = :ANIOMES AND ANTSALDET.TIPO='FULL';
		
		
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
    VALUES ('PRE.SP_PRE_FCT_CARTERA_DELETE_FULL','RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas eliminadas: ', ROWS_INSERTED);

END;
$$;





CALL PRE.SP_PRE_FCT_CARTERA_DELETE_FULL('202505');



SELECT * FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET;

SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE PROCESO = 'CON.SP_CON_FACT_CARTERAACT';
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE PROCESO = 'CON.SP_CON_FACT_CARTERAHIST';
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE PROCESO = 'PRE.SP_PRE_FACT_CARTERAACT';

SELECT * FROM RAW.PARAMETROS_EXTRACCION;

SELECT DISTINCT SOCIEDAD_ID, COUNT(*) FROM CON.FCT_FIN_CARTERAHIST GROUP BY ALL;

SELECT DISTINCT SOCIEDAD_ID, COUNT(*) FROM PRE.PFCT_FIN_CARTERAHIST GROUP BY ALL;