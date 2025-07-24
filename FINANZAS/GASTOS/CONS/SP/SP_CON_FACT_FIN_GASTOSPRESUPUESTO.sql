CREATE OR REPLACE PROCEDURE CON.SP_CON_FACT_FIN_GASTOSPRESUPUESTO("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa PRE a CON para la tabla FCT_FIN_GASTOSPRESUPUESTO
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
        
        // Process DELETE
        DELETE FROM CON.FCT_FIN_GASTOSPRESUPUESTO
        WHERE (CONCAT(SUBSTRING("ANIOMES",1,4),'0',SUBSTRING("ANIOMES",5,2))) BETWEEN :FECHA_INICIO AND :FECHA_FIN; 
		
		
        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
        RETURN :TEXTO;
    END;
		

        ---------------------------------------------------------
        ------------------ LLENADO DE TABLAS --------------------
        ---------------------------------------------------------
	BEGIN
        // Inicia proceso INSERT 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Process INSERT
		INSERT INTO CON.FCT_FIN_GASTOSPRESUPUESTO
			SELECT
				TIPO,
                ANIO,
                MES,
                ANIOMES,
                FECHA_CONTABILIZACION,
                SOCIEDADCO_ID,
                SOCIEDAD_ID,
                CENTROCOSTO_ID,
                CENTROCOSTO,
                CUENTA_ID,
                CUENTA,
                CUENTAMAYOR_ID,
                GASTOER_ID,
                VERSION_ID,
                CENTRO_ID,
                ACREEDOR_ID,
                SOCIEDADGLASOC_ID,
                IND_GASTOPRESUPUESTO_SOC,
                IND_GASTOREAL_SOC,
                MON_SOC,
                IND_GASTOPRESUPUESTO_MXN,
                IND_GASTOREAL_MXN,
                MON_MXN,
                IND_GASTOPRESUPUESTO_USD,
                IND_GASTOREAL_USD,
                MON_USD,
                IND_GASTOPRESUPUESTO_V0_SOC,
                IND_GASTOPRESUPUESTO_V1_SOC,
                IND_GASTOPRESUPUESTO_V2_SOC,
                IND_GASTOPRESUPUESTO_V3_SOC,
                IND_GASTOPRESUPUESTO_V0_MXN,
                IND_GASTOPRESUPUESTO_V1_MXN,
                IND_GASTOPRESUPUESTO_V2_MXN,
                IND_GASTOPRESUPUESTO_V3_MXN,
                IND_GASTOPRESUPUESTO_V0_USD,
                IND_GASTOPRESUPUESTO_V1_USD,
                IND_GASTOPRESUPUESTO_V2_USD,
                IND_GASTOPRESUPUESTO_V3_USD,
                MANDANTE,
                SISORIGEN_ID,
                FECHA_CARGA,
                ZONA_HORARIA
			FROM PRE.PFCT_FIN_GASTOSPRESUPUESTO
			WHERE (CONCAT(SUBSTRING("ANIOMES",1,4),'0',SUBSTRING("ANIOMES",5,2))) BETWEEN :FECHA_INICIO AND :FECHA_FIN;
        
         -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_GASTOSPRESUPUESTO;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
        RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('CON.SP_CON_FACT_FIN_GASTOSPRESUPUESTO','CON.FCT_FIN_GASTOSPRESUPUESTO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;