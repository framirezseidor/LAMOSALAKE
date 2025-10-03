CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_CONTROLPRESUPUESTAL_DELTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa PRE a CON para la tabla FCT_FIN_CONTROLPRESUPUESTAL
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
        DELETE FROM CON.FCT_FIN_CONTROLPRESUPUESTAL
        WHERE ANIOMES IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA); 
		
		
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
			INSERT INTO CON.FCT_FIN_CONTROLPRESUPUESTAL (
				ANIO,
                MES,
                ANIOMES,
                SOCIEDADCO_ID,
                SOCIEDAD_ID,
                ENTIDADCP_ID,
                CATEGORIAPRES_ID,
                TIPOVALOR_ID,
                FONDO_ID,
                CENTROGESTOR_ID,
                DETALLECOMPREAL_ID,
                INDICADOREST_ID,
                PROGRAMAPRES_ID,
                POSICIONPRES_ID,
                CENTROCOSTO_ID,
                CUENTA_ID,
                ACREEDOR_ID,
                DEUDOR_ID,
                GASTOER_ID,
                FECHA_CONTABILIZACION,
                TEXTO_TEXT,
                ENTIDAD_CENGES_ID,
                ENTIDAD_PROPRES_ID,
                IND_PRESUPUESTO_ASIGNABLE,
                IND_PRESUPUESTO_ASIGNADO,
                IND_PRESUPUESTO_GASTOREAL,
                IND_PRESUPUESTO_GASTOCOMP ,
                IND_PRESUPUESTO_DISPONIBLE,
                IND_PRESUPUESTO_ACTUAL,
                IND_PRESUPUESTO_COMPREAL,
                IND_PRESUPUESTO_VALORREAL,
                MON_ENTCP,
                MANDANTE,
                SISORIGEN_ID,
                FECHA_CARGA,
                ZONA_HORARIA )
					SELECT 
						ANIO,
                        MES,
                        ANIOMES,
                        CP.SOCIEDADCO_ID,
                        CP.SOCIEDAD_ID,
                        ENTIDADCP_ID,
                        CATEGORIAPRES_ID,
                        TIPOVALOR_ID,
                        FONDO_ID,
                        CENTROGESTOR_ID,
                        DETALLECOMPREAL_ID,
                        INDICADOREST_ID,
                        PROGRAMAPRES_ID,
                        POSICIONPRES_ID,
                        CP.CENTROCOSTO_ID,
                        CP.CUENTA_ID,
                        ACREEDOR_ID,
                        DEUDOR_ID,
                        GASTOER_ID,
                        FECHA_CONTABILIZACION,
                        TEXTO_TEXT,
                        ENTIDAD_CENGES_ID,
                        ENTIDAD_PROPRES_ID,
                        IND_PRESUPUESTO_ASIGNABLE,
                        IND_PRESUPUESTO_ASIGNADO,
                        IND_PRESUPUESTO_GASTOREAL,
                        IND_PRESUPUESTO_GASTOCOMP ,
                        IND_PRESUPUESTO_DISPONIBLE,
                        IND_PRESUPUESTO_ACTUAL,
                        IND_PRESUPUESTO_COMPREAL,
                        IND_PRESUPUESTO_VALORREAL,
                        MON_ENTCP,
                        CP.MANDANTE,
                        CP.SISORIGEN_ID,
                        CP.FECHA_CARGA,
                        CP.ZONA_HORARIA
					FROM PRE.PFCT_FIN_CONTROLPRESUPUESTAL AS CP
					LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
					  ON CP.CENTROCOSTO_ID = CECO.CENTROCOSTO_ID
					--LEFT JOIN PRE_PRD_SAP.DATOS_MAESTROS.PRE_DIM_CUENTA AS COSTE
					--  ON LTRIM(CP.CUENTA_ID,'0') = LTRIM(COSTE.CUENTA_ID,'0')
					WHERE (CECO.FECHA_DESDE IS NULL OR CECO.FECHA_HASTA IS NULL OR CURRENT_DATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
                    AND ENTIDADCP_ID IN ('FMGL')
                    --AND PROGRAMAPRES_ID = 'FMGL_GTOSGLES'   --------Confirmar con Usuarios----------
			;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_FIN_CONTROLPRESUPUESTAL;

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
    VALUES ('CON.SP_CON_FCT_CONTROLPRESUPUESTAL','CON.FCT_FIN_CONTROLPRESUPUESTAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

    CALL MIRRORING.SP_ACTUALIZAR_CALENDARIO('FCT_FIN_GASTOSCONTROLPRES', 'DIM_CAL_GASTOSCONTROLPRES', 'FECHA_CONTABILIZACION');


END;
$$;