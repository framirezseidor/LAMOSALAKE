CREATE OR REPLACE PROCEDURE CON.SP_CON_FACT_CARTERAACT("ANIOMES" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa PRE a CON para la tabla FCT_FIN_CARTERAACT
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
        DELETE FROM CON.FCT_FIN_CARTERAACT;
		
		
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
		INSERT INTO CON.FCT_FIN_CARTERAACT
		SELECT  ANIO,
                MES,
                ANIO_MES,
                SOCIEDAD_ID,
                ORGVENTAS_ID,
                IFNULL(CANALDISTRIB_ID,''),
                SECTOR_ID,
                IFNULL(OFICINAVENTAS_ID,''),
                DEUDOR_ID,
                PAIS_ID,
                SOLICITANTE,
                SOLICITANTE_ID,
                DESTINATARIO,
                DESTINATARIO_ID,
                CLASEDOCFI_ID,
                DOCUMENTOFI,
                CENTROBENEFICIO_ID,
                CUENTALIBROMAYOR_ID,
                DOCREFERENCIA,
                FACTURA,
                CLASEFACTURA_ID,
                ASESORFACTURA_ID,
                ASESORFACTURA_TEXT,
                AREACTRLCDTO_ID,
                CLASERIESGO,
                CLASERIESGO_ID,
                EQUIPORESPCREDITO_ID,
                EQUIPORESPCREDITO_TEXT,
                GRUPOCREDCLIENTE_ID,
                GRUPCREDCLIENTE_TEXT,
                FECHACONTAB,
                FECHA_VENCIMIENTO,
                FECHA_CLAVE,
                IND_IMP_CARTERA_TOTAL_MDOC,
                IND_IMP_VENCIDO_030_MDOC,
                IND_IMP_VENCIDO_3160_MDOC,
                IND_IMP_VENCIDO_6190_MDOC,
                IND_IMP_VENCIDO_MAYOR90_MDOC,
                IND_IMP_VENCIDO_TOTAL_MDOC,
                IND_IMP_PORVENCER_030_MDOC,
                IND_IMP_PORVENCER_3160_MDOC,
                IND_IMP_PORVENCER_6190_MDOC,
                IND_IMP_PORVENCER_MAYOR90_MDOC,
                IND_IMP_PORVENCER_TOTAL_MDOC,
                IND_IMP_COBRANZA_MDOC,
                IND_IMP_FACTURAS_MDOC,
                MON_DOC,
                IND_IMP_CARTERA_TOTAL_MSOC,
                IND_IMP_VENCIDO_030_MSOC,
                IND_IMP_VENCIDO_3160_MSOC,
                IND_IMP_VENCIDO_6190_MSOC,
                IND_IMP_VENCIDO_MAYOR90_MSOC,
                IND_IMP_VENCIDO_TOTAL_MSOC,
                IND_IMP_PORVENCER_030_MSOC,
                IND_IMP_PORVENCER_3160_MSOC,
                IND_IMP_PORVENCER_6190_MSOC,
                IND_IMP_PORVENCER_MAYOR90_MSOC,
                IND_IMP_PORVENCER_TOTAL_MSOC,
                IND_IMP_COBRANZA_MSOC,
                IND_IMP_FACTURAS_MSOC,
                MON_SOC,
                IND_IMP_CARTERA_TOTAL_MUSD,
                IND_IMP_VENCIDO_030_MUSD,
                IND_IMP_VENCIDO_3160_MUSD,
                IND_IMP_VENCIDO_6190_MUSD,
                IND_IMP_VENCIDO_MAYOR90_MUSD,
                IND_IMP_VENCIDO_TOTAL_MUSD,
                IND_IMP_PORVENCER_030_MUSD,
                IND_IMP_PORVENCER_3160_MUSD,
                IND_IMP_PORVENCER_6190_MUSD,
                IND_IMP_PORVENCER_MAYOR90_MUSD,
                IND_IMP_PORVENCER_TOTAL_MUSD,
                IND_IMP_COBRANZA_MUSD,
                IND_IMP_FACTURAS_MUSD,
                MON_USD,
                TIPO
        FROM (

            SELECT  ANIO,
                    MES,
                    ANIO_MES,
                    SOCIEDAD_ID,
                    ORGVENTAS_ID,
                    CANALDISTRIB_ID,
                    SECTOR_ID,
                    OFICINAVENTAS_ID,
                    DEUDOR_ID,
                    PAIS_ID,
                    SOLICITANTE,
                    SOLICITANTE_ID,
                    DESTINATARIO,
                    DESTINATARIO_ID,
                    CLASEDOCFI_ID,
                    DOCUMENTOFI,
                    CENTROBENEFICIO_ID,
                    CUENTALIBROMAYOR_ID,
                    DOCREFERENCIA,
                    FACTURA,
                    CLASEFACTURA_ID,
                    ASESORFACTURA_ID,
                    ASESORFACTURA_TEXT,
                    AREACTRLCDTO_ID,
                    CLASERIESGO,
                    CLASERIESGO_ID,
                    EQUIPORESPCREDITO_ID,
                    EQUIPORESPCREDITO_TEXT,
                    GRUPOCREDCLIENTE_ID,
                    GRUPCREDCLIENTE_TEXT,
                    FECHA_CONTAB AS FECHACONTAB,
                    FECHA_VENCIMIENTO,
                    FECHA_CLAVE,
                    IND_IMP_CARTERA_TOTAL_MDOC,
                    IND_IMP_VENCIDO_030_MDOC,
                    IND_IMP_VENCIDO_3160_MDOC,
                    IND_IMP_VENCIDO_6190_MDOC,
                    IND_IMP_VENCIDO_MAYOR90_MDOC,
                    IND_IMP_VENCIDO_TOTAL_MDOC,
                    IND_IMP_PORVENCER_030_MDOC,
                    IND_IMP_PORVENCER_3160_MDOC,
                    IND_IMP_PORVENCER_6190_MDOC,
                    IND_IMP_PORVENCER_MAYOR90_MDOC,
                    IND_IMP_PORVENCER_TOTAL_MDOC,
                    IND_IMP_COBRANZA_MDOC,
                    IND_IMP_FACTURAS_MDOC,
                    MON_DOC,
                    IND_IMP_CARTERA_TOTAL_MSOC,
                    IND_IMP_VENCIDO_030_MSOC,
                    IND_IMP_VENCIDO_3160_MSOC,
                    IND_IMP_VENCIDO_6190_MSOC,
                    IND_IMP_VENCIDO_MAYOR90_MSOC,
                    IND_IMP_VENCIDO_TOTAL_MSOC,
                    IND_IMP_PORVENCER_030_MSOC,
                    IND_IMP_PORVENCER_3160_MSOC,
                    IND_IMP_PORVENCER_6190_MSOC,
                    IND_IMP_PORVENCER_MAYOR90_MSOC,
                    IND_IMP_PORVENCER_TOTAL_MSOC,
                    IND_IMP_COBRANZA_MSOC,
                    IND_IMP_FACTURAS_MSOC,
                    MON_SOC,
                    IND_IMP_CARTERA_TOTAL_MUSD,
                    IND_IMP_VENCIDO_030_MUSD,
                    IND_IMP_VENCIDO_3160_MUSD,
                    IND_IMP_VENCIDO_6190_MUSD,
                    IND_IMP_VENCIDO_MAYOR90_MUSD,
                    IND_IMP_VENCIDO_TOTAL_MUSD,
                    IND_IMP_PORVENCER_030_MUSD,
                    IND_IMP_PORVENCER_3160_MUSD,
                    IND_IMP_PORVENCER_6190_MUSD,
                    IND_IMP_PORVENCER_MAYOR90_MUSD,
                    IND_IMP_PORVENCER_TOTAL_MUSD,
                    IND_IMP_COBRANZA_MUSD,
                    IND_IMP_FACTURAS_MUSD,
                    MON_USD,
                    TIPO
            FROM PRE.PFCT_FIN_CARTERAACT
            WHERE ANIO_MES = :ANIOMES
        );

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_FIN_CARTERAACT;

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
    VALUES ('CON.SP_CON_FACT_CARTERAACT','CON.FCT_FIN_CARTERAACT', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;