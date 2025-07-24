CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FCT_CARTERAHIST( "ANIOMES" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-20
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_CARTERAHIST
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
        DELETE FROM PRE.PFCT_FIN_CARTERAHIST
        WHERE ANIO_MES = :ANIOMES;
		
		
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
        
        // Proceso INSERT
	INSERT INTO PRE.PFCT_FIN_CARTERAHIST
	SELECT  SUBSTR(FACT.VALID_FROM,1,4) AS ANIO,
                SUBSTR(FACT.VALID_FROM,6,2) AS MES,
                CONCAT(SUBSTR(FACT.VALID_FROM,1,4),SUBSTR(FACT.VALID_FROM,6,2)) AS ANIO_MES,
                FACT.BUKRS AS SOCIEDAD_ID,
                FACT.VKORG AS ORGVENTAS_ID,
                FACT.VTWEG AS CANALDISTRIB_ID,
                SECTOR.SPART AS SECTOR_ID,
                FACT.VKBUR AS OFICINAVENTAS_ID,
                LTRIM(FACT.KUNNR, '0') AS DEUDOR_ID,
                PAIS.LAND1 AS PAIS_ID,
                LTRIM(FACT.SOLIC, '0') AS SOLICITANTE,
                CONCAT(FACT.VKORG, '_', FACT.VTWEG, '_', SECTOR.SPART, '_', SOLICITANTE) AS SOLICITANTE_ID,
                LTRIM(FACT.DESTI, '0') AS DESTINATARIO,
                CONCAT(FACT.VKORG, '_', FACT.VTWEG, '_', SECTOR.SPART, '_', DESTINATARIO) AS DESTINATARIO_ID,
                FACT.BLART AS CLASEDOCFI_ID,
                FACT.BELNR AS DOCUMENTOFI,
                FACT.CEBE AS CENTROBENEFICIO_ID,
                FACT.HKONT AS CUENTALIBROMAYOR_ID,
                FACT.XBLNR AS DOCREFERENCIA,
                FACT.VBELN AS FACTURA,
                FACT.FKART AS CLASEFACTURA_ID,
                LTRIM(FACT.ZASESOR, '0') AS ASESORFACTURA_ID,
                FACT.ZNASESOR AS ASESORFACTURA_NAME,
                FACT.KKBER AS AREACTRLCDTO_ID,
                FACT.CTLPC AS CLASERIESGO_ID,
                FACT.SBGRP AS EQUIPORESPCREDITO_ID,
                FACT.DESCEQUI AS EQUIPORESPCREDITO_NAME,
                FACT.GRUPP AS GRUPOCREDCLIENTE_ID,
                FACT.DESCGRUP AS GRUPCREDCLIENTE_NAME,
                FACT.BUDAT AS FECHA_CONTAB,
                FACT.ZFVEN AS FECHA_VENCIMIENTO,
                FACT.VALID_FROM AS FECHACLAVE,

                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.WRBTR) * 100 ELSE (FACT.WRBTR)
                        END AS IND_IMP_CARTERA_TOTAL_MDOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.Z0A30V) * 100 ELSE (FACT.Z0A30V)
                        END AS IND_IMP_VENCIDO_030_MDOC, ----------------VENCIDO EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.Z31A60V) * 100 ELSE (FACT.Z31A60V)
                        END AS IND_IMP_VENCIDO_3160_MDOC, --------------VENCIDO EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.Z61A90V) * 100 ELSE (FACT.Z61A90V)
                        END AS IND_IMP_VENCIDO_6190_MDOC, --------------VENCIDO EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.ZMAY90V) * 100 ELSE (FACT.ZMAY90V)
                        END AS IND_IMP_VENCIDO_MAYOR90_MDOC, -----------VENCIDO EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.SVENCIDO) * 100 ELSE (FACT.SVENCIDO)
                        END AS IND_IMP_VENCIDO_TOTAL_MDOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.ZPV0A30) * 100 ELSE (FACT.ZPV0A30)
                        END AS IND_IMP_PORVENCER_030_MDOC, -------------POR VENCER EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.ZPV31A60) * 100 ELSE (FACT.ZPV31A60)
                        END AS IND_IMP_PORVENCER_3160_MDOC, -----------POR VENCER EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.ZPV61A90) * 100 ELSE (FACT.ZPV61A90)
                        END AS IND_IMP_PORVENCER_6190_MDOC, -----------POR VENCER EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.ZPVM90) * 100 ELSE (FACT.ZPVM90)
                        END AS IND_IMP_PORVENCER_MAYOR90_MDOC, ----------POR VENCER EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (FACT.SPORVENCER) * 100 ELSE (FACT.SPORVENCER)
                        END AS IND_IMP_PORVENCER_TOTAL_MDOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE (NULL) END AS IND_IMP_COBRANZA_MDOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE (NULL) END AS IND_IMP_FACTURAS_MDOC,
                FACT.WAERS AS MON_DOC, ------------------------------MONEDA DOCUMENTO
                
                FACT.UKURSB AS TIPOCAMBIO_MONSOC, -------------------TIPO DE CAMBIO - MONEDA SOCIEDAD
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_CARTERA_TOTAL_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_CARTERA_TOTAL_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_CARTERA_TOTAL_MSOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_030_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_VENCIDO_030_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_VENCIDO_030_MSOC, ----------------VENCIDO EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_3160_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_VENCIDO_3160_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_VENCIDO_3160_MSOC, --------------VENCIDO EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_6190_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_VENCIDO_6190_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_VENCIDO_6190_MSOC, --------------VENCIDO EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_MAYOR90_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_VENCIDO_MAYOR90_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_VENCIDO_MAYOR90_MSOC, -----------VENCIDO EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_TOTAL_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_VENCIDO_TOTAL_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_VENCIDO_TOTAL_MSOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_030_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_PORVENCER_030_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_PORVENCER_030_MSOC, -------------POR VENCER EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_3160_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_PORVENCER_3160_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_PORVENCER_3160_MSOC, -----------POR VENCER EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_6190_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_PORVENCER_6190_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_PORVENCER_6190_MSOC, -----------POR VENCER EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_MAYOR90_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_PORVENCER_MAYOR90_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_PORVENCER_MAYOR90_MSOC, ----------POR VENCER EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_TOTAL_MDOC * TIPOCAMBIO_MONSOC) * 100
                        ELSE (IND_IMP_PORVENCER_TOTAL_MDOC * TIPOCAMBIO_MONSOC)
                        END AS IND_IMP_PORVENCER_TOTAL_MSOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE NULL
                        END AS IND_IMP_COBRANZA_MSOC,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE NULL
                        END AS IND_IMP_FACTURAS_MSOC,
                FACT.BWAERS AS MON_SOC, -----------------------------MONEDA SOCIEDAD
                
                FACT.UKURSU AS TIPOCAMBIO_MONUSD, -------------------TIPO DE CAMBIO - MONEDA USD
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_CARTERA_TOTAL_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_CARTERA_TOTAL_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_CARTERA_TOTAL_MUSD,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_030_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_VENCIDO_030_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_VENCIDO_030_MUSD, ----------------VENCIDO EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_3160_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_VENCIDO_3160_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_VENCIDO_3160_MUSD, --------------VENCIDO EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_6190_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_VENCIDO_6190_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_VENCIDO_6190_MUSD, --------------VENCIDO EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_MAYOR90_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_VENCIDO_MAYOR90_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_VENCIDO_MAYOR90_MUSD, -----------VENCIDO EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_VENCIDO_total_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_VENCIDO_total_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_VENCIDO_total_MUSD,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_030_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_PORVENCER_030_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_PORVENCER_030_MUSD, -------------POR VENCER EDAD 0 - 30
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_3160_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_PORVENCER_3160_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_PORVENCER_3160_MUSD, -----------POR VENCER EDAD 31 - 60
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_6190_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_PORVENCER_6190_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_PORVENCER_6190_MUSD, -----------POR VENCER EDAD 61 - 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_MAYOR90_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_PORVENCER_MAYOR90_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_PORVENCER_MAYOR90_MUSD, ----------POR VENCER EDAD > 90
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (IND_IMP_PORVENCER_TOTAL_MDOC * TIPOCAMBIO_MONUSD) * 100
                        ELSE (IND_IMP_PORVENCER_TOTAL_MDOC * TIPOCAMBIO_MONUSD)
                        END AS IND_IMP_PORVENCER_TOTAL_MUSD,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE NULL
                        END AS IND_IMP_COBRANZA_MUSD,
                CASE WHEN FACT.WAERS IN ('COP', 'CLP')
                        THEN (NULL) * 100 ELSE NULL
                        END AS IND_IMP_FACTURAS_MUSD,
                FACT.LWAERS AS MON_USD, -----------------------------MONEDA USD

                FACT.KURST AS TIPOCOTIZACION,
                FACT.TIPO AS TIPO,
        FROM (
                (SELECT BUDAT,
                        BUKRS,
                        VKORG,
                        VTWEG,
                        VKBUR,
                        KUNNR,
                        SOLIC,
                        DESTI,
                        BLART,
                        BELNR,
                        CEBE,
                        HKONT,
                        XBLNR,
                        VBELN,
                        FKART,
                        ZASESOR,
                        ZNASESOR,
                        KKBER,
                        CTLPC,
                        SBGRP,
                        DESCEQUI,
                        GRUPP,
                        DESCGRUP,
                        ZFVEN,
                        WRBTR,
                        Z0A30V,
                        Z31A60V,
                        Z61A90V,
                        ZMAY90V,
                        SVENCIDO,
                        ZPV0A30,
                        ZPV31A60,
                        ZPV61A90,
                        ZPVM90,
                        SPORVENCER,
                        WAERS,
                        UKURSB,
                        BWAERS,
                        UKURSU,
                        LWAERS,
                        KURST,
                        VALID_FROM,
                        TIPO
                FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET) AS FACT
        
                LEFT JOIN
                
                (SELECT DISTINCT SPART,
                        KUNNR
                FROM RAW.SQ1_EXT_0CUST_SALES_TEXT
                WHERE SPART = '00') AS SECTOR
        
                ON FACT.KUNNR = SECTOR.KUNNR

                LEFT OUTER JOIN

                
                (SELECT DISTINCT KUNNR,
                LAND1,
                FROM RAW.SQ1_EXT_0CUSTOMER_ATTR
                WHERE SPRAS = 'S') AS PAIS

                ON FACT.KUNNR = PAIS.KUNNR
        )
        WHERE CONCAT(SUBSTR(FACT.VALID_FROM,1,4),SUBSTR(FACT.VALID_FROM,6,2)) = :ANIOMES AND TIPO='FULL';

        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_CARTERAHIST;

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
    VALUES ('PRE.SP_PRE_FACT_CARTERAHIST','PRE.PFCT_FIN_CARTERAHIST', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 