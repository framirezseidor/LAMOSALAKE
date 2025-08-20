CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FACT_FIN_GASTOSREAL("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_GASTOSREAL
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
        DELETE FROM PRE.PFCT_FIN_GASTOSREAL
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
        INSERT INTO PRE.PFCT_FIN_GASTOSREAL
        
        SELECT
            'Real' AS "TIPO",
            BUKRS AS "SOCIEDAD_ID",
            WERKS AS "CENTRO_ID",
            KOKRS AS "SOCIEDADCO_ID",
            KTOPL AS "PLANCUENTAS_ID",
            CONCAT(KOKRS,'_',LTRIM(CCA_9.KOSTL, '0')) AS "CENTROCOSTO_ID",
            LTRIM(CCA_9.KOSTL, '0') AS "CENTROCOSTO",
            CONCAT(KOKRS,'_',LTRIM(KSTAR, '0')) AS "CUENTA_ID",
            LTRIM(KSTAR, '0') AS "CUENTA",
            SAKNR AS "CUENTAMAYOR_ID",
            SUBSTR(FISCPER, 0, 4) AS "ANIO",
            SUBSTR(FISCPER, 6, 2) AS "MES",
            CONCAT(SUBSTR(FISCPER, 0, 4), SUBSTR(FISCPER, 6, 2)) AS "ANIOMES",
            TO_DATE(CONCAT(SUBSTR(FISCPER, 0, 4), '-', SUBSTR(FISCPER, 6, 2), '-01')) AS "FECHA_CONTABILIZACION",
            '' AS "VERSION_ID",
            ZZVBUND AS "SOCIEDADGLASOC_ID",
            LIFNR AS "ACREEDOR_ID",
            'ZTPOGASTO' AS "TIPOGASTO_ID",
            CURTYPE AS "TIPO_MONEDA",
            
            IFF(CUENTA.SUBDIVISION_ID = 'Z08', '8', IFF(CUENTA.SUBDIVISION_ID = 'Z25', '9', TIPOGASTO_ID)) AS "GASTOER_ID",
            --'' AS "GASTOER_ID",
            
            0 AS "IND_GASTOPRESUPUESTO_SOC",
            SUM(IFF(CURTYPE = '10', SWG, 0)) AS "IND_GASTOREAL_SOC",
            WAERS AS "MON_SOC",

            0 AS "IND_GASTOPRESUPUESTO_MXN",
            SUM(IFF(CURTYPE = '20', SWG, 0)) AS "IND_GASTOREAL_MXN",
            WAERS AS "MON_MXN",

            0 AS "IND_GASTOPRESUPUESTO_USD",
            SUM(IFNULL(IFF(CURTYPE = '20', SWG * TCUR.RATE, 0), 0))  AS "IND_GASTOREAL_USD",
            'USD' AS "MON_USD",

            CURRENT_TIMESTAMP AS "FECHA_ACTUALIZACION",
            0 AS "IND_GASTOPRESUPUESTO_V0_SOC",
            0 AS "IND_GASTOPRESUPUESTO_V1_SOC",
            0 AS "IND_GASTOPRESUPUESTO_V2_SOC",
            0 AS "IND_GASTOPRESUPUESTO_V3_SOC",
            0 AS "IND_GASTOPRESUPUESTO_V0_MXN",
            0 AS "IND_GASTOPRESUPUESTO_V1_MXN",
            0 AS "IND_GASTOPRESUPUESTO_V2_MXN",
            0 AS "IND_GASTOPRESUPUESTO_V3_MXN",
            0 AS "IND_GASTOPRESUPUESTO_V0_USD",
            0 AS "IND_GASTOPRESUPUESTO_V1_USD",
            0 AS "IND_GASTOPRESUPUESTO_V2_USD",
            0 AS "IND_GASTOPRESUPUESTO_V3_USD",
            CCA_9.MANDANTE AS "MANDANTE",
            CCA_9.SISORIGEN_ID AS "SISORIGEN_ID",
            CCA_9.FECHA_CARGA AS "FECHA_CARGA",
            CCA_9.ZONA_HORARIA "ZONA_HORARIA"
        FROM RAW.SQ1_EXT_0CO_OM_CCA_9 AS CCA_9
        
        LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
        ON CECO.CENTROCOSTO_ID = CCA_9.KOSTL

        LEFT JOIN PRE.PDIM_FIN_CUENTA AS CUENTA
		ON LTRIM(CCA_9.KSTAR,'0') = LTRIM(CUENTA.CUENTA_ID,'0')

        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS TCUR
        ON KURST = 'M' AND FCURR = WAERS AND TCURR = 'USD' AND GDATU = BLDAT
        
        WHERE BUKRS BETWEEN 'A000' AND 'R999'
        AND CURTYPE IN ('10', '20')
        --AND KSTAR BETWEEN '5100000000' AND '6199999999'
        AND FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN

        GROUP BY ALL;


        //BORRADO E INSERSION TABLA SHADOW

    -- PASO 1: ELIMINAR REGISTROS DE SHADOW EN EL RANGO ESPECIFICADO
    DELETE FROM RAW.SQ1_EXT_0CO_OM_CCA_9_SHADOW
    WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN;

    -- PASO 2: INSERTAR REGISTROS DE SHADOW EN EL RANGO ESPECIFICADO
    INSERT INTO RAW.SQ1_EXT_0CO_OM_CCA_9_SHADOW (
        KOKRS,
        BELNR,
        BUZEI,
        FISCVAR,
        FISCPER,
        KOSTL,
        LSTAR,
        VTYPE,
        VTDETAIL,
        VTSTAT,
        MEASTYPE,
        VERSN,
        VALUTYP,
        CORRTYPE,
        KSTAR,
        SEKNZ,
        RSPOBART,
        RSPAROBVAL,
        CCTR_IBV,
        SWG,
        SWF,
        SWV,
        SMEG,
        SMEF,
        SMEV,
        WAERS,
        CURTYPE,
        MEINH,
        BLDAT,
        BUDAT,
        SGTXT,
        RSAUXACCTYPE,
        RSAUXACCVAL,
        BUKRS,
        GSBER,
        FKBER,
        PBUKRS,
        PFKBER,
        WERKS,
        MATNR,
        PERNR,
        KTOPL,
        SAKNR,
        LIFNR,
        KUNNR,
        REFBT,
        REFBN,
        REFBK,
        REFGJ,
        REFBZ,
        QMNUM,
        UPDMODE,
        GEBER,
        PGEBER,
        GRANT_NBR,
        PGRANT_NBR,
        FIKRS,
        BUDGET_PD,
        PBUDGET_PD,
        ZZVBUND,
        MANDANTE,
        SISORIGEN_ID,
        FECHA_CARGA,
        ZONA_HORARIA,
        TIPO)
    SELECT  KOKRS,
            BELNR,
            BUZEI,
            FISCVAR,
            FISCPER,
            KOSTL,
            LSTAR,
            VTYPE,
            VTDETAIL,
            VTSTAT,
            MEASTYPE,
            VERSN,
            VALUTYP,
            CORRTYPE,
            KSTAR,
            SEKNZ,
            RSPOBART,
            RSPAROBVAL,
            CCTR_IBV,
            SWG,
            SWF,
            SWV,
            SMEG,
            SMEF,
            SMEV,
            WAERS,
            CURTYPE,
            MEINH,
            BLDAT,
            BUDAT,
            SGTXT,
            RSAUXACCTYPE,
            RSAUXACCVAL,
            BUKRS,
            GSBER,
            FKBER,
            PBUKRS,
            PFKBER,
            WERKS,
            MATNR,
            PERNR,
            KTOPL,
            SAKNR,
            LIFNR,
            KUNNR,
            REFBT,
            REFBN,
            REFBK,
            REFGJ,
            REFBZ,
            QMNUM,
            UPDMODE,
            GEBER,
            PGEBER,
            GRANT_NBR,
            PGRANT_NBR,
            FIKRS,
            BUDGET_PD,
            PBUDGET_PD,
            ZZVBUND,
            MANDANTE,
            SISORIGEN_ID,
            FECHA_CARGA,
            ZONA_HORARIA,
            TIPO
    FROM RAW.SQ1_EXT_0CO_OM_CCA_9
    WHERE FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN;



        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_GASTOSREAL;

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
    VALUES ('PRE.SP_PRE_FACT_FIN_GASTOSREAL','PRE.PFCT_FIN_GASTOSREAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;