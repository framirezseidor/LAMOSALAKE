CREATE OR ALTER PROCEDURE PRE.SP_PRE_FACT_FIN_GASTOSREALDELTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP Delta que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_GASTOSREAL
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


        ---------------------------------------------------------
        ------------------ LLENADO DE TABLAS --------------------
        ---------------------------------------------------------
	BEGIN
        // Inicia proceso INSERT 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;


        //Merge entre DELTA y Tabla Espejo
MERGE INTO RAW.SQ1_EXT_0CO_OM_CCA_9_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY KOKRS,
                            BELNR,
                            BUZEI,
                            FISCVAR,
                            CURTYPE,
                            MEASTYPE,
                            VALUTYP

                ORDER BY TS_SEQUENCE_NUMBER DESC, -- Primero por TS_SEQUENCE_NUMBER en orden descendente
                     FECHA_CARGA DESC -- Luego por FECHA_CARGA en orden descendente
            ) AS RN
        FROM RAW.SQ1_EXT_0CO_OM_CCA_9_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
        ON T.KOKRS = S.KOKRS
        AND T.BELNR = S.BELNR
        AND T.BUZEI = S.BUZEI
        AND T.FISCVAR = S.FISCVAR
        AND T.CURTYPE = S.CURTYPE
        AND T.MEASTYPE = S.MEASTYPE
        AND T.VALUTYP = S.VALUTYP


WHEN MATCHED THEN
    UPDATE SET
        T.KOKRS = S.KOKRS,
        T.BELNR = S.BELNR,
        T.BUZEI = S.BUZEI,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.KOSTL = S.KOSTL,
        T.LSTAR = S.LSTAR,
        T.VTYPE = S.VTYPE,
        T.VTDETAIL = S.VTDETAIL,
        T.VTSTAT = S.VTSTAT,
        T.MEASTYPE = S.MEASTYPE,
        T.VERSN = S.VERSN,
        T.VALUTYP = S.VALUTYP,
        T.CORRTYPE = S.CORRTYPE,
        T.KSTAR = S.KSTAR,
        T.SEKNZ = S.SEKNZ,
        T.RSPOBART = S.RSPOBART,
        T.RSPAROBVAL = S.RSPAROBVAL,
        T.CCTR_IBV = S.CCTR_IBV,
        T.SWG = S.SWG,
        T.SWF = S.SWF,
        T.SWV = S.SWV,
        T.SMEG = S.SMEG,
        T.SMEF = S.SMEF,
        T.SMEV = S.SMEV,
        T.WAERS = S.WAERS,
        T.CURTYPE = S.CURTYPE,
        T.MEINH = S.MEINH,
        T.BLDAT = S.BLDAT,
        T.BUDAT = S.BUDAT,
        T.SGTXT = S.SGTXT,
        T.RSAUXACCTYPE = S.RSAUXACCTYPE,
        T.RSAUXACCVAL = S.RSAUXACCVAL,
        T.BUKRS = S.BUKRS,
        T.GSBER = S.GSBER,
        T.FKBER = S.FKBER,
        T.PBUKRS = S.PBUKRS,
        T.PFKBER = S.PFKBER,
        T.WERKS = S.WERKS,
        T.MATNR = S.MATNR,
        T.PERNR = S.PERNR,
        T.KTOPL = S.KTOPL,
        T.SAKNR = S.SAKNR,
        T.LIFNR = S.LIFNR,
        T.KUNNR = S.KUNNR,
        T.REFBT = S.REFBT,
        T.REFBN = S.REFBN,
        T.REFBK = S.REFBK,
        T.REFGJ = S.REFGJ,
        T.REFBZ = S.REFBZ,
        T.QMNUM = S.QMNUM,
        T.UPDMODE = S.UPDMODE,
        T.GEBER = S.GEBER,
        T.PGEBER = S.PGEBER,
        T.GRANT_NBR = S.GRANT_NBR,
        T.PGRANT_NBR = S.PGRANT_NBR,
        T.FIKRS = S.FIKRS,
        T.BUDGET_PD = S.BUDGET_PD,
        T.PBUDGET_PD = S.PBUDGET_PD,
        T.ZZVBUND = S.ZZVBUND,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN
    INSERT (
        KOKRS, BELNR, BUZEI, FISCVAR, FISCPER, KOSTL, LSTAR, VTYPE, VTDETAIL, VTSTAT, 
        MEASTYPE, VERSN, VALUTYP, CORRTYPE, KSTAR, SEKNZ, RSPOBART, RSPAROBVAL, CCTR_IBV, 
        SWG, SWF, SWV, SMEG, SMEF, SMEV, WAERS, CURTYPE, MEINH, BLDAT, BUDAT, SGTXT, 
        RSAUXACCTYPE, RSAUXACCVAL, BUKRS, GSBER, FKBER, PBUKRS, PFKBER, WERKS, MATNR, 
        PERNR, KTOPL, SAKNR, LIFNR, KUNNR, REFBT, REFBN, REFBK, REFGJ, REFBZ, QMNUM, 
        UPDMODE, GEBER, PGEBER, GRANT_NBR, PGRANT_NBR, FIKRS, BUDGET_PD, PBUDGET_PD, 
        ZZVBUND, TIPO, FECHA_CARGA
    )
    VALUES (
        S.KOKRS, S.BELNR, S.BUZEI, S.FISCVAR, S.FISCPER, S.KOSTL, S.LSTAR, S.VTYPE, S.VTDETAIL, S.VTSTAT, 
        S.MEASTYPE, S.VERSN, S.VALUTYP, S.CORRTYPE, S.KSTAR, S.SEKNZ, S.RSPOBART, S.RSPAROBVAL, S.CCTR_IBV, 
        S.SWG, S.SWF, S.SWV, S.SMEG, S.SMEF, S.SMEV, S.WAERS, S.CURTYPE, S.MEINH, S.BLDAT, S.BUDAT, S.SGTXT, 
        S.RSAUXACCTYPE, S.RSAUXACCVAL, S.BUKRS, S.GSBER, S.FKBER, S.PBUKRS, S.PFKBER, S.WERKS, S.MATNR, 
        S.PERNR, S.KTOPL, S.SAKNR, S.LIFNR, S.KUNNR, S.REFBT, S.REFBN, S.REFBK, S.REFGJ, S.REFBZ, S.QMNUM, 
        S.UPDMODE, S.GEBER, S.PGEBER, S.GRANT_NBR, S.PGRANT_NBR, S.FIKRS, S.BUDGET_PD, S.PBUDGET_PD, 
        S.ZZVBUND, S.TIPO, CURRENT_TIMESTAMP
    );
    //Fin Merge

 //Inicio Proceso carga ANIOMES
    TRUNCATE TABLE RAW.SQ1_EXT_0CO_OM_CCA_9_ANIOMESDELTA;
    INSERT INTO RAW.SQ1_EXT_0CO_OM_CCA_9_ANIOMESDELTA 
    SELECT DISTINCT 
            LEFT(FISCPER, 4) || RIGHT('00' || CAST(CAST(RIGHT(FISCPER, 3) AS INT) AS STRING), 2) AS ANIOMES
    FROM 
        RAW.SQ1_EXT_0CO_OM_CCA_9_DELTA;
 //Fin Proceso carga ANIOMES 



//Inicio Borrado ANIOMES tabla PRE de los meses DELTA
    DELETE FROM PRE.PFCT_FIN_GASTOSREAL WHERE ANIOMES IN (SELECT ANIOMES FROM RAW.SQ1_EXT_0CO_OM_CCA_9_ANIOMESDELTA);
//Fin Borrado ANIOMES tabla PRE de los meses DELTA


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
            LTRIM(LIFNR, '0') AS "ACREEDOR_ID",
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
        AND KSTAR BETWEEN '5100000000' AND '6199999999'
        AND ANIOMES IN (SELECT ANIOMES FROM RAW.SQ1_EXT_0CO_OM_CCA_9_ANIOMESDELTA)
        AND (CECO.fecha_desde IS NULL OR CECO.fecha_hasta IS NULL OR CURRENT_DATE BETWEEN CECO.fecha_desde AND CECO.fecha_hasta) --Fecha de validez del CECO
        
        GROUP BY ALL;
        
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
    VALUES ('PRE.SP_PRE_FACT_FIN_GASTOSREALDELTA','PRE.PFCT_FIN_GASTOSREAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;