CREATE OR ALTER PROCEDURE PRE.SP_PRE_FACT_FIN_GASTOSPRESUPUESTO("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_GASTOSPRESUPUESTO
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
        DELETE FROM PRE.PFCT_FIN_GASTOSPRESUPUESTO
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
        INSERT INTO PRE.PFCT_FIN_GASTOSPRESUPUESTO
        
        SELECT
            'Presupuesto' AS "TIPO",
            '' AS "SOCIEDAD_ID",
            '' AS "CENTRO_ID",
            KOKRS AS "SOCIEDADCO_ID",
            '' AS "PLANCUENTAS_ID",
            CONCAT(KOKRS,'_',LTRIM(CCA_1.KOSTL, '0')) AS "CENTROCOSTO_ID",
            LTRIM(CCA_1.KOSTL, '0') AS "CENTROCOSTO",
            CONCAT(KOKRS,'_',LTRIM(KSTAR, '0')) AS "CUENTA_ID",
            LTRIM(KSTAR, '0') AS "CUENTA",
            '' AS "CUENTAMAYOR_ID",
            SUBSTR(FISCPER, 0, 4) AS "ANIO",
            SUBSTR(FISCPER, 6, 2) AS "MES",
            CONCAT(SUBSTR(FISCPER, 0, 4), SUBSTR(FISCPER, 6, 2)) AS "ANIOMES",
            TO_DATE(CONCAT(SUBSTR(FISCPER, 0, 4), '-', SUBSTR(FISCPER, 6, 2), '-01')) AS "FECHA_CONTABILIZACION",
            VERSN AS "VERSION_ID",
            '' AS "SOCIEDADGLASOC_ID",
            '' AS "ACREEDOR_ID",
            'ZTPOGASTO' AS "TIPOGASTO_ID",
            CURTYPE AS "TIPO_MONEDA",

            IFF(CUENTA.SUBDIVISION_ID = 'Z08', '8', IFF(CUENTA.SUBDIVISION_ID = 'Z25', '9', TIPOGASTO_ID)) AS "GASTOER_ID",
            --'' AS "GASTOER_ID",
            
            IFF(WAERS IN ('COP','CLP'),
                (SUM(IFF(CURTYPE = '10', SWG, 0))) * 100,
                (SUM(IFF(CURTYPE = '10', SWG, 0)))) AS "IND_GASTOPRESUPUESTO_SOC",
            0 AS "IND_GASTOREAL_SOC",
            WAERS AS "MON_SOC",

            SUM(IFF(CURTYPE = '20', SWG, 0)) AS "IND_GASTOPRESUPUESTO_MXN",
            0 AS "IND_GASTOREAL_MXN",
            WAERS AS "MON_MXN",

            SUM(IFNULL(IFF(CURTYPE = '10', SWG * TCUR.RATE, 0), 0)) AS "IND_GASTOPRESUPUESTO_USD",
            0 AS "IND_GASTOREAL_USD",
            'USD' AS "MON_USD",

            CURRENT_TIMESTAMP AS "FECHA_ACTUALIZACION",

            IFF(MON_SOC IN ('COP','CLP'),
                (IFF(VERSION_ID = '000', IND_GASTOPRESUPUESTO_SOC, 0)) * 100,
                (IFF(VERSION_ID = '000', IND_GASTOPRESUPUESTO_SOC, 0))) AS "IND_GASTOPRESUPUESTO_V0_SOC",
            IFF(MON_SOC IN ('COP','CLP'),
                (IFF(VERSION_ID = '001', IND_GASTOPRESUPUESTO_SOC, 0)) * 100,
                (IFF(VERSION_ID = '001', IND_GASTOPRESUPUESTO_SOC, 0))) AS "IND_GASTOPRESUPUESTO_V1_SOC",
            IFF(MON_SOC IN ('COP','CLP'),
                (IFF(VERSION_ID = '002', IND_GASTOPRESUPUESTO_SOC, 0)) * 100,
                (IFF(VERSION_ID = '002', IND_GASTOPRESUPUESTO_SOC, 0))) AS "IND_GASTOPRESUPUESTO_V2_SOC",
            IFF(MON_SOC IN ('COP','CLP'),
                (IFF(VERSION_ID = '003', IND_GASTOPRESUPUESTO_SOC, 0)) * 100,
                (IFF(VERSION_ID = '003', IND_GASTOPRESUPUESTO_SOC, 0))) AS "IND_GASTOPRESUPUESTO_V3_SOC",

            IFF(VERSION_ID = '000', IND_GASTOPRESUPUESTO_MXN, 0) AS "IND_GASTOPRESUPUESTO_V0_MXN",
            IFF(VERSION_ID = '001', IND_GASTOPRESUPUESTO_MXN, 0) AS "IND_GASTOPRESUPUESTO_V1_MXN",
            IFF(VERSION_ID = '002', IND_GASTOPRESUPUESTO_MXN, 0) AS "IND_GASTOPRESUPUESTO_V2_MXN",
            IFF(VERSION_ID = '003', IND_GASTOPRESUPUESTO_MXN, 0) AS "IND_GASTOPRESUPUESTO_V3_MXN",

            IFF(VERSION_ID = '000', IND_GASTOPRESUPUESTO_USD, 0) AS "IND_GASTOPRESUPUESTO_V0_USD",
            IFF(VERSION_ID = '001', IND_GASTOPRESUPUESTO_USD, 0) AS "IND_GASTOPRESUPUESTO_V1_USD",
            IFF(VERSION_ID = '002', IND_GASTOPRESUPUESTO_USD, 0) AS "IND_GASTOPRESUPUESTO_V2_USD",
            IFF(VERSION_ID = '003', IND_GASTOPRESUPUESTO_USD, 0) AS "IND_GASTOPRESUPUESTO_V3_USD",
            
            CCA_1.MANDANTE AS "MANDANTE",
            CCA_1.SISORIGEN_ID AS "SISORIGEN_ID",
            CCA_1.FECHA_CARGA AS "FECHA_CARGA",
            CCA_1.ZONA_HORARIA "ZONA_HORARIA"
        FROM RAW.SQ1_EXT_0CO_OM_CCA_1 AS CCA_1
        
        LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
        ON LTRIM(CECO.CENTROCOSTO_ID, '0') = LTRIM(CCA_1.KOSTL, '0') AND CCA_1.KOKRS = CECO.SOCIEDADCO_ID
        AND (CECO.FECHA_DESDE IS NULL OR CECO.FECHA_HASTA IS NULL OR CURRENT_DATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA) --Fecha de validez del CECO

        LEFT JOIN PRE.PDIM_FIN_CUENTA AS CUENTA
		ON LTRIM(CCA_1.KSTAR,'0') = LTRIM(CUENTA.CUENTA_ID,'0')

        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS TCUR
        ON KURST = 'M' AND FCURR = WAERS AND TCURR = 'USD' AND GDATU = LAST_DAY(TO_DATE(CONCAT(SUBSTR(FISCPER, 0, 4), SUBSTR(FISCPER, 6, 2), '01'), 'YYYYMMDD'))

        WHERE CURTYPE IN ('10', '20')
		--AND KSTAR BETWEEN '5100000000' AND '6199999999' -- VALIDAR CUENTAS
		AND VTYPE = 20
		AND FISCPER BETWEEN :FECHA_INICIO AND :FECHA_FIN

        GROUP BY ALL;
        
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
    VALUES ('PRE.SP_PRE_FACT_FIN_GASTOSPRESUPUESTO','PRE.PFCT_FIN_GASTOSPRESUPUESTO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;