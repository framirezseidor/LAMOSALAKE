CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_OPE_PROD_ADH()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-06-09
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_OPE_ADH_PROD
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        --DELETE FROM PRE.PFCT_OPE_ADH_PROD
        --WHERE xxx IN (SELECT DISTINCT MBLNR FROM RAW.SQ1_EXT_2LIS_03_BF 
        --WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) ------------ULTIMOS REGISTROS--------
                      -- FROM RAW.SQ1_EXT_2LIS_03_BF));

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

                INSERT INTO PRE.PFCT_OPE_ADH_PROD
        WITH 
        MOV_131 AS (
            SELECT AUFNR, MATNR AS MATERIAL_131,
                   ROW_NUMBER() OVER (PARTITION BY AUFNR ORDER BY BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF
            WHERE BWART = '131'
        ),
        MOV_132 AS (
            SELECT AUFNR, MATNR AS MATERIAL_132,
                   ROW_NUMBER() OVER (PARTITION BY AUFNR ORDER BY BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF
            WHERE BWART = '132'
        ),
        MOV_311 AS (
            SELECT MKPF.BKTXT, BF.MATNR AS MATERIAL_311,
                   ROW_NUMBER() OVER (PARTITION BY MKPF.BKTXT ORDER BY BF.BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF BF
            JOIN RAW.SQ1_TBL_MKPF MKPF ON BF.MBLNR = MKPF.MBLNR
            WHERE BF.BWART = '311'
        ),
        MOV_312 AS (
            SELECT MKPF.BKTXT, BF.MATNR AS MATERIAL_312,
                   ROW_NUMBER() OVER (PARTITION BY MKPF.BKTXT ORDER BY BF.BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF BF
            JOIN RAW.SQ1_TBL_MKPF MKPF ON BF.MBLNR = MKPF.MBLNR
            WHERE BF.BWART = '312'
        ),
        MKPF_INFO AS (
            SELECT MBLNR, BKTXT
            FROM RAW.SQ1_TBL_MKPF
        )
        SELECT
            SUBSTRING(E.BUDAT, 1, 4),
            SUBSTRING(E.BUDAT, 5, 2),
            SUBSTRING(E.BUDAT, 1, 6),
            E.BUKRS, E.WERKS, E.LGORT, E.KOSTL, E.KOKRS, E.MATNR, E.CHARG,
            CASE 
                WHEN E.BWART = '261' THEN M131.MATERIAL_131
                WHEN E.BWART = '262' THEN M132.MATERIAL_132
                WHEN E.BWART IN ('131','311') THEN M311.MATERIAL_311
                WHEN E.BWART IN ('132','312') THEN M312.MATERIAL_312
                ELSE NULL
            END,
            MK.BKTXT,
            E.AUFNR, E.BWART,
            O.AUFNR,
            P.ARBPL,
            E.KUNNR,
            CASE 
                WHEN E.LGORT IN ('NC01', 'NC02', 'PA01') AND E.SHKZG = 'S' THEN E.MENGE * -1
                WHEN E.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E.SHKZG = 'H' THEN E.MENGE * -1
                ELSE E.MENGE
            END,
            CAST((
                CASE 
                    WHEN E.LGORT IN ('NC01', 'NC02', 'PA01') AND E.SHKZG = 'S' THEN E.MENGE * -1
                    WHEN E.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E.SHKZG = 'H' THEN E.MENGE * -1
                    ELSE E.MENGE
                END * COALESCE(MU.UMREN / NULLIF(MU.UMREZ, 0), 1)
            ) AS NUMBER(17,3)),
            NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            E.SISORIGEN_ID,
            E.MANDANTE,
            CURRENT_TIMESTAMP,
            RIGHT(CURRENT_TIMESTAMP, 5)
        FROM RAW.SQ1_EXT_2LIS_03_BF E
        LEFT JOIN RAW.SQ1_EXT_0PRODORDER_ATTR O ON E.AUFNR = O.AUFNR
        LEFT JOIN RAW.SQ1_EXT_0WORKCENTER_ATTR P ON O.WERKS = P.WERKS AND O.KOKRS = P.KOKRS AND O.ZZARBPL = P.ARBPL
        LEFT JOIN RAW.SQ1_EXT_0MAT_UNIT_ATTR MU ON E.MATNR = MU.MATNR
        LEFT JOIN MKPF_INFO MK ON E.MBLNR = MK.MBLNR
        LEFT JOIN MOV_131 M131 ON E.AUFNR = M131.AUFNR AND M131.RN = 1
        LEFT JOIN MOV_132 M132 ON E.AUFNR = M132.AUFNR AND M132.RN = 1
        LEFT JOIN MOV_311 M311 ON MK.BKTXT = M311.BKTXT AND M311.RN = 1
        LEFT JOIN MOV_312 M312 ON MK.BKTXT = M312.BKTXT AND M312.RN = 1
    --AND E2LISBF.FECHA_CARGA::DATE = CURRENT_DATE;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_OPE_ADH_PROD;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PFCT_OPE_PROD_ADH','PRE.PFCT_OPE_ADH_PROD', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 