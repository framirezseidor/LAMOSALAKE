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

        DELETE FROM PRE.PFCT_OPE_ADH_PROD
        WHERE xxx IN (SELECT DISTINCT MBLNR FROM RAW.SQ1_EXT_2LIS_03_BF 
        WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) ------------ULTIMOS REGISTROS--------
                       FROM RAW.SQ1_EXT_2LIS_03_BF));

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
        (
            -- Lista de columnas destino en tabla PRE
            -- Ejemplo: ID, FECHA, CLIENTE_ID, MONTO
        ANIO ,
        MES,
        ANIOMES ,
        SOCIEDAD_ID,
        CENTRO_ID, --,
        ALMACEN_ID,
        CENTRO_COSTO_ID , 
        SOCIEDADCO_ID,
        MATERIAL_ID ,
        LOTE ,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO,
        ORDENFABRICACION_ID ,
        CLAVEMODELO,
        SOLICITANTE,
        IND_INVFABRICACION_UMB ,
        IND_INVFABRICACION_UME ,
        IND_PRODTERMINADA_UME ,
        ID_CANTIDAD ,
        IND_CONSUMO_UMB ,
        IND_INVPRODUCCION_UME ,
        IND_EMBARCADO_UME ,
        IND_AJUSTEINVENTARIO_UME ,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA,
        )
        SELECT
            -- Transformaciones desde RAW
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
        YEAR(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD')) AS ANIO,
        MONTH(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD')) AS MES,
        TO_CHAR(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD'), 'YYYYMM') AS ANIOMES, 
        E2LISBF.BUKRS AS SOCIEDAD_ID,
        E2LISBF.WERKS AS CENTRO_ID,
        E2LISBF.LGORT AS ALMACEN_ID,
        E2LISBF.KOSTL AS CENTRO_COSTO_ID,
        E2LISBF.KOKRS AS SOCIEDADCO_ID,
        E2LISBF.MATNR AS MATERIAL_ID,
        E2LISBF.CHARG AS LOTE,
        E2LISBF.AUFNR AS CLASEORDFAB_ID, 
        E2LISBF.BWART AS CLASEMOVIMIENTO,
        ORDENFABRICACION.ORDENFABRICACION_ID AS ORDENFABRICACION_ID, --AUFNR
        --AFVC.KTSCH AS CLAVEMODELO, 
        PUESTOTRABAJO.ARBPL AS PUESTOTRABAJO_ID,
        E2LISBF.KUNNR AS SOLICITANTE_ID,
        E2LISBF.ZZLABST AS IND_INVFABRICACION_UMB,
        (E2LISBF.ZZLABST * MU.UMREN) / NULLIF(MU.UMREZ, 0) AS IND_INVFABRICACION_UME,

        CASE 
            WHEN E2LISBF.BWART IN ('311', '312') THEN
              CASE
                WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') THEN
                    CASE 
                        WHEN E2LISBF.SHKZG = 'S' THEN (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0) * -1
                        ELSE (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
                    END
                ELSE
                    CASE 
                        WHEN E2LISBF.SHKZG = 'H' THEN (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0) * -1
                        ELSE (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
                    END
                END
            ELSE NULL
        END AS IND_PRODTERMINADA_UME,-- 311,312

        CASE 
            WHEN E2LISBF.LGORT  IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
            WHEN E2LISBF.LGORT  IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
            ELSE E2LISBF.MENGE
        END AS ID_CANTIDAD,

        --E2LISBF AS  IND_CONSUMO_UMB, 
        CASE 
            WHEN E2LISBF.BWART IN ('261', '262') THEN
                CASE
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0) * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0) * -1
                    ELSE (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
                END
        END AS IND_CONSUMO_UMB,

        --E2LISBF AS IND_INVPRODUCCION_UME,
        CASE 
            WHEN E2LISBF.BWART = '101' THEN --131
                (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
        END AS IND_INVPRODUCCION_UME,

        --E2LISBF AS IND_EMBARCADO_UME,
        CASE 
            WHEN E2LISBF.BWART IN ('601') THEN --711 712
                (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
        END AS IND_EMBARCADO_UME,

        --E2LISBF AS IND_AJUSTEINVENTARIO_UME,
        CASE 
            WHEN E2LISBF.BWART IN ('701', '702') THEN --611 612
                CASE
                    WHEN E2LISBF.SHKZG = 'S' THEN (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0) * -1
                    ELSE (E2LISBF.MENGE * MU.UMREN) / NULLIF(MU.UMREZ, 0)
                END
        END AS IND_AJUSTEINVENTARIO_UME,
        E2LISBF.SISORIGEN_ID,
        E2LISBF.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA
    FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF
    LEFT JOIN CON.DIM_DOC_ORDENFABRICACION ORDENFABRICACION
        ON E2LISBF.AUFNR = ORDENFABRICACION.ORDENFABRICACION_ID
    --LEFT JOIN 2LIS_04_P_ARBPL AS E2LISARBPL
        --ON E2LISBF.WERKS = E2LISARBPL.WERKS 
        --AND E2LISBF.AUFNR = E2LISARBPL.AUFNR
    --LEFT JOIN RAW.SQ1_TBL_AFKO AFKO
        --ON E2LISBF.AUFNR = AFKO.AUFNR
    --LEFT JOIN RAW.SQ1_TBL_AFVC AFVC
        --ON AFKO.AUFPL = AFVC.AUFPL
        --WHERE LEFT(E2LISBF.WERKS, 1) = 'R'
    LEFT JOIN RAW.SQ1_EXT_0WORKCENTER_ATTR PUESTOTRABAJO--CON.DIM_XXX_PUESTOTRABAJO PUESTOTRABAJO
        ON E2LISBF.WERKS = PUESTOTRABAJO.WERKS
        AND E2LISBF.KOKRS = PUESTOTRABAJO.KOKRS
    LEFT JOIN RAW.SQ1_EXT_0MAT_UNIT_ATTR AS MU --UNIT
        ON E2LISBF.MATNR = MU.MATNR
    WHERE LEFT(E2LISBF.WERKS, 1) = 'R'

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
    VALUES ('SP_PRE_PFCT_OPE_PROD','PRE.PFCT_OPE_ADH_PROD', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 