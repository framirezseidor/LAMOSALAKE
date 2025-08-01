CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_OPE_PROD_ADH_PRUEBA(
    EXTRACTOR VARCHAR(50),
    NEGOCIO VARCHAR(50)
)
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_OPE_ADH_PROD_PRUEBA
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

        DELETE FROM PRE.PFCT_OPE_ADH_PROD_PRUEBA;
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

        INSERT INTO PRE.PFCT_OPE_ADH_PROD_PRUEBA
        (
        ANIO ,
        MES,
        ANIOMES ,
        FECHA_CONTABILIZACION,
        --DIA,
        SOCIEDAD_ID,
        CENTRO_ID, 
        ALMACEN_ID,
        ALMACENCENTRO_ID,
        --CENTROCOSTO_ID , 
        --SOCIEDADCO_ID,
        MATERIAL_ID ,
        LOTE ,
        MATERIAL_ENTREGADO_ID,
        TEXTO_CABECERA_DOCUMENTO,
        --DOCMATERIAL,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO_ID,
        ORDENFABRICACION_ID ,
        PUESTOTRABAJO_ID,
        CLIENTE_ID,
        IND_CANTIDAD_UMB,
        IND_CANTIDAD_UME,
        IND_CONSUMO_UMB,
        IND_CONSUMO_UME,
        IND_PRODENTREGADA_UMB,
        IND_PRODENTREGADA_UME,
        IND_PRODTERMINADA_UMB,
        IND_PRODTERMINADA_UME,
        IND_EMBARCADO_UMB,
        IND_EMBARCADO_UME,
        IND_AJUSTEINVENTARIO_UMB,
        IND_AJUSTEINVENTARIO_UME,
        UNI_UMB,
        UNI_EST,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA
        )
        SELECT
        SUBSTRING(E2LISBF.BUDAT, 1, 4) AS ANIO,           
        SUBSTRING(E2LISBF.BUDAT, 5, 2) AS MES,            
        SUBSTRING(E2LISBF.BUDAT, 1, 6) AS ANIOMES,
        TO_DATE(E2LISBF.BUDAT,'YYYYMMDD') AS FECHA_CONTABILIZACION,
        --SUBSTRING(E2LISBF.BUDAT, 7, 2) AS DIA,
        E2LISBF.BUKRS AS SOCIEDAD_ID,
        E2LISBF.WERKS AS CENTRO_ID,
        E2LISBF.LGORT AS ALMACEN_ID,
        CONCAT(E2LISBF.WERKS,'_',E2LISBF.LGORT) AS ALMACENCENTRO_ID,
        --E2LISBF.KOSTL AS CENTROCOSTO_ID,
        --E2LISBF.KOKRS AS SOCIEDADCO_ID,
        CASE 
          WHEN LENGTH(E2LISBF.MATNR) = 18 AND REGEXP_LIKE(E2LISBF.MATNR, '^[0-9]+$') THEN LTRIM(E2LISBF.MATNR, '0')
          ELSE E2LISBF.MATNR
        END AS MATERIAL_ID,
        E2LISBF.CHARG AS LOTE,
CASE
          WHEN E2LISBF.BWART = '261' THEN 
            CASE 
              WHEN LENGTH(MOV131.MATNR) = 18 AND REGEXP_LIKE(MOV131.MATNR, '^[0-9]+$') THEN LTRIM(MOV131.MATNR, '0')
              ELSE MOV131.MATNR 
            END

          WHEN E2LISBF.BWART = '262' THEN 
            CASE 
              WHEN LENGTH(MOV132.MATNR) = 18 AND REGEXP_LIKE(MOV132.MATNR, '^[0-9]+$') THEN LTRIM(MOV132.MATNR, '0')
              ELSE MOV132.MATNR 
            END

          WHEN E2LISBF.BWART = '131' THEN LTRIM(E2LISBF.MATNR, '0')
          
          WHEN E2LISBF.BWART = '132' THEN LTRIM(E2LISBF.MATNR, '0')

          WHEN E2LISBF.BWART  = '311' THEN 
            CASE 
              WHEN LENGTH(MOV311.MATNR) = 18 AND REGEXP_LIKE(MOV311.MATNR, '^[0-9]+$') THEN LTRIM(MOV311.MATNR, '0')
              ELSE MOV311.MATNR 
            END

          WHEN E2LISBF.BWART = '312' THEN 
            CASE 
              WHEN LENGTH(MOV312.MATNR) = 18 AND REGEXP_LIKE(MOV312.MATNR, '^[0-9]+$') THEN LTRIM(MOV312.MATNR, '0')
              ELSE MOV312.MATNR 
            END

          ELSE NULL
        END AS MATERIAL_ENTREGADO_ID,
        MKPF.BKTXT AS TEXTO_CABECERA_DOCUMENTO,
        --E2LISBF.MBLNR AS DOCMATERIAL,
        LTRIM(CLS_ORDNFAB.AUART, '0') AS CLASEORDFAB_ID, 
        E2LISBF.BWART AS CLASEMOVIMIENTO_ID,
        LTRIM(E2LISBF.AUFNR,'0') AS ORDENFABRICACION_ID, 
        PUESTOTRABAJO.ARBPL AS PUESTOTRABAJO_ID,
        CASE 
        WHEN LENGTH(E2LISBF.KUNNR) >= 10 AND REGEXP_LIKE(E2LISBF.KUNNR, '^[0-9]+$') THEN LTRIM(E2LISBF.KUNNR, '0')
        ELSE E2LISBF.KUNNR
        END AS CLIENTE_ID,
        CASE 
            WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
            WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
            ELSE E2LISBF.MENGE
        END AS IND_CANTIDAD_UMB,
        
        CAST(
            (
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END
            )
            *
            CASE 
                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
            END
         AS NUMBER(17,3)) AS IND_CANTIDAD_UME,
        
        CASE
            WHEN E2LISBF.BWART IN ('261','262') THEN 
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END -- IND_CANTIDAD_UMB
            ELSE NULL
        END AS IND_CONSUMO_UMB,
        
        CASE
            WHEN E2LISBF.BWART IN ('261','262') THEN  
                CAST(
                            (
                                CASE 
                                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                                    ELSE E2LISBF.MENGE
                                END
                            )
                            *
                            CASE 
                                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
                            END
                AS NUMBER(17,3)) ---- IND_CANTIDAD_UME
            ELSE NULL
        END AS IND_CONSUMO_UME,
        
        CASE
            WHEN E2LISBF.BWART IN ('131','132') THEN 
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END -- IND_CANTIDAD_UMB
            ELSE NULL
        END AS IND_PRODENTREGADA_UMB,

        CASE
            WHEN E2LISBF.BWART IN ('131','132') THEN  
                CAST(
                            (
                                CASE 
                                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                                    ELSE E2LISBF.MENGE
                                END
                            )
                            *
                            CASE 
                                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
                            END
                AS NUMBER(17,3)) ---- IND_CANTIDAD_UME
            ELSE NULL
        END AS IND_PRODENTREGADA_UME,
        
        CASE
            WHEN E2LISBF.BWART IN ('311','312') THEN 
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END -- IND_CANTIDAD_UMB
            ELSE NULL
        END AS IND_PRODTERMINADA_UMB,

        CASE
            WHEN E2LISBF.BWART IN ('311','312') THEN  
                CAST(
                            (
                                CASE 
                                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                                    ELSE E2LISBF.MENGE
                                END
                            )
                            *
                            CASE 
                                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
                            END
                AS NUMBER(17,3)) ---- IND_CANTIDAD_UME
            ELSE NULL
        END AS IND_PRODTERMINADA_UME,

        CASE
            WHEN E2LISBF.BWART IN ('601','602') THEN 
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END -- IND_CANTIDAD_UMB
            ELSE NULL
        END AS IND_EMBARCADO_UMB,

        CASE
            WHEN E2LISBF.BWART IN ('601','602') THEN  
                CAST(
                            (
                                CASE 
                                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                                    ELSE E2LISBF.MENGE
                                END
                            )
                            *
                            CASE 
                                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
                            END
                AS NUMBER(17,3)) ---- IND_CANTIDAD_UME
            ELSE NULL
        END AS IND_EMBARCADO_UME,

        CASE
            WHEN E2LISBF.BWART IN ('701','702') THEN 
                CASE 
                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                    ELSE E2LISBF.MENGE
                END -- IND_CANTIDAD_UMB
            ELSE NULL
        END AS IND_AJUSTEINVENTARIO_UMB,

        CASE
            WHEN E2LISBF.BWART IN ('701','702') THEN  
                CAST(
                            (
                                CASE 
                                    WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
                                    WHEN E2LISBF.LGORT NOT IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
                                    ELSE E2LISBF.MENGE
                                END
                            )
                            *
                            CASE 
                                WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 THEN MU.UMREN / MU.UMREZ
                                ELSE 1 -- POR SI NO HAY FACTOR DE CONVERSION
                            END
                AS NUMBER(17,3)) ---- IND_CANTIDAD_UME
            ELSE NULL
        END AS IND_AJUSTEINVENTARIO_UME,
        E2LISBF.MEINS AS UNI_UMB,
        CASE 
            WHEN MU.UMREN IS NOT NULL AND MU.UMREZ != 0 AND MU.MEINH = 'TO' THEN 'T'
            ELSE E2LISBF.MEINS
        END AS UNI_EST,
        E2LISBF.SISORIGEN_ID,
        E2LISBF.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA
    FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF
    LEFT JOIN RAW.SQ1_EXT_0PRODORDER_ATTR ORDENFABRICACION
        ON E2LISBF.AUFNR = ORDENFABRICACION.AUFNR
    LEFT JOIN RAW.SQ1_EXT_0COORD_TYPE_TEXT ClS_ORDNFAB
        ON ORDENFABRICACION.AUART = CLS_ORDNFAB.AUART 
        AND ClS_ORDNFAB.LANGU = 'S'
    LEFT JOIN RAW.SQ1_EXT_0WORKCENTER_ATTR PUESTOTRABAJO
        ON ORDENFABRICACION.WERKS = PUESTOTRABAJO.WERKS
        AND ORDENFABRICACION.KOKRS = PUESTOTRABAJO.KOKRS
        AND ORDENFABRICACION.ZZARBPL = PUESTOTRABAJO.ARBPL
    LEFT JOIN RAW.SQ1_EXT_0MAT_UNIT_ATTR AS MU --UNIT
        ON E2LISBF.MATNR = MU.MATNR
    LEFT JOIN RAW.SQ1_TBL_MKPF MKPF
        ON E2LISBF.MBLNR = MKPF.MBLNR
    -- Para 261 → 131
     LEFT JOIN (
        SELECT *
        FROM RAW.SQ1_EXT_2LIS_03_BF
        WHERE BWART = '131'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY AUFNR ORDER BY BUDAT DESC) = 1
    ) MOV131 ON MOV131.AUFNR = E2LISBF.AUFNR
    
    -- Para 262 → 132
    LEFT JOIN (
        SELECT *
        FROM RAW.SQ1_EXT_2LIS_03_BF
        WHERE BWART = '132'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY AUFNR ORDER BY BUDAT DESC) = 1
    ) MOV132 ON MOV132.AUFNR = E2LISBF.AUFNR
    
    LEFT JOIN (
          SELECT *
          FROM (
            SELECT MOV.*, MKPF.BKTXT, ROW_NUMBER() OVER (PARTITION BY MKPF.BKTXT ORDER BY MOV.BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF MOV
            JOIN RAW.SQ1_TBL_MKPF MKPF ON MOV.MBLNR = MKPF.MBLNR
            WHERE MOV.BWART = '311'
          )
          WHERE RN = 1
        ) MOV311
            ON MOV311.BKTXT = MKPF.BKTXT

    LEFT JOIN (
          SELECT *
          FROM (
            SELECT MOV.*, MKPF.BKTXT, ROW_NUMBER() OVER (PARTITION BY MKPF.BKTXT ORDER BY MOV.BUDAT DESC) AS RN
            FROM RAW.SQ1_EXT_2LIS_03_BF MOV
            JOIN RAW.SQ1_TBL_MKPF MKPF ON MOV.MBLNR = MKPF.MBLNR
            WHERE MOV.BWART = '312'
          )
          WHERE RN = 1
        ) MOV312
            ON MOV312.BKTXT = MKPF.BKTXT
    --WHERE E2LISBF.BWART IN ('261','262','131','132','311','312');
    WHERE E2LISBF.BWART IN (
                            SELECT R_INICIO FROM RAW.PARAMETROS_EXTRACCION
                            WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'CLASEMOVIMIENTO%'
                            UNION
                            SELECT R_FIN FROM RAW.PARAMETROS_EXTRACCION
                            WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'CLASEMOVIMIENTO%'
                           )
    AND EXISTS (
        SELECT 1 
        FROM RAW.PARAMETROS_EXTRACCION P
        WHERE P.EXTRACTOR = :EXTRACTOR 
          AND P.NEGOCIO = :NEGOCIO 
          AND P.PARAMETRO LIKE 'CENTRO%' 
          AND (
              E2LISBF.WERKS LIKE TRIM(P.R_INICIO) || '%' 
              OR E2LISBF.WERKS LIKE TRIM(P.R_FIN) || '%'
          )
    )
    AND NOT (
            E2LISBF.BWART IN ('311', '312')
            AND ALMACEN_ID LIKE 'FB%'
        );
    --AND  SUBSTRING(E2LISBF.BUDAT, 5, 2) = '06';
    --AND E2LISBF.FECHA_CARGA::DATE = CURRENT_DATE;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_OPE_ADH_PROD_PRUEBA;

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
    VALUES ('SP_PRE_PFCT_OPE_PROD_ADH_PRUEBA','PRE.PFCT_OPE_ADH_PROD_PRUEBA', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 