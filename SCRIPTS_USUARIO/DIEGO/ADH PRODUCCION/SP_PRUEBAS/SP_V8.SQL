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
        (
        ANIO ,
        MES,
        ANIOMES ,
        --DIA,
        SOCIEDAD_ID,
        CENTRO_ID, 
        ALMACEN_ID,
        CENTROCOSTO_ID , 
        SOCIEDADCO_ID,
        MATERIAL_ID ,
        LOTE ,
        MATERIAL_ENTREGADO_ID,
        TEXTO_CABECERA_DOCUMENTO,
        --FECHA_CONTABILIZACION,
        --DOCMATERIAL,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO,
        ORDENFABRICACION_ID ,
        PUESTOTRABAJO_ID,
        SOLICITANTE_ID,
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
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA
        )
        SELECT
        SUBSTRING(E2LISBF.BUDAT, 1, 4) AS ANIO,           
        SUBSTRING(E2LISBF.BUDAT, 5, 2) AS MES,            
        SUBSTRING(E2LISBF.BUDAT, 1, 6) AS ANIOMES,
        --SUBSTRING(E2LISBF.BUDAT, 7, 2) AS DIA,
        E2LISBF.BUKRS AS SOCIEDAD_ID,
        E2LISBF.WERKS AS CENTRO_ID,
        E2LISBF.LGORT AS ALMACEN_ID,
        E2LISBF.KOSTL AS CENTRO_COSTO_ID,
        E2LISBF.KOKRS AS SOCIEDADCO_ID,
        E2LISBF.MATNR AS MATERIAL_ID,
        E2LISBF.CHARG AS LOTE,
        CASE 
          -- Si movimiento actual es 261 y existe 131 con la misma orden de fabricación
          WHEN E2LISBF.BWART = '261' AND EXISTS (
              SELECT 1
              FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF_13X
              WHERE E2LISBF_13X.AUFNR = E2LISBF.AUFNR
                AND E2LISBF_13X.BWART = '131'
          ) THEN '131'
          
          -- Si movimiento actual 262 y existe 132 con la misma orden de fabricación
          WHEN E2LISBF.BWART = '262' AND EXISTS (
              SELECT 1
              FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF_13X
              WHERE E2LISBF_13X.AUFNR = E2LISBF.AUFNR
                AND E2LISBF_13X.BWART = '132'
          ) THEN '132'


          -- Si movimiento actual es 131 y existe 311 con el mismo texto de cabecera
          WHEN E2LISBF.BWART = '131' AND EXISTS (
              SELECT 1
              FROM RAW.SQ1_TBL_MKPF AS MKPF_311
              INNER JOIN RAW.SQ1_EXT_2LIS_03_BF AS MOV311
                ON MKPF_311.MBLNR = MOV311.MBLNR
              WHERE MKPF_311.BKTXT = MKPF.BKTXT
                AND MOV311.BWART = '311'
          ) THEN '311'
        
        -- Si movimiento actual es 132 y existe 312 con el mismo texto de cabecera
          WHEN E2LISBF.BWART = '132' AND EXISTS (
              SELECT 1
              FROM RAW.SQ1_TBL_MKPF AS MKPF_312
              INNER JOIN RAW.SQ1_EXT_2LIS_03_BF AS MOV312
                ON MKPF_312.MBLNR = MOV312.MBLNR
              WHERE MKPF_312.BKTXT = MKPF.BKTXT
                AND MOV312.BWART = '312'
          ) THEN '312'        

          ELSE NULL
        END AS MATERIAL_ENTREGADO_ID,
        MKPF.BKTXT AS TEXTO_CABECERA_DOCUMENTO,
        --TO_DATE(E2LISBF.BUDAT,'YYYYMMDD') AS FECHA_CONTABILIZACION,
        --E2LISBF.MBLNR AS DOCMATERIAL,
        E2LISBF.AUFNR AS CLASEORDFAB_ID, 
        E2LISBF.BWART AS CLASEMOVIMIENTO,
        ORDENFABRICACION.AUFNR AS ORDENFABRICACION_ID, --AUFNR
        PUESTOTRABAJO.ARBPL AS PUESTOTRABAJO_ID,
        E2LISBF.KUNNR AS SOLICITANTE_ID,
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
        E2LISBF.SISORIGEN_ID,
        E2LISBF.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA
    FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF
    LEFT JOIN RAW.SQ1_EXT_0PRODORDER_ATTR ORDENFABRICACION
        ON E2LISBF.AUFNR = ORDENFABRICACION.AUFNR
    LEFT JOIN RAW.SQ1_EXT_0WORKCENTER_ATTR PUESTOTRABAJO
        ON ORDENFABRICACION.WERKS = PUESTOTRABAJO.WERKS
        AND ORDENFABRICACION.KOKRS = PUESTOTRABAJO.KOKRS
        AND ORDENFABRICACION.ZZARBPL = PUESTOTRABAJO.ARBPL
    LEFT JOIN RAW.SQ1_EXT_0MAT_UNIT_ATTR AS MU --UNIT
        ON E2LISBF.MATNR = MU.MATNR
    LEFT JOIN RAW.SQ1_TBL_MKPF MKPF
        ON E2LISBF.MBLNR = MKPF.MBLNR;
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
 