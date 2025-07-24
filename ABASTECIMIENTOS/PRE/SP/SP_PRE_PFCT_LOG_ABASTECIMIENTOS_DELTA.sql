CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_LOG_ABASTECIMIENTOS_DELTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            2.0
 Fecha de creación:  2025-04-25
 Creador:            Juan Esteban Méndez N
 Descripción:        SP DELTA que transforma datos desde la capa RAW a PRE para ABASTECIMIENTOS
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);
    FECHA_INICIO    VARCHAR(100);
    FECHA_FIN       VARCHAR(100);
    SISORIGEN       VARCHAR(100);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN

        SELECT R_INICIO --R_INICIO para CARGAS FACT y RFIN para HISTORICOS
        INTO :SISORIGEN 
        FROM RAW.PARAMETROS_EXTRACCION 
        WHERE ORDEN = '1' AND EXTRACTOR = '2LIS_02_SCL' AND NEGOCIO = '' AND PARAMETRO = 'SISORIGEN_ID';

        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM PRE.PFCT_LOG_ABASTECIMIENTOS
        WHERE DOCCOMPRAS IN (SELECT DISTINCT EBELN FROM RAW.SQ1_EXT_2LIS_02_SCL
            WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) ------------ULTIMOS REGISTROS--------
                                FROM RAW.SQ1_EXT_2LIS_02_SCL)
                    AND TIPO = 'DELTA')
        AND SISORIGEN_ID = :SISORIGEN
        ;

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

        INSERT INTO PRE.PFCT_LOG_ABASTECIMIENTOS(
        CENTRO_ID,
        ALMACENCENTRO_ID,
        ORGCOMPRAS_ID,
        GRUPOCOMPRAS_ID,
        MATERIAL_ID,
        MATERIALCENTRO_ID,
        GRUPOARTICULOS_ID,
        PROVEEDOR_ID,
        DOCCOMPRAS_CLASE,
        DOCCOMPRAS_TIPO,
        DOCCOMPRAS,
        DOCCOMPRAS_POS,
        DOCCOMPRAS_REPARTO,
        FECHA_LIBERACION,
        FECHA_DOCCOMPRAS,
        FECHA_ENTREGAPLANIFICADA,
        FECHA_ENTREGAOBJETIVO,
        FECHA_CONTABILIZACION,
        FECHA_REPARTO,
        CLAVEOPERACION,
        SOLPED,
        SOLPED_POS,

        IND_CANTIDAD_UM_PED,
        IND_CANTIDAD_TOTAL,
        UM_PEDIDO,

        IND_IMPTE_COMPRA_PED,
        IND_IMPTE_FLETE_PED,
        IND_IMPTE_COMPRA_SIN_FLETE_PED,
        MON_PED,

        IND_IMPTE_COMPRA_LOC,
        IND_IMPTE_FLETE_LOC,
        IND_IMPTE_COMPRA_SIN_FLETE_LOC ,
        MON_LOC,

        IND_IMPTE_COMPRA_USD,
        IND_IMPTE_FLETE_USD,
        IND_IMPTE_COMPRA_SIN_FLETE_USD,
        MON_USD,

        IND_IMPTE_COMPRA_EUR,
        IND_IMPTE_FLETE_EUR,
        IND_IMPTE_COMPRA_SIN_FLETE_EUR,
        MON_EUR,

        TIPOCAMBIO_USD,
        TIPOCAMBIO_EUR,

        IND_CANT_DIAS_COLOCACION,
        IND_CANT_POSICIONES_PEDIDO,

        CLAVEMP_LOC,
        CLAVEMP_USD,
        CLAVEMP_EUR,

        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA
        )

        WITH REPARTOS AS (

        SELECT
        ROCANCEL AS ind_anulacion,
        BEDAT AS fecha_doccompras,
        BSART AS DOCCOMPRAS_CLASE,
        BSTYP AS DOCCOMPRAS_TIPO,
        BUDAT AS fecha_contabilizacion,
        EBELN AS DOCCOMPRAS,
        EKGRP AS GRUPOCOMPRAS_ID,
        EKORG AS orgcompras_id,
        HWAER AS mon_loc,
        LTRIM(LIFNR,'0') AS proveedor_id,
        STATU AS statusdoccompras,
        SYDAT AS fecha_regcompras,
        WAERS AS mon_ped,
        IFF(BWVORG IN ('001', '004', '005', '011'), WKURS,tc_ped.RATE) AS tipocambio_documento,
        KNUMV AS conddocumento,
        AFNAM AS solicitante_id,
        BWVORG AS claveoperacion,
        EBELP AS DOCCOMPRAS_POS,
        ELIKZ AS ind_entregafinal,
        EREKZ AS ind_factfinal,
        KONNR AS contrato_id,
        KTPNR AS posicioncontrato_id,
        TRIM(LGORT,' ') AS almacen_id,
        CONCAT(TRIM(LGORT,' '),'_',WERKS) AS ALMACENCENTRO_ID,
        LMEIN AS um_base,
        MATKL AS GRUPOARTICULOS_ID,
        LTRIM(TRIM(MATNR,' '),'0') AS material_id ,
        COALESCE(U.VALOR_ESTANDARD, T.MEINS) AS um_pedido,
        NETPR AS precio_neto,
        PEINH AS cantidad_base,
        PSTYP AS tipopos_doccompras,
        TXZ01 AS texto_breve,
        UMREN AS denominador_um_base,
        UMREZ AS numerador_um_base,
        WERKS AS centro_id,
        AKTWE AS cantidad_um_pedido,
        BPRME AS um_preciopedido,
        BANFN AS SOLPED,
        BNFPO AS SOLPED_POS,
        BWBRTWR AS importe_bruto_mon_ped,
        BWEFFWR AS importe_efectivo_mon_ped,
        BWGEO AS importe_compra_mon_loc,
        BWGEOO AS importe_compra_mon_ped,
        BWGVO AS importe_venta_mon_loc,
        BWMNG AS cantidad_um_ped,
        CHARG AS numlote_id,
        DBWGEO AS deltapedido_em,
        DBWMNG AS deltapedido_em_um_base,
        EINDT AS FECHA_ENTREGAPLANIFICADA,
        ETENR AS DOCCOMPRAS_REPARTO,
        SLFDT AS fecha_entregaestadistica,
        ATTYP AS categoriamat_id,
        VLFKZ AS tipocentro_id,
        SCL_BEDAT AS fecha_reparto,
        NOSCL AS contador_repartos_planent,
        BPUMN AS conversion_cantidad,
        ZZFECHA_LIB AS fecha_liberacion,
        ZZKBETR AS importe_condicion,
        ZZKONWA AS ud_condicion,
        ZZKPEIN AS cantidad_basecondicion,
        ZZKMEIN AS um_condicion,
        ZZUKURS AS tc_condicion,
        ZZKBETR_FLETE AS importe_flete,
        ZZUKURS_USD AS tc_usd,
        ZZUKURS_EUR AS tc_eur,

        TS_SEQUENCE_NUMBER, UZEIT as hora_reparto, PERIV as variante_fiscal, --Agregados para llave días de colocación

        CONCAT(LTRIM(TRIM(MATNR,' '),'0'),'_',WERKS) AS MATERIALCENTRO_ID,

        IFF(MATKL IN ('000','001','102','104','107'),DATEADD(day,15,EINDT),IFF(BSTYP IN ('F','L'),DATEADD(day,1,EINDT),EINDT)) AS FECHA_ENTREGAOBJETIVO,

        'USD' AS MON_USD,
        'EUR' AS MON_EUR,

                    ----------------------------------------------------------Rutina impte_compra_ml-------------------------------------------------------------------
        IFF(DOCCOMPRAS_CLASE = 'ZDEV',

                IFF(tipopos_doccompras = '2',-- OR DOCCOMPRAS_CLASE = 'ZIPT',
                    IFF(importe_condicion <> 0,
                        ((importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped) * tc_ped.RATE,
                        (precio_neto * cantidad_um_ped)
                    ),importe_compra_mon_loc) * -1,

                IFF(tipopos_doccompras = '2' OR DOCCOMPRAS_CLASE = 'ZINV',
                    IFF(importe_condicion <> 0,
                        ((importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped) *IFF(LENGTH(tc_ped.RATE) > 0, tc_ped.RATE, IFF(BWVORG IN ('001', '004', '005', '011'), WKURS,tc_ped.RATE)),
                        (precio_neto * cantidad_um_ped)
                    ),importe_compra_mon_loc)) / IFF(mon_ped IN ('USD', 'EUR') AND mon_loc IN ('COP', 'CLP'), 100, 1) AS IND_IMPTE_COMPRA_ML,

        ----------------------------------------------------------------------------------------------------------------------------------------------------
        

        T.SISORIGEN_ID,
        T.MANDANTE,
        T.FECHA_CARGA,
        T.ZONA_HORARIA

        FROM RAW.SQ1_EXT_2LIS_02_SCL AS T
        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS tc_ped
            ON tc_ped.KURST = 'M'
            AND tc_ped.FCURR = MON_PED
            AND tc_ped.TCURR = MON_LOC
            AND tc_ped.GDATU = FECHA_CONTABILIZACION
        LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U
        ON UPPER(TRIM(T.MEINS)) = UPPER(TRIM(U.VALOR_ORIGINAL))
        WHERE DOCCOMPRAS IN (SELECT DISTINCT EBELN FROM RAW.SQ1_EXT_2LIS_02_SCL
            WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) ------------ULTIMOS REGISTROS--------
                                FROM RAW.SQ1_EXT_2LIS_02_SCL)
                    AND TIPO = 'DELTA')
        )
        -----------------------------------BASE------------------------------------------
        SELECT 
        CENTRO_ID,
        ALMACENCENTRO_ID,
        ORGCOMPRAS_ID,
        GRUPOCOMPRAS_ID,
        MATERIAL_ID,
        MATERIALCENTRO_ID,
        GRUPOARTICULOS_ID,
        PROVEEDOR_ID,
        DOCCOMPRAS_CLASE,
        DOCCOMPRAS_TIPO,
        DOCCOMPRAS,
        DOCCOMPRAS_POS,
        DOCCOMPRAS_REPARTO,
        FECHA_LIBERACION,
        FECHA_DOCCOMPRAS,
        FECHA_ENTREGAPLANIFICADA,
        FECHA_ENTREGAOBJETIVO,
        FECHA_CONTABILIZACION,
        FECHA_REPARTO,
        CLAVEOPERACION,
        SOLPED,
        SOLPED_POS,

        IND_CANTIDAD_UM_PED,
        IND_CANTIDAD_TOTAL,
        UM_PEDIDO,

        IND_IMPTE_COMPRA_MP AS IND_IMPTE_COMPRA_PED,
        COALESCE(WRBTR,0) AS IND_IMPTE_FLETE_PED,
        IND_IMPTE_COMPRA_MP-COALESCE(WRBTR,0) AS IND_IMPTE_COMPRA_SIN_FLETE_PED,
        MON_PED,

        IND_IMPTE_COMPRA_ML AS IND_IMPTE_COMPRA_LOC,
        COALESCE(DMBTR,0)  AS IND_IMPTE_FLETE_LOC,
        IND_IMPTE_COMPRA_ML-COALESCE(DMBTR,0) AS IND_IMPTE_COMPRA_SIN_FLETE_LOC,
        MON_LOC,

        IND_IMPTE_COMPRA_MP*TIPOCAMBIO_USD AS IND_IMPTE_COMPRA_USD,
        COALESCE(WRBTR,0)*TIPOCAMBIO_USD AS IND_IMPTE_FLETE_USD,
        (IND_IMPTE_COMPRA_MP-COALESCE(WRBTR,0))*TIPOCAMBIO_USD AS IND_IMPTE_COMPRA_SIN_FLETE_USD,
        MON_USD,

        IND_IMPTE_COMPRA_MP*TIPOCAMBIO_EUR AS IND_IMPTE_COMPRA_EUR,
        COALESCE(WRBTR,0)*TIPOCAMBIO_EUR AS IND_IMPTE_FLETE_EUR,
        (IND_IMPTE_COMPRA_MP-COALESCE(WRBTR,0))*TIPOCAMBIO_EUR AS IND_IMPTE_COMPRA_SIN_FLETE_EUR,
        MON_EUR,

        TIPOCAMBIO_USD,
        TIPOCAMBIO_EUR,

        0 AS IND_CANT_DIAS_COLOCACION ,
        0 AS IND_CANT_POSICIONES_PEDIDO,

        CONCAT(MON_PED,'_',MON_LOC) AS CLAVEMP_LOC,
        CONCAT(MON_PED,'_USD') AS CLAVEMP_USD,
        CONCAT(MON_PED,'_EUR') AS CLAVEMP_EUR,

        FINAL_SELECT.SISORIGEN_ID,
        FINAL_SELECT.MANDANTE,
        FINAL_SELECT.FECHA_CARGA,
        FINAL_SELECT.ZONA_HORARIA

        FROM (

        SELECT

        CENTRO_ID,
        ALMACENCENTRO_ID,
        ORGCOMPRAS_ID,
        GRUPOCOMPRAS_ID,
        MATERIAL_ID,
        MATERIALCENTRO_ID,
        GRUPOARTICULOS_ID,
        PROVEEDOR_ID,
        DOCCOMPRAS_CLASE,
        DOCCOMPRAS_TIPO,
        DOCCOMPRAS,
        DOCCOMPRAS_POS,
        DOCCOMPRAS_REPARTO,
        UM_PEDIDO,
        SOLPED,
        SOLPED_POS,

        FECHA_LIBERACION,
        FECHA_DOCCOMPRAS,
        FECHA_ENTREGAPLANIFICADA,
        FECHA_ENTREGAOBJETIVO,
        FECHA_CONTABILIZACION,
        FECHA_REPARTO,

        CLAVEOPERACION,

        SUM(IFF(MON_PED = 'USD',1,COALESCE(tc_usd.RATE,0))) AS TIPOCAMBIO_USD,
        SUM(IFF(MON_PED = 'EUR',1,COALESCE(tc_eur.RATE,0))) AS TIPOCAMBIO_EUR,

        SUM(cantidad_um_ped) AS IND_CANTIDAD_UM_PED,
        SUM(IFF(DOCCOMPRAS_CLASE = 'ZDEV', cantidad_um_ped*-1, cantidad_um_ped)) AS IND_CANTIDAD_TOTAL,
            
            
        SUM(IND_IMPTE_COMPRA_ML) AS IND_IMPTE_COMPRA_ML,
        
        
            ----------------------------------------------------------Rutina impte_compra_mp-------------------------------------------------------------------
        SUM(IFF(DOCCOMPRAS_CLASE = 'ZDEV', 

            IFF(mon_ped NOT IN ('MXN','PEN','ARS','COP','CLP')
            ,
                        IFF(tipopos_doccompras <> '2' AND DOCCOMPRAS_CLASE <> 'ZIPT',
                            IFF(importe_condicion <> 0, 
                                (importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped,
                                --(precio_neto * cantidad_um_ped) / tipocambio_documento),
                                (IND_IMPTE_COMPRA_ML)/tipocambio_documento),


                            IFF(tipopos_doccompras = '2', --OR DOCCOMPRAS_CLASE = 'ZIPT',
                                IFF(importe_condicion <> 0,
                                    ((importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped) * tipocambio_documento,
                                    ((precio_neto * cantidad_um_ped) / tipocambio_documento) * tipocambio_documento
                                ), importe_compra_mon_ped)),
                                
                        IFF(tipopos_doccompras = '2',-- OR DOCCOMPRAS_CLASE = 'ZIPT',
                            IFF(importe_condicion <> 0,
                                ((importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped) * tipocambio_documento,
                                ((precio_neto * cantidad_um_ped) / tipocambio_documento) * tipocambio_documento
                            ),importe_compra_mon_ped) ) * -1,
                    
                    
                    IFF(mon_ped NOT IN ('MXN','PEN','ARS','COP','CLP'),
                    
                        IFF(tipopos_doccompras <> '2' AND DOCCOMPRAS_CLASE <> 'ZIPT',
                            IFF(importe_condicion <> 0, 
                                (importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad)) * cantidad_um_ped,
                                --(precio_neto * cantidad_um_ped) / tipocambio_documento),
                                (IND_IMPTE_COMPRA_ML)/tipocambio_documento),


                            IFF(tipopos_doccompras = '2', --OR DOCCOMPRAS_CLASE = 'ZIPT',
                                IFF(importe_condicion <> 0,
                                    (importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad))
                                    * cantidad_um_ped,
                                    (precio_neto * cantidad_um_ped) / tipocambio_documento),importe_compra_mon_ped)),
                                    
                        IFF(tipopos_doccompras = '2' OR DOCCOMPRAS_CLASE = 'ZINV',
                            IFF(importe_condicion <> 0,
                                (importe_condicion / IFF(um_pedido = um_preciopedido, cantidad_basecondicion, conversion_cantidad))
                                * cantidad_um_ped,
                                ((precio_neto * cantidad_um_ped) / tipocambio_documento)), importe_compra_mon_ped))
                    
            )) AS IND_IMPTE_COMPRA_MP,

        ---------------------------------------------------------------------------------------------------------------------------------------------------


        MON_LOC,
        MON_PED,
        MON_USD,
        MON_EUR,
        REPARTOS.SISORIGEN_ID,
        REPARTOS.MANDANTE,
        REPARTOS.FECHA_CARGA,
        REPARTOS.ZONA_HORARIA

        FROM REPARTOS
        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS tc_usd
            ON tc_usd.KURST = 'M'
            AND tc_usd.FCURR = MON_PED
            AND tc_usd.TCURR = 'USD'
            AND tc_usd.GDATU = FECHA_CONTABILIZACION 
        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS tc_eur
            ON tc_eur.KURST = 'M'
            AND tc_eur.FCURR = MON_PED
            AND tc_eur.TCURR = 'EUR'
            AND tc_eur.GDATU = FECHA_CONTABILIZACION
        LEFT JOIN PRE.PDIM_ABA_TASACAMBIO AS tc_tipocamb
            ON tc_tipocamb.KURST = 'M'
            AND tc_tipocamb.FCURR = MON_PED
            AND tc_tipocamb.TCURR = MON_LOC
            AND tc_tipocamb.GDATU = FECHA_CONTABILIZACION
        GROUP BY ALL
        ) AS FINAL_SELECT
        LEFT JOIN (----------agrupacion de fletes---------
            SELECT 
            EBELN,
            EBELP,
            BUDAT,
            EKORG,
            WERKS,
            SUM(IFF(SHKZG = 'S',DMBTR*-1,DMBTR)) AS DMBTR,
            SUM(IFF(SHKZG = 'S',WRBTR*-1,WRBTR)) AS WRBTR

            FROM RAW.SQ1_MOF_ZMM_IND_ADQUISICION
            GROUP BY ALL
            ) AS flete ----------llaves para flete-----------
            ON flete.EBELN = FINAL_SELECT.DOCCOMPRAS
            AND flete.EBELP = FINAL_SELECT.DOCCOMPRAS_POS
            AND flete.BUDAT = FINAL_SELECT.FECHA_CONTABILIZACION
            AND flete.EKORG = FINAL_SELECT.ORGCOMPRAS_ID
            AND flete.WERKS = FINAL_SELECT.CENTRO_ID


        ------------------------------------------FIN BASE--------------------------------------
        
        UNION ALL

        ----------------------------------------- DIAS COLOCACION ----------------------------------------
        --------------------------------------------------------------------------------------------------

        SELECT  

        CENTRO_ID,
        ALMACENCENTRO_ID,
        ORGCOMPRAS_ID,
        GRUPOCOMPRAS_ID,
        MATERIAL_ID,
        MATERIALCENTRO_ID,
        GRUPOARTICULOS_ID,
        PROVEEDOR_ID,
        DOCCOMPRAS_CLASE,
        DOCCOMPRAS_TIPO,
        DOCCOMPRAS,
        DOCCOMPRAS_POS,
        DOCCOMPRAS_REPARTO,
        FECHA_LIBERACION,
        FECHA_DOCCOMPRAS,
        FECHA_ENTREGAPLANIFICADA,
        FECHA_ENTREGAOBJETIVO,
        FECHA_CONTABILIZACION,
        FECHA_REPARTO,
        CLAVEOPERACION,
        SOLPED,
        SOLPED_POS,

        0 AS IND_CANTIDAD_UM_PED,
        0 AS IND_CANTIDAD_TOTAL,
        UM_PEDIDO,

        0 AS IND_IMPTE_COMPRA_PED,
        0 AS IND_IMPTE_FLETE_PED,
        0 AS IND_IMPTE_COMPRA_SIN_FLETE_PED,
        MON_PED,

        0 AS IND_IMPTE_COMPRA_LOC,
        0 AS IND_IMPTE_FLETE_LOC,
        0 AS IND_IMPTE_COMPRA_SIN_FLETE_LOC ,
        MON_LOC,

        0 AS IND_IMPTE_COMPRA_USD,
        0 AS IND_IMPTE_FLETE_USD,
        0 AS IND_IMPTE_COMPRA_SIN_FLETE_USD,
        MON_USD,

        0 AS IND_IMPTE_COMPRA_EUR,
        0 AS IND_IMPTE_FLETE_EUR,
        0 AS IND_IMPTE_COMPRA_SIN_FLETE_EUR,
        MON_EUR,

        0 AS TIPOCAMBIO_USD,
        0 AS TIPOCAMBIO_EUR,

        CANT_DIAS_COLOCACION AS IND_CANT_DIAS_COLOCACION ,
        CANT_POSICIONES_PEDIDO AS IND_CANT_POSICIONES_PEDIDO,

        CONCAT(MON_PED,'_',MON_LOC) AS CLAVEMP_LOC,
        CONCAT(MON_PED,'_USD') AS CLAVEMP_USD,
        CONCAT(MON_PED,'_EUR') AS CLAVEMP_EUR,

        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA

            FROM (
 
                SELECT  *,
                        TO_DECIMAL(  CASE   WHEN IND_ANULACION = '' THEN 1
                                                ELSE -1
                                        END * COUNT(*)
                        ) AS CANT_POSICIONES_PEDIDO,
                                    
                        CASE    WHEN FECHA_LIBERACION IS NULL
                                    OR FECHA_LIBERACION = '1970-01-01'
                                    OR FECHA_LIBERACION = '00000000'
                                    OR LENGTH(FECHA_LIBERACION) = 0
                                THEN 0
                                ELSE    CASE    WHEN DOCCOMPRAS_TIPO = 'F'
                                                THEN DATE(FECHA_DOCCOMPRAS) - DATE(FECHA_LIBERACION)
                                                ELSE 0
                                        END
                        END AS AUX,
                        
                        TO_DECIMAL( CASE    WHEN IND_ANULACION = 'R'
                                            THEN AUX * -1
                                            ELSE AUX 
                        END) AS CANT_DIAS_COLOCACION          
                FROM (

                    -------------------------------------------------------------------------------
                    -----------------Segunda transformacion actuaizacion llaves--------------------
                    -------------------------------------------------------------------------------
                    
                    (WITH RankedData2 AS
                    
                        (   SELECT *,
                        
                                    ROW_NUMBER() OVER (PARTITION BY DOCCOMPRAS,
                                                                    DOCCOMPRAS_POS,
                                                                    DOCCOMPRAS_REPARTO,
                                                                    SOLPED,
                                                                    SOLPED_POS,
                                                                    ANIOMES
                                                        ORDER BY    FECHA_CONTABILIZACION DESC,
                                                                    FECHA_CARGA DESC,
                                                                    TS_SEQUENCE_NUMBER DESC
                                   ) AS row_num
                            FROM(
                            
                                SELECT  
                                        SUBSTR(FECHA_DOCCOMPRAS, 0, 4) AS anio,
                                        --YEAR(DATE(FECHA_DOCCOMPRAS)) AS ANIO,
                                        SUBSTR(FECHA_DOCCOMPRAS, 6, 2) AS MES,
                                        --MONTH(DATE(FECHA_DOCCOMPRAS)) AS MES,
                                        CONCAT( SUBSTR(  CASE    WHEN DOCCOMPRAS_TIPO = 'L'
                                                                 THEN FECHA_REPARTO
                                                                 ELSE FECHA_DOCCOMPRAS
                                                         END,1,4),
                                                SUBSTR(  CASE    WHEN DOCCOMPRAS_TIPO = 'L'
                                                                 THEN FECHA_REPARTO
                                                                 ELSE FECHA_DOCCOMPRAS
                                                         END,6,2)
                                        ) AS ANIOMES,
    
                                        CENTRO_ID,
                                        --ALMACEN_ID,
                                        ALMACENCENTRO_ID,
                                        ORGCOMPRAS_ID,
                                        GRUPOCOMPRAS_ID,
                                        MATERIAL_ID,
                                        MATERIALCENTRO_ID,
                                        GRUPOARTICULOS_ID,
                                        PROVEEDOR_ID,
                                        DOCCOMPRAS_CLASE,
                                        DOCCOMPRAS_TIPO,
                                        DOCCOMPRAS,
                                        DOCCOMPRAS_POS,
                                        DOCCOMPRAS_REPARTO,

                                        FECHA_ENTREGAPLANIFICADA,
                                        FECHA_ENTREGAOBJETIVO,

                                        UM_PEDIDO,
                                        MON_LOC,
                                        MON_PED,
                                        MON_USD,
                                        MON_EUR,
                                        SOLPED,
                                        SOLPED_POS,
                                        IND_ANULACION,
                                        TS_SEQUENCE_NUMBER,
                                        
                                        CASE    WHEN DOCCOMPRAS_TIPO = 'F'
                                                THEN
                                                    CASE    WHEN    FECHA_LIBERACION = '00000000'
                                                                OR  FECHA_LIBERACION = '1970-01-01'
                                                                OR LENGTH(FECHA_LIBERACION) = 0
                                                                OR  FECHA_LIBERACION IS NULL
                                                            THEN FECHA_ENTREGAESTADISTICA
                                                            ELSE FECHA_LIBERACION
                                                    END
                                                WHEN DOCCOMPRAS_TIPO = 'L'
                                                THEN FECHA_DOCCOMPRAS
                                                ELSE FECHA_LIBERACION
                                        END AS FECHA_LIBERACION,
                                        FECHA_LIBERACION AS FECHA_LIBERACION_OR, -- Original sin transformar para calculos
                                        CASE    WHEN DOCCOMPRAS_TIPO = 'L'
                                                THEN FECHA_REPARTO
                                                ELSE FECHA_DOCCOMPRAS
                                        END AS FECHA_DOCCOMPRAS,
                                        FECHA_REPARTO, FECHA_CONTABILIZACION,
                                        CLAVEOPERACION,
                                        SISORIGEN_ID,
                                        MANDANTE,
                                        FECHA_CARGA,
                                        ZONA_HORARIA        
                                FROM (
        
                            -------------------------------------------------------------------------------
                            -----------------Primera transformacion actualizacion llaves-------------------
                            -------------------------------------------------------------------------------
                            
                                    WITH RankedData AS
                                    
                                        (SELECT *
                                        ,
                                                ROW_NUMBER() OVER (PARTITION BY ORGCOMPRAS_ID,
                                                                                GRUPOCOMPRAS_ID,
                                                                                ALMACEN_ID,
                                                                                CENTRO_ID, 
                                                                                DOCCOMPRAS_REPARTO,
                                                                                SOLPED,
                                                                                SOLPED_POS, 
                                                                                FECHA_REPARTO,
                                                                                HORA_REPARTO,
                                                                                VARIANTE_FISCAL,
                                                                                NUMLOTE_ID,
                                                                                PROVEEDOR_ID, 
                                                                                DOCCOMPRAS,
                                                                                DOCCOMPRAS_POS,
                                                                                FECHA_CONTABILIZACION
                                                                    ORDER BY    FECHA_CONTABILIZACION DESC,
                                                                                FECHA_CARGA DESC,
                                                                                TS_SEQUENCE_NUMBER DESC
                                               ) AS row_num
                                        FROM REPARTOS)
                                    
                                        SELECT *
                                        FROM RankedData
                                        WHERE row_num = 1
                                )
                            
                            ----------------------------------------------------------------------------------------------
                            ----------------------------------Fin Primera transformacion----------------------------------
                            ----------------------------------------------------------------------------------------------
                            
                            --WHERE ORGCOMPRAS_ID IN ('1001','1002','1003','1201','1202','1203','1204','2001','7204','8204')
                            GROUP BY ALL                           
                            )
                        )
                        
                        SELECT *
                        FROM RankedData2
                        WHERE row_num = 1
                        AND IND_ANULACION <> 'X'
                    )
                    
                    ----------------------------------------------------------------------------------------------
                    ----------------------------------Fin Segunda transformacion----------------------------------
                    ----------------------------------------------------------------------------------------------
                )
                WHERE IND_ANULACION = ''
                GROUP BY ALL
                
        )


---------------------------------------------- FIN DIAS COLOCACION --------------------------------------------------------

;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_LOG_ABASTECIMIENTOS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PFCT_LOG_ABASTECIMIENTOS_DELTA','PRE.PFCT_LOG_ABASTECIMIENTOS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 