CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_COM_BACKORDER(
    EXTRACTOR       VARCHAR(50),
    NEGOCIO         VARCHAR(30)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.2
 Fecha de creación:  2025-04-16
 Creador:            Fernando Cuellar
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_COM_BACKORDER,
                     con parámetro de negocio para filtrado en DELETE
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);
    TOTAL_FILAS NUMBER(38,0);
    PARAMETRO       VARCHAR(20);
    SOCIEDAD_INICIO   VARCHAR(20);
    SOCIEDAD_FIN      VARCHAR(20);
    CANAL_INICIO   VARCHAR(20);
    CANAL_FIN      VARCHAR(20);
    UNI_EST_INICIO   VARCHAR(20);
    UNI_EST_FIN      VARCHAR(20);

BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE SOCIEDADES
    ---------------------------------------------------------------------------------
    PARAMETRO := 'VKORG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :SOCIEDAD_INICIO ;

    SOCIEDAD_FIN := SPLIT_PART(SOCIEDAD_INICIO,'-',2);
    SOCIEDAD_INICIO := SPLIT_PART(SOCIEDAD_INICIO,'-',1);    

    PARAMETRO := 'VTWEG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :CANAL_INICIO ;

    CANAL_FIN := SPLIT_PART(CANAL_INICIO,'-',2);
    CANAL_INICIO := SPLIT_PART(CANAL_INICIO,'-',1); 

    PARAMETRO := 'VRKME';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :UNI_EST_INICIO ;

    UNI_EST_FIN := SPLIT_PART(UNI_EST_INICIO,'-',2);
    UNI_EST_INICIO := SPLIT_PART(UNI_EST_INICIO,'-',1); 

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        -- RETURN 'UNI EST INICIO: ' || UNI_EST_INICIO || ', UNIEST FIN: ' || UNI_EST_FIN;

        DELETE FROM PRE.PFCT_COM_BACKORDER
        WHERE
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
            AND CANALDISTRIB_ID BETWEEN :CANAL_INICIO AND :CANAL_FIN
            AND UNI_EST BETWEEN :UNI_EST_INICIO AND :UNI_EST_FIN;

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

        INSERT INTO PRE.PFCT_COM_BACKORDER (
            FECHA_BACKORDER,
            ANIO,
            MES,
            ANIOMES,
            SOCIEDAD_ID,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            CENTRO_ID,
            OFICINAVENTAS_ID,
            ZONAVENTAS_ID,
            GRUPOVENDEDORES_ID,
            UENADHESIVOS_ID,
            PAIS_ID,
            REGION_ID,
            ZONATRANSPORTE_ID,
            PEDIDO,
            PEDIDO_POS,
            CLASEPEDIDO_ID,
            FECHA_DOCUMENTO,
            FECHA_CREACION_PEDIDO,
            FECHA_PEDIDO_CLIENTE,
            FECHA_PREFERENTE_ENTREGA,
            MOTIVOPEDIDO_ID,
            CONDICIONEXP_ID,
            INCOTERMS_ID,
            MOTIVORECHAZO_ID,
            BLOQUEOENTREGA_ID,
            BLOQUEOENTREGA_POS_ID,
            STATUSCREDITO_ID,
            STATUS_BLOQUEADO_TOTAL,
            RANGOANTPED_ID,
            ASESORPEDIDO_ID,
            EJECUTIVOCIS_ID,
            CONSTRUCTORA_ID,
            TIPO_OBRA_ID,
            CONVENIO_OBRA,
            PLAN_OBRA,
            SEGMENTO_OBRA_ID,
            PROMOTOR_ID,
            NIO_OBRA,
            TIPOTRANSPORTE_ID,
            CLIENTE_ID,
            SOLICITANTE_ID,
            DESTINATARIO_ID,
            MATERIAL_ID,
            MATERIALVENTAS_ID,
            MATERIALCENTRO_ID,
            LOTE,
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_EST,
            IND_BO_ENTREGA_EST,
            IND_BO_CONFIRMADO_EST,
            IND_BO_NO_CONFIRMADO_EST,
            UNI_EST,
            IND_BO_MONEDA_DOCUMENTO,
            MON_DOC,
            --NEGOCIO,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            TO_VARCHAR(CURRENT_DATE)                                                  AS FECHA_BACKORDER,
            TO_VARCHAR(CURRENT_DATE, 'YYYY')                                          AS ANIO,
            TO_VARCHAR(CURRENT_DATE, 'MM')                                            AS MES,
            TO_VARCHAR(CURRENT_DATE, 'YYYYMM')                                        AS ANIOMES,
            T.VKORG                                                                   AS SOCIEDAD_ID,
            T.VKORG                                                                   AS ORGVENTAS_ID,
            T.VTWEG                                                                   AS CANALDISTRIB_ID,
            '00'                                                                      AS SECTOR_ID, --SECTOR FIJO A 00
            T.WERKS                                                                   AS CENTRO_ID,
            T.VKBUR                                                                   AS OFICINAVENTAS_ID,
            T.BZIRK                                                                   AS ZONAVENTAS_ID,
            T.VKGRP                                                                   AS GRUPOVENDEDORES_ID,
            -------------------------------------------------------------
            CASE
            WHEN T.VKORG = 'A101' AND T.WERKS IN ('A111', 'A119') THEN 'AMT1'
            WHEN T.VKORG = 'A101' AND T.WERKS IN ('A112', 'A141', 'A126') THEN 'AMX2'
            WHEN T.VKORG = 'A101' AND T.WERKS IN ('A113', 'A114') THEN 'AGD2'
            WHEN T.VKORG = 'A101' AND T.WERKS = 'A115' THEN 'ACH1'
            WHEN T.VKORG = 'A101' AND T.WERKS IN ('A118', 'A124') THEN 'ACA1'
            WHEN T.VKORG = 'A102' AND T.WERKS = 'A121' THEN 'AGD1'
            WHEN T.VKORG = 'A102' AND T.WERKS IN ('A122', 'A119') THEN 'ALE1'
            WHEN T.VKORG = 'A102' AND T.WERKS IN ('A123', 'A141', 'A126', 'A132') THEN 'AMX1'
            WHEN T.VKORG = 'A102' AND T.WERKS = 'A124' THEN 'AMR1'
            WHEN T.VKORG = 'A102' AND T.WERKS = 'A118' THEN 'ACA1'
            WHEN T.VKORG = 'A103' AND T.WERKS = 'A131' AND VKBUR = 'AGD3' THEN 'AGD3'
            WHEN T.VKORG = 'A103' AND T.WERKS = 'A131' AND VKBUR <> 'AGD3' THEN 'ANV1'
            WHEN T.VKORG = 'A103' AND T.WERKS IN ('A132', 'A141', 'A123', 'A126') THEN 'AMX3'
            WHEN T.VKORG = 'A103' AND T.WERKS IN ('A133', 'A121', 'A122') THEN 'AGD3'
            WHEN T.VKORG = 'A103' AND T.WERKS = 'A119' AND VKBUR = 'ACH2' THEN 'ACH2'
            WHEN T.VKORG = 'A103' AND T.WERKS = 'A119' AND VKBUR <> 'ACH2' THEN 'AGD3'
            WHEN T.VKORG = 'A103' AND T.WERKS IN ('A134', 'A115') THEN 'ACH2'
            WHEN T.VKORG = 'A103' AND T.WERKS = 'A135' THEN 'ACH4'
            END                                                                       AS UENADHESIVOS_ID,
            -------------------------------------------------------------
            T.COUNTRY                                                                 AS PAIS_ID,
            T.COUNTRY || '_' || T.REGION                                              AS REGION_ID,
            T.COUNTRY || '_' || T.TRANSPZONE                                          AS ZONATRANSPORTE_ID,
            LTRIM(T.VBELN, '0')                                                       AS PEDIDO,
            T.POSNR                                                                   AS PEDIDO_POS,
            T.AUART                                                                   AS CLASEPEDIDO_ID,
            NULL                                                                      AS FECHA_DOCUMENTO,  -- FECHA DOCUMENTO NO SE ENCUENTRA EN LA FUENTE AUN NULLIF('1990-01-01', '1970-01-01')
            NULLIF(NULLIF(T.ERDAT::DATE, DATE '1970-01-01'), DATE '1990-01-01')       AS FECHA_CREACION_PEDIDO,
            NULLIF(NULLIF(T.BSTDK::DATE, DATE '1970-01-01'), DATE '1990-01-01')       AS FECHA_PEDIDO_CLIENTE,
            NULLIF(NULLIF(T.VDATU::DATE, DATE '1970-01-01'), DATE '1990-01-01')       AS FECHA_PREFERENTE_ENTREGA,    
            T.AUGRU                                                                   AS MOTIVOPEDIDO_ID,
            T.VSBED                                                                   AS CONDICIONEXP_ID,
            T.INCO1                                                                   AS INCOTERMS_ID,
            T.ABGRU                                                                   AS MOTIVORECHAZO_ID,
            T.LIFSK                                                                   AS BLOQUEOENTREGA_ID,
            T.LIFSP                                                                   AS BLOQUEOENTREGA_POS_ID,
            T.CMGST                                                                   AS STATUSCREDITO_ID,
            --------------------------------------------------------------
            CASE 
            WHEN T.LIFSK IS NULL OR T.LIFSK = '' THEN 'C'
            ELSE T.LIFSK
            END                                                                       AS STATUS_BLOQUEADO_TOTAL,
            --------------------------------------------------------------
            CASE 
            WHEN T.ERDAT is null THEN 0
            WHEN DATEDIFF(DAY, T.ERDAT, CURRENT_DATE - 1) <= 15 THEN 1
            WHEN DATEDIFF(DAY, T.ERDAT, CURRENT_DATE - 1) BETWEEN 16 AND 30 THEN 2
            WHEN DATEDIFF(DAY, T.ERDAT, CURRENT_DATE - 1) BETWEEN 31 AND 60 THEN 3
            WHEN DATEDIFF(DAY, T.ERDAT, CURRENT_DATE - 1) BETWEEN 61 AND 90 THEN 4
            WHEN DATEDIFF(DAY, T.ERDAT, CURRENT_DATE - 1) BETWEEN 91 AND 180 THEN 5
            ELSE 6
            END                                                                       AS RANGOANTPED_ID,
            --------------------------------------------------------------
            LTRIM(T.KUNNR_ZV, '0')                                                    AS ASESORPEDIDO_ID,
            T.KUNNR_ZC                                                                AS EJECUTIVOCIS_ID,
            T.CNSTR_ID                                                                AS CONSTRUCTORA_ID,
            --------------------------------------------------------------
            CASE 
            WHEN T.PLAN_OBRA = '' OR T.PLAN_OBRA IS NULL THEN 'NA'
            WHEN LEFT(T.PLAN_OBRA, 2) = 'IN' THEN 'IN'
            WHEN LEFT(T.PLAN_OBRA, 2) = 'CO' THEN 'CO'
            ELSE 'NA'
            END                                                                       AS TIPO_OBRA_ID,
            --------------------------------------------------------------
            T.ID_CONVENIO                                                             AS CONVENIO_OBRA,
            T.PLAN_OBRA                                                               AS PLAN_OBRA,
            LEFT(T.SEGMENTO, 2)                                                       AS SEGMENTO_OBRA_ID,
            T.KUNNR_ZP                                                                AS PROMOTOR_ID,
            'NIO'                                                                     AS NIO_OBRA, ---NIO_OBRA NO SE ENCUENTRA EN LA FUENTE AUN
            --------------------------------------------------------------
            CASE 
                WHEN T.AUART = 'ZMEX' THEN 
                    CASE 
                        WHEN T.INCO1 IN ('ZF0', 'ZF1', 'ZF2', 'ZF3') THEN 'MARITIMO'
                        ELSE 'TERRESTRE'
                    END
                WHEN T.AUART = 'ZEXM' THEN 'MARITIMO'
                ELSE 'TERRESTRE'
            END                                                                       AS TIPOTRANSPORTE_ID,
            --------------------------------------------------------------
            LTRIM(T.KUNNR, '0')                                                       AS CLIENTE_ID,
            T.VKORG || '_' || T.VTWEG || '_' || '00' || '_' || LTRIM(T.KUNNR, '0')    AS SOLICITANTE_ID,
            T.VKORG || '_' || T.VTWEG || '_' || '00' || '_' || LTRIM(T.KUNNR_EM, '0') AS DESTINATARIO_ID,
            LTRIM(T.MATNR, '0')                                                       AS MATERIAL_ID,
            T.VKORG || '_' || T.VTWEG || '_' || LTRIM(T.MATNR, '0')                   AS MATERIALVENTAS_ID,
            T.WERKS || '_' || LTRIM(T.MATNR, '0')                                     AS MATERIALCENTRO_ID,
            T.CHARG                                                                   AS LOTE,
            T.MTART                                                                   AS TIPOMATERIAL_ID,
            T.CANT_PEND                                                               AS IND_BO_TOTAL_EST,
            --------------------------------------------------------------
            T.ZLFIMG - T.ZFKIMG                                                       AS IND_BO_ENTREGA_EST,
            --------------------------------------------------------------
            CASE 
                WHEN T.ABGRU = '' THEN T.BMENG - T.ZLFIMG
                ELSE 0
            END                                                                       AS IND_BO_CONFIRMADO_EST,
            --------------------------------------------------------------
            CASE 
                WHEN T.ABGRU = '' OR T.ABGRU IS NULL THEN T.KWMENG - T.BMENG
                ELSE 0
            END                                                                       AS IND_BO_NO_CONFIRMADO_EST,
            --------------------------------------------------------------
            T.VRKME                                                                   AS UNI_EST,
            --------------------------------------------------------------
            T.VALOR_NETO / T.KWMENG * T.CANT_PEND                                     AS IND_BO_MONEDA_DOCUMENTO,
            --------------------------------------------------------------
            T.MONEDA_DOC                                                              AS MON_DOC,
            --NEGOCIO                                                                 AS NEGOCIO,
            T.SISORIGEN_ID,
            T.MANDANTE,
            CURRENT_TIMESTAMP()                                                       AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM')                                   AS ZONA_HORARIA
        FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER T
        WHERE
            T.VKORG BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
            AND T.VTWEG BETWEEN :CANAL_INICIO AND :CANAL_FIN
            AND T.VRKME BETWEEN :UNI_EST_INICIO AND :UNI_EST_FIN;

        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_COM_BACKORDER
        WHERE
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
            AND CANALDISTRIB_ID BETWEEN :CANAL_INICIO AND :CANAL_FIN
            AND UNI_EST BETWEEN :UNI_EST_INICIO AND :UNI_EST_FIN;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
        SELECT COALESCE(:TEXTO,'EJECUCION CORRECTA') INTO :TEXTO;

        INSERT INTO LOGS.HISTORIAL_EJECUCIONES 
        VALUES('SP_PRE_PFCT_COM_BACKORDER','PFCT_COM_BACKORDER', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );
    
    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    SELECT COUNT(*) INTO TOTAL_FILAS FROM PRE.PFCT_COM_BACKORDER;
    RETURN CASE
        WHEN TEXTO IS NULL OR TEXTO = 'EJECUCION CORRECTA' THEN 
            CONCAT('✅ EJECUCION CORRECTA - Filas insertadas: ', ROWS_INSERTED, 
                ', Total en tabla PRE: ', TOTAL_FILAS)
        
        WHEN ROWS_INSERTED IS NULL THEN 
            '❌ Error: El SP no insertó ni detectó filas'

        ELSE 
            CONCAT('⚠️ Finalizado con advertencia: ', TEXTO)
    END;



    
END;
$$;


-- SELECT pedido, fecha_backorder, fecha_creacion_pedido, fecha_documento, fecha_pedido_cliente, fecha_preferente_entrega FROM CON.FCT_COM_ADH_BACKORDER_ACT;


