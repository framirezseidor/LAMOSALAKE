        SELECT
            -- Aquí van tus transformaciones. EJEMPLO:
            TO_VARCHAR(ERDAT, 'YYYY') AS ANIO,
            TO_VARCHAR(ERDAT, 'MM') AS MES,
            TO_VARCHAR(ERDAT, 'YYYYMM') AS ANIOMES,
            VKORG AS SOCIEDAD_ID,
            VKORG AS ORGVENTAS_ID,
            VTWEG AS CANALDISTRIB_ID,
            00 AS SECTOR_ID,
            WERKS AS CENTRO_ID,
            VKBUR AS OFICINAVENTAS_ID,
            BZIRK AS ZONAVENTAS_ID,
            VKGRP GRUPOVENDEDORES_ID,
            -------------------------------------------------------------
            CASE
            WHEN VKORG = 'A101' AND WERKS IN ('A111', 'A119') THEN 'AMT1'
            WHEN VKORG = 'A101' AND WERKS IN ('A112', 'A141', 'A126') THEN 'AMX2'
            WHEN VKORG = 'A101' AND WERKS IN ('A113', 'A114') THEN 'AGD2'
            WHEN VKORG = 'A101' AND WERKS = 'A115' THEN 'ACH1'
            WHEN VKORG = 'A101' AND WERKS IN ('A118', 'A124') THEN 'ACA1'
            WHEN VKORG = 'A102' AND WERKS = 'A121' THEN 'AGD1'
            WHEN VKORG = 'A102' AND WERKS IN ('A122', 'A119') THEN 'ALE1'
            WHEN VKORG = 'A102' AND WERKS IN ('A123', 'A141', 'A126', 'A132') THEN 'AMX1'
            WHEN VKORG = 'A102' AND WERKS = 'A124' THEN 'AMR1'
            WHEN VKORG = 'A102' AND WERKS = 'A118' THEN 'ACA1'
            WHEN VKORG = 'A103' AND WERKS = 'A131' AND VKBUR = 'AGD3' THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS = 'A131' AND VKBUR <> 'AGD3' THEN 'ANV1'
            WHEN VKORG = 'A103' AND WERKS IN ('A132', 'A141', 'A123', 'A126') THEN 'AMX3'
            WHEN VKORG = 'A103' AND WERKS IN ('A133', 'A121', 'A122') THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS = 'A119' AND VKBUR = 'ACH2' THEN 'ACH2'
            WHEN VKORG = 'A103' AND WERKS = 'A119' AND VKBUR <> 'ACH2' THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS IN ('A134', 'A115') THEN 'ACH2'
            WHEN VKORG = 'A103' AND WERKS = 'A135' THEN 'ACH4'
            END AS UEN_ADHESIVOS_ID,
            -------------------------------------------------------------
            COUNTRY AS PAIS_ID,
            COUNTRY || '_' || REGION AS REGION_ID,
            COUNTRY || '_' || TRANSPZONE AS ZONATRANSPORTE_ID,
            VBELN AS PEDIDO,
            POSNR AS PEDIDO_POS,
            AUART AS CLASEPEDIDO_ID,
            '1990-01-01' AS FECHA_DOCUMENTO,
            ERDAT AS FECHA_CREACION_PEDIDO,
            BSTDK AS FECHA_PEDIDO_CLIENTE,
            VDATU AS FECHA_PREFERENTE_ENTREGA,
            AUGRU AS MOTIVOPEDIDO_ID,
            VSBED AS CONDICIONEXP_ID,
            INCO1 AS INCOTERMS_ID,
            ABGRU AS MOTIVORECHAZO_ID,
            LIFSK AS BLOQUEOENTREGA_ID,
            LIFSP AS BLOQUEOENTREGA_POS_ID,
            CMGST AS STATUSCREDITO_ID,
            --------------------------------------------------------------
            CASE 
            WHEN LIFSK IS NULL OR LIFSK = '' THEN 'C'
            ELSE LIFSK
            END AS STATUS_BLOQUEADO_TOTAL,
            --------------------------------------------------------------
            CASE 
            WHEN ERDAT is null THEN 0
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) <= 15 THEN 1
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 16 AND 30 THEN 2
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 31 AND 60 THEN 3
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 61 AND 90 THEN 4
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 91 AND 180 THEN 5
            ELSE 6
            END AS RANGOANTPED_ID,
            --------------------------------------------------------------
            KUNNR_ZV AS ASESORFACTURA_ID,
            KUNNR_ZC AS EJECUTIVOCIS_ID,
            CNSTR_ID AS CONSTRUCTORA_ID,
            --------------------------------------------------------------
            CASE 
            WHEN PLAN_OBRA = '' OR PLAN_OBRA IS NULL THEN 'NA'
            WHEN LEFT(PLAN_OBRA, 2) = 'IN' THEN 'IN'
            WHEN LEFT(PLAN_OBRA, 2) = 'CO' THEN 'CO'
            ELSE 'NA'
            END AS TIPO_OBRA_ID,
            --------------------------------------------------------------
            ID_CONVENIO AS CONVENIO_OBRA,
            PLAN_OBRA AS PLAN_OBRA,
            SEGMENTO AS SEGMENTO_OBRA_ID,
            KUNNR_ZP AS PROMOTOR_ID,
            'NIO' AS NIO_OBRA,
            --------------------------------------------------------------
            CASE 
                WHEN AUART = 'ZMEX' THEN 
                    CASE 
                        WHEN INCO1 IN ('ZF0', 'ZF1', 'ZF2', 'ZF3') THEN 'MARITIMO'
                        ELSE 'TERRESTRE'
                    END
                WHEN AUART = 'ZEXM' THEN 'MARITIMO'
                ELSE 'TERRESTRE'
            END AS TIPOTRANSPORTE_ID,
            --------------------------------------------------------------
            KUNNR AS CLIENTE_ID,
            VKORG || '_' || VTWEG || '_' || '00' || '_' || KUNNR AS SOLICITANTE_ID,
            VKORG || '_' || VTWEG || '_' || '00' || '_' || KUNNR_EM AS DESTINATARIO_ID,
            MATNR AS MATERIAL_ID,
            VKORG || '_' || VTWEG || '_' || MATNR AS MATERIALVENTAS_ID,
            WERKS || '_' || MATNR AS MATERIALCENTRO_ID,
            CHARG AS LOTE,
            MTART AS TIPOMATERIAL_ID,
            CANT_PEND AS IND_BO_TOTAL_EST,
            --------------------------------------------------------------
            ZLFIMG - ZFKIMG AS IND_BO_ENTREGA_EST,
            --------------------------------------------------------------
            CASE 
                WHEN ABGRU = '' THEN BMENG - ZLFIMG
                ELSE 0
            END AS IND_BO_CONFIRMADO_EST,
            --------------------------------------------------------------
            CASE 
                WHEN ABGRU = '' OR ABGRU IS NULL THEN KWMENG - BMENG
                ELSE 0
            END AS IND_BO_NO_CONFIRMADO_EST,
            --------------------------------------------------------------
            VRKME AS UNI_EST,
            --------------------------------------------------------------
            VALOR_NETO / KWMENG * CANT_PEND AS IND_BO_MONEDA_DOCUMENTO,
            --------------------------------------------------------------
            MONEDA_DOC AS MON_DOC,
            ID_SISORIGEN,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER;



INSERT INTO LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER (
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
            UEN_ADHESIVOS_ID,
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
            ASESORFACTURA_ID,
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
            ID_SISORIGEN,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            TO_VARCHAR(ERDAT, 'YYYY')   AS ANIO,
            TO_VARCHAR(ERDAT, 'MM')     AS MES,
            TO_VARCHAR(ERDAT, 'YYYYMM') AS ANIOMES,
            VKORG                       AS SOCIEDAD_ID,
            VKORG                       AS ORGVENTAS_ID,
            VTWEG                       AS CANALDISTRIB_ID,
            '00'                        AS SECTOR_ID, --SECTOR FIJO A 00
            WERKS                       AS CENTRO_ID,
            VKBUR                       AS OFICINAVENTAS_ID,
            BZIRK                       AS ZONAVENTAS_ID,
            VKGRP                       AS GRUPOVENDEDORES_ID,
            -------------------------------------------------------------
            CASE
            WHEN VKORG = 'A101' AND WERKS IN ('A111', 'A119') THEN 'AMT1'
            WHEN VKORG = 'A101' AND WERKS IN ('A112', 'A141', 'A126') THEN 'AMX2'
            WHEN VKORG = 'A101' AND WERKS IN ('A113', 'A114') THEN 'AGD2'
            WHEN VKORG = 'A101' AND WERKS = 'A115' THEN 'ACH1'
            WHEN VKORG = 'A101' AND WERKS IN ('A118', 'A124') THEN 'ACA1'
            WHEN VKORG = 'A102' AND WERKS = 'A121' THEN 'AGD1'
            WHEN VKORG = 'A102' AND WERKS IN ('A122', 'A119') THEN 'ALE1'
            WHEN VKORG = 'A102' AND WERKS IN ('A123', 'A141', 'A126', 'A132') THEN 'AMX1'
            WHEN VKORG = 'A102' AND WERKS = 'A124' THEN 'AMR1'
            WHEN VKORG = 'A102' AND WERKS = 'A118' THEN 'ACA1'
            WHEN VKORG = 'A103' AND WERKS = 'A131' AND VKBUR = 'AGD3' THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS = 'A131' AND VKBUR <> 'AGD3' THEN 'ANV1'
            WHEN VKORG = 'A103' AND WERKS IN ('A132', 'A141', 'A123', 'A126') THEN 'AMX3'
            WHEN VKORG = 'A103' AND WERKS IN ('A133', 'A121', 'A122') THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS = 'A119' AND VKBUR = 'ACH2' THEN 'ACH2'
            WHEN VKORG = 'A103' AND WERKS = 'A119' AND VKBUR <> 'ACH2' THEN 'AGD3'
            WHEN VKORG = 'A103' AND WERKS IN ('A134', 'A115') THEN 'ACH2'
            WHEN VKORG = 'A103' AND WERKS = 'A135' THEN 'ACH4'
            END AS UEN_ADHESIVOS_ID,
            -------------------------------------------------------------
            COUNTRY                      AS PAIS_ID,
            COUNTRY || '_' || REGION     AS REGION_ID,
            COUNTRY || '_' || TRANSPZONE AS ZONATRANSPORTE_ID,
            VBELN                        AS PEDIDO,
            POSNR                        AS PEDIDO_POS,
            AUART                        AS CLASEPEDIDO_ID,
            '1990-01-01'                 AS FECHA_DOCUMENTO, --- FECHA DOCUMENTO NO SE ENCUENTRA EN LA FUENTE AUN
            ERDAT                        AS FECHA_CREACION_PEDIDO,
            BSTDK                        AS FECHA_PEDIDO_CLIENTE,
            VDATU                        AS FECHA_PREFERENTE_ENTREGA,
            AUGRU                        AS MOTIVOPEDIDO_ID,
            VSBED                        AS CONDICIONEXP_ID,
            INCO1                        AS INCOTERMS_ID,
            ABGRU                        AS MOTIVORECHAZO_ID,
            LIFSK                        AS BLOQUEOENTREGA_ID,
            LIFSP                        AS BLOQUEOENTREGA_POS_ID,
            CMGST                        AS STATUSCREDITO_ID,
            --------------------------------------------------------------
            CASE 
            WHEN LIFSK IS NULL OR LIFSK = '' THEN 'C'
            ELSE LIFSK
            END AS STATUS_BLOQUEADO_TOTAL,
            --------------------------------------------------------------
            CASE 
            WHEN ERDAT is null THEN 0
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) <= 15 THEN 1
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 16 AND 30 THEN 2
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 31 AND 60 THEN 3
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 61 AND 90 THEN 4
            WHEN DATEDIFF(DAY, ERDAT, CURRENT_DATE - 1) BETWEEN 91 AND 180 THEN 5
            ELSE 6
            END AS RANGOANTPED_ID,
            --------------------------------------------------------------
            KUNNR_ZV AS ASESORFACTURA_ID,
            KUNNR_ZC AS EJECUTIVOCIS_ID,
            CNSTR_ID AS CONSTRUCTORA_ID,
            --------------------------------------------------------------
            CASE 
            WHEN PLAN_OBRA = '' OR PLAN_OBRA IS NULL THEN 'NA'
            WHEN LEFT(PLAN_OBRA, 2) = 'IN' THEN 'IN'
            WHEN LEFT(PLAN_OBRA, 2) = 'CO' THEN 'CO'
            ELSE 'NA'
            END AS TIPO_OBRA_ID,
            --------------------------------------------------------------
            ID_CONVENIO AS CONVENIO_OBRA,
            PLAN_OBRA   AS PLAN_OBRA,
            SEGMENTO    AS SEGMENTO_OBRA_ID,
            KUNNR_ZP    AS PROMOTOR_ID,
            'NIO'       AS NIO_OBRA, ---NIO_OBRA NO SE ENCUENTRA EN LA FUENTE AUN
            --------------------------------------------------------------
            CASE 
                WHEN AUART = 'ZMEX' THEN 
                    CASE 
                        WHEN INCO1 IN ('ZF0', 'ZF1', 'ZF2', 'ZF3') THEN 'MARITIMO'
                        ELSE 'TERRESTRE'
                    END
                WHEN AUART = 'ZEXM' THEN 'MARITIMO'
                ELSE 'TERRESTRE'
            END AS TIPOTRANSPORTE_ID,
            --------------------------------------------------------------
            KUNNR                                                   AS CLIENTE_ID,
            VKORG || '_' || VTWEG || '_' || '00' || '_' || KUNNR    AS SOLICITANTE_ID,
            VKORG || '_' || VTWEG || '_' || '00' || '_' || KUNNR_EM AS DESTINATARIO_ID,
            MATNR                                                   AS MATERIAL_ID,
            VKORG || '_' || VTWEG || '_' || MATNR                   AS MATERIALVENTAS_ID,
            WERKS || '_' || MATNR                                   AS MATERIALCENTRO_ID,
            CHARG                                                   AS LOTE,
            MTART                                                   AS TIPOMATERIAL_ID,
            CANT_PEND                                               AS IND_BO_TOTAL_EST,
            --------------------------------------------------------------
            ZLFIMG - ZFKIMG                                         AS IND_BO_ENTREGA_EST,
            --------------------------------------------------------------
            CASE 
                WHEN ABGRU = '' THEN BMENG - ZLFIMG
                ELSE 0
            END                                                     AS IND_BO_CONFIRMADO_EST,
            --------------------------------------------------------------
            CASE 
                WHEN ABGRU = '' OR ABGRU IS NULL THEN KWMENG - BMENG
                ELSE 0
            END                                                     AS IND_BO_NO_CONFIRMADO_EST,
            --------------------------------------------------------------
            VRKME                                                   AS UNI_EST,
            --------------------------------------------------------------
            VALOR_NETO / KWMENG * CANT_PEND                         AS IND_BO_MONEDA_DOCUMENTO,
            --------------------------------------------------------------
            MONEDA_DOC AS MON_DOC,
            ID_SISORIGEN,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER;


SELECT * FROM LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER;

SELECT
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
    --UEN_ADHESIVOS_ID,
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
    ASESORFACTURA_ID,
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
    ID_SISORIGEN,
    MANDANTE,
    FECHA_CARGA,
    ZONA_HORARIA
FROM LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER;

SELECT
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
            UEN_ADHESIVOS_ID,
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
            ASESORFACTURA_ID,
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
            --LOTE,
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_EST,
            IND_BO_ENTREGA_EST,
            IND_BO_CONFIRMADO_EST,
            IND_BO_NO_CONFIRMADO_EST,
            UNI_EST,
            IND_BO_MONEDA_DOCUMENTO,
            MON_DOC,
            ID_SISORIGEN,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER;


CALL LAMOSALAKE_DEV.PRE.SP_PRE_PFCT_COM_BACKORDER();

CALL LAMOSALAKE_DEV.CON.SP_CON_FCT_REV_BACKORDER();

CALL LAMOSALAKE_DEV.CON.SP_CON_FCT_ADH_BACKORDER();


SELECT 
    STATUSCREDITO_ID,
    STATUS_BLOQUEADO_TOTAL,
    LENGTH(STATUSCREDITO_ID) AS LEN_STATUSCREDITO_ID,
    LENGTH(STATUS_BLOQUEADO_TOTAL) AS LEN_STATUS_BLOQUEADO_TOTAL
FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER
WHERE LENGTH(STATUSCREDITO_ID) > 1 OR LENGTH(STATUS_BLOQUEADO_TOTAL) > 1;


ALTER TABLE LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER
MODIFY COLUMN SEGMENTO_OBRA_ID VARCHAR(20);  -- o el que corresponda

SELECT *
FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER
WHERE 
    UPPER(VKORG) LIKE '%VIVIENDA%' OR
    UPPER(VTWEG) LIKE '%VIVIENDA%' OR
    UPPER(WERKS) LIKE '%VIVIENDA%' OR
    UPPER(VKBUR) LIKE '%VIVIENDA%' OR
    UPPER(BZIRK) LIKE '%VIVIENDA%' OR
    UPPER(VKGRP) LIKE '%VIVIENDA%' OR
    UPPER(COUNTRY) LIKE '%VIVIENDA%' OR
    UPPER(REGION) LIKE '%VIVIENDA%' OR
    UPPER(TRANSPZONE) LIKE '%VIVIENDA%' OR
    UPPER(VBELN) LIKE '%VIVIENDA%' OR
    UPPER(POSNR) LIKE '%VIVIENDA%' OR
    UPPER(AUART) LIKE '%VIVIENDA%' OR
    UPPER(AUGRU) LIKE '%VIVIENDA%' OR
    UPPER(VSBED) LIKE '%VIVIENDA%' OR
    UPPER(INCO1) LIKE '%VIVIENDA%' OR
    UPPER(ABGRU) LIKE '%VIVIENDA%' OR
    UPPER(LIFSK) LIKE '%VIVIENDA%' OR
    UPPER(LIFSP) LIKE '%VIVIENDA%' OR
    UPPER(CMGST) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_ZV) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_ZC) LIKE '%VIVIENDA%' OR
    UPPER(CNSTR_ID) LIKE '%VIVIENDA%' OR
    UPPER(ID_CONVENIO) LIKE '%VIVIENDA%' OR
    UPPER(PLAN_OBRA) LIKE '%VIVIENDA%' OR
    UPPER(SEGMENTO) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_ZP) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_EM) LIKE '%VIVIENDA%' OR
    UPPER(MATNR) LIKE '%VIVIENDA%' OR
    UPPER(CHARG) LIKE '%VIVIENDA%' OR
    UPPER(MTART) LIKE '%VIVIENDA%' OR
    UPPER(VRKME) LIKE '%VIVIENDA%' OR
    UPPER(MONEDA_DOC) LIKE '%VIVIENDA%' OR
    UPPER(SPART) LIKE '%VIVIENDA%' OR
    UPPER(PSTYV) LIKE '%VIVIENDA%' OR
    UPPER(MATKL) LIKE '%VIVIENDA%' OR
    UPPER(BSTNK) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_ZN) LIKE '%VIVIENDA%' OR
    UPPER(STAT) LIKE '%VIVIENDA%' OR
    UPPER(KUNNR_ZQ) LIKE '%VIVIENDA%';



