-- Script para la carga de datos de backorder a la tabla de staging y la tabla de producción
CALL PRE.SP_PRE_PFCT_COM_BACKORDER();

CALL CON.SP_CON_FCT_REV_BACKORDER();

CALL CON.SP_CON_FCT_ADH_BACKORDER();

-----------------------------------------------------------------------------
select COUNT(*)from raw.sq1_mof_zbwsd_pedidos_backorder;

SELECT COUNT(*) FROM PRE.PFCT_COM_BACKORDER --1714
WHERE ORGVENTAS_ID NOT LIKE 'R%'
AND ORGVENTAS_ID NOT LIKE 'A%'; -- 429

SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT; --1285

SELECT COUNT(*) FROM CON.FCT_COM_ADH_BACKORDER_ACT; --429

SELECT DISTINCT VKORG FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER
ORDER BY VKORG; -- 4 valores: R101, R311, R401, R501
select * from raw.parametros_extraccion;


SELECT * FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER;
SELECT * FROM PRE.PFCT_COM_BACKORDER;

SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT;
SELECT * FROM CON.FCT_COM_ADH_BACKORDER_ACT;

select * from pre.pfct_com_backorder;

SELECT * FROM LOGS.HISTORIAL_EJECUCIONES limit 20 orde;

select count(*) from con.fct_com_adh_backorder_act;

select * from raw.parametros_extraccion
where extractor = 'ZBWSD_PEDIDOS_BACKORDER';


UPDATE raw.parametros_extraccion
SET R_FIN = R_INICIO
WHERE R_FIN IS NULL
  AND extractor = 'ZBWSD_PEDIDOS_BACKORDER';


CALL PRE.SP_PRE_PFCT_COM_BACKORDER();

CALL CON.SP_CON_FCT_REV_BACKORDER();

SELECT * FROM PRE.PFCT_COM_BACKORDER;

SELECT * FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_PEDIDOS_BACKORDER';

SELECT COUNT(*) FROM PRE.PFCT_COM_BACKORDER;

SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT;

SELECT
    VALOR_NETO, KWMENG, CANT_PEND,
    VALOR_NETO / KWMENG * CANT_PEND AS IND_BO_MONEDA_DOCUMENTO
FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER
WHERE NEGOCIO = 'REV-MEXICO-R311-R401-NA';




CALL PRE.SP_PRE_PFCT_COM_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R101');
CALL PRE.SP_PRE_PFCT_COM_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-NA');
CALL PRE.SP_PRE_PFCT_COM_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-EX');
----
CALL CON.SP_CON_FCT_REV_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R101');
CALL CON.SP_CON_FCT_REV_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-NA');
CALL CON.SP_CON_FCT_REV_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-EX');

CALL CON.SP_CON_FCT_ADH_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-NA');
----------------

CALL CON.SP_CON_FCT_ADH_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','ADH-A000-A201-TO');

truncate table pre.pfct_com_backorder;
TRUNCATE TABLE con.fct_com_rev_backorder_act;
TRUNCATE TABLE con.fct_com_adh_backorder_act;
SELECT * FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER;

SELECT 
  NEGOCIO,
  LENGTH(NEGOCIO),
  '>' || NEGOCIO || '<' AS VISUAL
FROM PRE.PFCT_COM_BACKORDER
WHERE NEGOCIO LIKE '%R311%';



SELECT COUNT(*) AS FILAS_ANTES_DELETE
FROM PRE.PFCT_COM_BACKORDER
WHERE TRIM(UPPER(NEGOCIO)) = TRIM(UPPER('REV-MEXICO-R311-R401-NA'));

RETURN 'Filas antes del DELETE: ' || FILAS_ANTES_DELETE;

DELETE FROM PRE.PFCT_COM_BACKORDER
        WHERE TRIM(UPPER(NEGOCIO)) = TRIM(UPPER('REV-MEXICO-R311-R401-NA'));



select * from raw.sq1_mof_zbwsd_pedidos_backorder;

SELECT COUNT(*) FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER
        WHERE
            VKORG BETWEEN 'R311' AND 'R311' 
            AND VTWEG BETWEEN 'NA' AND 'NA'
            AND VRKME BETWEEN 'M2' AND 'M2';


SELECT count(*) FROM PRE.PFCT_COM_BACKORDER;
select count(*) from con.fct_com_adh_backorder_act;
select count(*) from con.fct_com_rev_backorder_act;
        

SELECT 
    SOCIEDAD_ID,
    CANALDISTRIB_ID,
    UNI_EST,
    COUNT(*) AS TOTAL_REGISTROS
FROM PRE.PFCT_COM_BACKORDER
GROUP BY 
    SOCIEDAD_ID,
    CANALDISTRIB_ID,
    UNI_EST
ORDER BY 
    SOCIEDAD_ID, 
    CANALDISTRIB_ID, 
    UNI_EST;


select count(*) from PRE.PFCT_COM_BACKORDER
    WHERE ORGVENTAS_ID LIKE 'A%';

SELECT count(*) FROM CON.FCT_COM_rev_BACKORDER_ACT;
SELECT count(*) FROM CON.FCT_COM_rev_BACKORDER_foto;
SELECT count(*) FROM CON.FCT_COM_adh_BACKORDER_ACT;
SELECT count(*) FROM CON.FCT_COM_adh_BACKORDER_foto;

 call CON.SP_CON_FCT_COM_VENTAS_HIST();   
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
ORDER BY FECHA_HORA_INICIO DESC limit 20;

------------check unidad de venta
SELECT DISTINCT UNI_EST FROM CON.FCT_COM_ADH_VENTAS_HIST
UNION
SELECT DISTINCT UNI_EST FROM CON.FCT_COM_ADH_VENTAS_ACT
UNION
SELECT DISTINCT UNI_EST FROM CON.FCT_COM_ADH_VENTAS_PCP
UNION
SELECT DISTINCT UNI_EST FROM CON.fct_com_adh_backorder_act
UNION
SELECT DISTINCT UNI_EST FROM CON.fct_com_adh_pedidos_act;

-----------check unidad de venta
truncate table con.fct_com_adh_backorder_act;

SELECT COUNT(*) FROM CON.FCT_COM_ADH_BACKORDER_ACT;
SELECT COUNT(*) FROM raw.sq1_mof_zbwsd_pedidos_backorder;


select distinct UNI_EST from con.fct_com_rev_backorder_act;

SELECT UNI_EST FROM PRE.PFCT_COM_BACKORDER;


SELECT CURRENT_TIMESTAMP() AS FECHA_CARGA;
SELECT 
    CONVERT_TIMEZONE('America/Los_Angeles', 'America/Mexico_City', CURRENT_TIMESTAMP()) AS fecha_actual_mx,
    LAST_DAY(DATEADD(MONTH, -1, CONVERT_TIMEZONE('America/Los_Angeles', 'America/Mexico_City', CURRENT_TIMESTAMP()))) AS fecha_carga;

SHOW PARAMETERS LIKE 'TIMEZONE' IN ACCOUNT;


SELECT * FROM SQ1_EXT_0CUSTOMER_ATTR LIMIT 10;
SELECT * FROM SQ1_EXT_0CUSTOMER_TEXT LIMIT 10;
SELECT * FROM SQ1_EXT_ZSD_GPO_CTES_TEXT;

select * from raw.sq1_ext_zmm_ind_adquisicion;

SELECT count(*) FROM CON.FCT_COM_ADH_BACKORDER_FOTO;

SELECT * FROM CON.FCT_COM_ADH_BACKORDER_FOTO;

SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT;
SELECT * FROM CON.FCT_COM_REV_BACKORDER_FOTO;


TRUNCATE TABLE CON.FCT_COM_REV_BACKORDER_FOTO;


CALL CON.SP_CON_FCT_ADH_BACKORDER_FOTO('ZBWSD_PEDIDOS_BACKORDER','ADH-A000-A201-TO');
CALL CON.SP_CON_FCT_ADH_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','ADH-A000-A201-TO');


CALL CON.SP_CON_FCT_REV_BACKORDER_FOTO('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R101');
SELECT * FROM CON.FCT_COM_REV_BACKORDER_FOTO;
SELECT * FROM CON.FCT_COM_ADH_BACKORDER_FOTO;


SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT;
SELECT MIN(FECHA_CARGA) FROM CON.FCT_COM_REV_BACKORDER_ACT;


SELECT * FROM RAW.SQ1_DELETEDELETE;

DROP TABLE RAW.SQ1_DELETEDELETE;

TRUNCATE TABLE CON.FCT_COM_REV_BACKORDER_FOTO;
TRUNCATE TABLE CON.FCT_COM_REV_BACKORDER_ACT;
SELECT COUNT(*) FROM CON.FCT_COM_REV_BACKORDER_FOTO;
SELECT * FROM CON.FCT_COM_REV_BACKORDER_FOTO;
SELECT COUNT(*) FROM CON.FCT_COM_REV_BACKORDER_ACT; --896
SELECT * FROM CON.FCT_COM_REV_BACKORDER_ACT; 

SELECT *
FROM CON.FCT_COM_REV_BACKORDER_ACT
WHERE TRIM(ASESORPEDIDO_ID) IS NOT NULL
  AND TRIM(ASESORPEDIDO_ID) != ''
LIMIT 10;

SELECT DISTINCT(KUNNR_ZV) FROM RAW.SQ1_MOF_ZBWSD_PEDIDOS_BACKORDER;

SELECT * FROM pre.pfct_com_backorder; --1714 

SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
ORDER BY FECHA_HORA_INICIO DESC;

--------------------------------
TRUNCATE TABLE CON.FCT_COM_REV_BACKORDER_FOTO;
TRUNCATE TABLE CON.FCT_COM_REV_BACKORDER_ACT;
TRUNCATE TABLE CON.FCT_COM_ADH_BACKORDER_FOTO;
TRUNCATE TABLE CON.FCT_COM_ADH_BACKORDER_ACT;
TRUNCATE TABLE PRE.PFCT_COM_BACKORDER;
-----------------------------------

CALL PRE.SP_PRE_PDIM_CLI_CLIENTE();
CALL CON.SP_CON_DIM_CLI_SOLICITANTE();
CALL CON.SP_CON_DIM_CLI_DESTINATARIO();

SELECT COUNT  (*) FROM CON.DIM_CLI_SOLICITANTE;
SELECT COUNT  (*) FROM CON.DIM_CLI_DESTINATARIO;

CALL PRE.SP_PRE_PFCT_COM_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R101-SQ1');
call con.sp_con_fct_rev_backorder('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R101-SQ1');
CALL PRE.SP_PRE_PFCT_COM_BACKORDER('ZBWSD_PEDIDOS_BACKORDER','REV-MEXICO-R311-R401-NA');

SELECT * FROM PRE.PFCT_COM_BACKORDER;

select LAST_DAY(DATEADD(MONTH, -1, CONVERT_TIMEZONE('UTC', 'America/Mexico_City', CURRENT_TIMESTAMP()))) AS FECHA_CARGA;


CALL PRE.SP_PRE_PDIM_CLI_RAMO();
CALL CON.SP_CON_DIM_CLI_RAMO();





















;


select * from CON.DIM_CLI_GRUPOCLIENTES5;
select COUNT(*) from CON.DIM_CLI_GRUPOCLIENTES5;


















select parvw ,count(distinct lifnr) from raw.sq1_tbl_knvp
WHERE parvw IN ('ZJ', 'ZV', 'ZC', 'ZS')
group by parvw
;

SELECT DISTINCT TIENDARECIBO_ID FROM CON.DIM_CLI_TIENDARECIBO;



SELECT * FROM CON.DIM_CLI_CLIENTE
WHERE CLIENTE_ID = '1100049';

SELECT * FROM CON.VW_DIM_CLI_CLIENTE;

SELECT * FROM CON.VW_DIM_CLI_COORDINADORCOMERCIAL;
SELECT * FROM CON.VW_DIM_CLI_ASESORCOMERCIAL;
SELECT * FROM CON.VW_DIM_CLI_EJECUTIVOCIS;
SELECT * FROM CON.VW_DIM_CLI_TIENDARECIBO;


CREATE OR ALTER TABLE DIM_TIEMPO AS
WITH CTE_FECHAS AS (
  SELECT DATEADD(DAY, SEQ4(), '1990-01-01') AS FECHA
  FROM TABLE(GENERATOR(ROWCOUNT => 22280)) -- ~61 años
)
SELECT
  FECHA,
  YEAR(FECHA) AS ANIO,
  MONTH(FECHA) AS MES,
  DAY(FECHA) AS DIA,
  TO_NUMBER(TO_CHAR(FECHA, 'YYYYMM')) AS ANIOMES,
  DAYNAME(FECHA) AS DIASEMANA,
  MONTHNAME(FECHA) AS NOMBRE_MES,
  QUARTER(FECHA) AS TRIMESTRE,
  WEEKOFYEAR(FECHA) AS SEMANA_ANIO,
  CASE WHEN DAYOFWEEK(FECHA) IN (1, 7) THEN FALSE ELSE TRUE END AS ES_LABORAL,
  TO_CHAR(FECHA, 'DD Mon YYYY') AS FECHA_TEXTO
FROM CTE_FECHAS;


CREATE OR ALTER TABLE DIM_TIEMPO AS
WITH CTE_FECHAS AS (
  SELECT DATEADD(DAY, SEQ4(), '1990-01-01') AS FECHA
  FROM TABLE(GENERATOR(ROWCOUNT => 22280))
)
SELECT
  FECHA,
  YEAR(FECHA) AS ANIO,
  MONTH(FECHA) AS MES,
  DAY(FECHA) AS DIA,
  TO_NUMBER(TO_CHAR(FECHA, 'YYYYMM')) AS ANIOMES,
  
  -- Día de la semana en español
  CASE DAYOFWEEK(FECHA)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Lunes'
    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles'
    WHEN 5 THEN 'Jueves'
    WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
  END AS DIASEMANA,
  
  -- Mes en español
  CASE MONTH(FECHA)
    WHEN 1 THEN 'Enero'
    WHEN 2 THEN 'Febrero'
    WHEN 3 THEN 'Marzo'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Mayo'
    WHEN 6 THEN 'Junio'
    WHEN 7 THEN 'Julio'
    WHEN 8 THEN 'Agosto'
    WHEN 9 THEN 'Septiembre'
    WHEN 10 THEN 'Octubre'
    WHEN 11 THEN 'Noviembre'
    WHEN 12 THEN 'Diciembre'
  END AS NOMBRE_MES,

  QUARTER(FECHA) AS TRIMESTRE,
  WEEKOFYEAR(FECHA) AS SEMANA_ANIO,
  CASE WHEN DAYOFWEEK(FECHA) IN (1, 7) THEN FALSE ELSE TRUE END AS ES_LABORAL,
  
  -- Fecha en formato textual en español
  TO_CHAR(DAY(FECHA), '00') || ' ' || 
  CASE MONTH(FECHA)
    WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
    WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
    WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
  END || ' ' || TO_CHAR(YEAR(FECHA)) AS FECHA_TEXTO

FROM CTE_FECHAS;


select * from DIM_TIEMPO;


select * from raw.ventas_hist_codetext;

CALL PRE.SP_PRE_PFCT_COM_VENTAS_HIST();


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
            T.VBELN                                                                   AS PEDIDO,
            T.POSNR                                                                   AS PEDIDO_POS,
            T.AUART                                                                   AS CLASEPEDIDO_ID,
            NULLIF('1990-01-01', '1970-01-01')                                        AS FECHA_DOCUMENTO,  -- FECHA DOCUMENTO NO SE ENCUENTRA EN LA FUENTE AUN
            T.ERDAT                                                                   AS FECHA_CREACION_PEDIDO,
            T.BSTDK                                                                   AS FECHA_PEDIDO_CLIENTE,
            T.VDATU                                                                   AS FECHA_PREFERENTE_ENTREGA,
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
            T.SEGMENTO                                                                AS SEGMENTO_OBRA_ID,
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
            T.MATNR                                                                   AS MATERIAL_ID,
            T.VKORG || '_' || T.VTWEG || '_' || T.MATNR                               AS MATERIALVENTAS_ID,
            T.WERKS || '_' || T.MATNR                                                 AS MATERIALCENTRO_ID,
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
            COALESCE(U.VALOR_ESTANDARD, T.VRKME)                                      AS UNI_EST,
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
            LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U
            ON UPPER(TRIM(T.VRKME)) = UPPER(TRIM(U.VALOR_ORIGINAL))
;

select  * from mirroring.dim_cli_asesorfactura;
select  * from mirroring.sub;