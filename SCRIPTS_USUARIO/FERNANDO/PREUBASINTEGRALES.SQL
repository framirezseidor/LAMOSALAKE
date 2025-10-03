DELETE FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CUADERNO_FINANCIERO'
  AND NEGOCIO = 'ADH_MEXICO';

SELECT * FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CUADERNO_FINANCIERO'
AND NEGOCIO = 'ADH_MEXICO';

truncate table con.fct_com_adh_ventas_act;
truncate table con.fct_com_adh_backorder_act;
-- truncate table con.fct_com_adh_backorder_foto;
truncate table con.fct_com_adh_pedidos_act;
-- truncate table con.fct_com_adh_pedidos_arch;


-- -- NO. DE CASO: 0
-- INSERT INTO RAW.PARAMETROS_EXTRACCION (
--     ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN
-- )
-- VALUES
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'MSEHI', 'TO', NULL),
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'SPART', '00', '80'),
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VKORG', 'A101', 'A104'),
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'WAERK', 'MXN', NULL),
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'FKDAT', '20250101', '20250330'),
-- (1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VTWEG', 'EX', 'NA'); --1376

-- NO. DE CASO: 1
INSERT INTO RAW.PARAMETROS_EXTRACCION (
    ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN
)
VALUES
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'MSEHI', 'TO', NULL),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'SPART', '00', '80'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VKORG', 'A101', 'A104'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'WAERK', 'MXN', NULL),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'FKDAT', '20250101', '20250918'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VTWEG', 'EX', 'NA'); --1376

-- NO. DE CASO: 2
INSERT INTO RAW.PARAMETROS_EXTRACCION (ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN) VALUES
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'FKDAT', '20250101', '20250729'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'MSEHI', 'TO', NULL),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'SPART', '00', '80'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VKORG', 'A101', 'A104'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VTWEG', 'EX', 'NA'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'WAERK', 'MXN', NULL);

-- NO. DE CASO: 3
INSERT INTO RAW.PARAMETROS_EXTRACCION (ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN) VALUES --386
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'FKDAT', '20250101', '20250531'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'MSEHI', 'TO', NULL),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'SPART', '00', '80'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VKORG', 'A201', 'A201'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VTWEG', 'NA', 'NA'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'WAERK', 'GTQ', NULL);

--NO. DE CASO: 4
INSERT INTO RAW.PARAMETROS_EXTRACCION (ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN) VALUES --483
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'FKDAT', '20250101', '20250731'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'MSEHI', 'TO', NULL),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'SPART', '00', '80'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VKORG', 'A201', 'A201'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'VTWEG', 'NA', 'NA'),
(1, 'ZBWSD_CUADERNO_FINANCIERO', 'ADH_MEXICO', 'WAERK', 'GTQ', NULL);

UPDATE RAW.PARAMETROS_EXTRACCION
SET R_INICIO = '20250101',
    R_FIN = '20250731'
WHERE EXTRACTOR = 'ZBWSD_CUADERNO_FINANCIERO'
  AND NEGOCIO = 'ADH_MEXICO'
  AND PARAMETRO = 'FKDAT';

-------------------


--NO. DE CASO: 13 PEDIDOS
INSERT INTO RAW.PARAMETROS_EXTRACCION (ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN) VALUES
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VKORG', 'A101', 'A104'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'SPART', '00', '80'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VTWEG', 'EX', 'NA'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'FECHA_DOCUMENTO', '20250501', '20250731'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'MSEHI', 'TO', 'TO'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'KUNAG', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'TIPO_DOCUMENTO', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VBELN', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VKBUR', NULL, NULL);


DELETE FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA'
  AND NEGOCIO = 'ADHMEXICO';
  
  SELECT * FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA'
  AND NEGOCIO = 'ADHMEXICO';

  SELECT count(*) FROM MIRRORING.FCT_COM_ADH_COMERCIAL
  WHERE MODELO = 'PEDIDOS';

SELECT count(*), modelo FROM MIRRORING.FCT_COM_ADH_COMERCIAL
GROUP BY modelo; --37453929

SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
ORDER BY FECHA_HORA_INICIO DESC limit 20;


select count(*) from mirroring.fct_com_adh_comercial
where modelo = 'VENTAS' and sisorigen_id = 'SQ1';

select sum(vta_ind_venta_real_ton) as total_toneladas
from mirroring.fct_com_adh_comercial COM
join mirroring.dim_doc_clasefactura CF on COM.vta_clasefactura_id = CF.clasefactura_id
where UENADHESIVOS_ID in ('AGD2','AMT1','AMX2','ACA1','ACH1')
and aniomes = '202507'
and CF.clasefactura_id not in ('ZMUE','ZRB4');

select vta_clasefactura_id from mirroring.fct_com_adh_comercial limit;
select * from mirroring.dim_doc_clasefactura;
--where modelo = 'VENTAS' and sisorigen_id = 'SQ1'; --3547.001


select * from mirroring.fct_com_adh_comercial
where vta_factura = '9100000121' and 
sisorigen_id = 'SQ1';

select asesorfactura_id, * from con.fct_com_adh_ventas_pcp
join con.dim_cli_asesorcomercial on con.fct_com_adh_ventas_pcp.asesorfactura_id = con.dim_cli_asesorcomercial.ASESORCOMERCIAL_ID;

-- Script para ver los clientes que son asesores comerciales
select distinct asesorfactura_id, cliente_text from con.fct_com_adh_ventas_pcp
join con.dim_cli_cliente on con.fct_com_adh_ventas_pcp.asesorfactura_id = con.DIM_CLI_CLIENTE.CLIENTE_ID;

select anio, count(*) from con.fct_com_adh_ventas_pcp
join con.dim_cli_cliente on con.fct_com_adh_ventas_pcp.asesorfactura_id = con.DIM_CLI_CLIENTE.CLIENTE_ID
group by con.fct_com_adh_ventas_pcp.anio;

select sum(ind_venta_real_ton) from con.fct_com_adh_ventas_act

;
select * from con.fct_com_adh_ventas_act
where aniomes = '202507';


select count(*) from con.fct_com_adh_ventas_act
where sisorigen_id = 'SQ1' and orgventas_id = 'A201';

select count(*) from mirroring.fct_com_adh_comercial
where sisorigen_id = 'SQ1' and orgventas_id = 'A201';

select COUNT(*) from raw.sq1_mof_zbwsd_cuaderno_financiero;

select * from LAMOSALAKE_DEV.MIRRORING.TR_FCT_COM_ADH_VENTAS_ACT;


select sociedad_id, factura, factura_pos ,count(*) from con.fct_com_adh_ventas_act
where sisorigen_id = 'SQ1' and orgventas_id = 'A201'
group by sociedad_id, factura, factura_pos
having count(*) > 1;

select sociedad_id, vta_factura, vta_factura_pos ,count(*) from mirroring.fct_com_adh_comercial
where sisorigen_id = 'SQ1' and orgventas_id = 'A201'
group by sociedad_id, vta_factura, vta_factura_pos
having count(*) > 1;

SELECT * FROM CON.FCT_COM_ADH_VENTAS_ACT
LEFT JOIN MIRRORING.FCT_COM_ADH_COMERCIAL ON 
    CON.FCT_COM_ADH_VENTAS_ACT.SOCIEDAD_ID = MIRRORING.FCT_COM_ADH_COMERCIAL.SOCIEDAD_ID AND 
    CON.FCT_COM_ADH_VENTAS_ACT.FACTURA = MIRRORING.FCT_COM_ADH_COMERCIAL.VTA_FACTURA AND 
    CON.FCT_COM_ADH_VENTAS_ACT.FACTURA_POS = MIRRORING.FCT_COM_ADH_COMERCIAL.VTA_FACTURA_POS AND
    CON.FCT_COM_ADH_VENTAS_ACT.ANIOMES = MIRRORING.FCT_COM_ADH_COMERCIAL.ANIOMES
WHERE 
    CON.FCT_COM_ADH_VENTAS_ACT.sisorigen_id = 'SQ1' AND
    CON.FCT_COM_ADH_VENTAS_ACT.orgventas_id = 'A201' ;

    
select * from con.fct_com_adh_ventas_act
where sisorigen_id = 'SQ1' and orgventas_id = 'A201' AND FACTURA = '9160000179';
    
    
----------------- BACKORDER -----------------

Select count(*) from con.fct_com_adh_backorder_act;
select count(*) from con.fct_com_adh_backorder_foto;

select count(*) from mirroring.fct_com_adh_comercial
where sisorigen_id = 'SQ1' and modelo = 'BACKORDER';


SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
ORDER BY FECHA_HORA_INICIO DESC limit 200;

SELECT DISTINCT VSBED FROM RAW.SQ1_MOF_ZBWSD_CUADERNO_FINANCIERO;

SELECT * FROM CON.DIM_DOC_CONDICIONEXP;

SELECT material_id FROM mirroring.fct_com_adh_comercial
join con.dim_mat_material on mirroring.fct_com_adh_comercial.material_id = con.dim_mat_material.material_id;


SELECT 
DISTINCT MATERIAL_ID FROM MIRRORING.FCT_COM_ADH_COMERCIAL 
WHERE MATERIAL_ID NOT IN (SELECT MATERIAL_ID FROM MIRRORING.DIM_MAT_MATERIAL_ADH)
AND SISORIGEN_ID = 'SQ1';

SELECT 
DISTINCT MATERIALCENTRO_ID, modelo FROM MIRRORING.FCT_COM_ADH_COMERCIAL 
WHERE MATERIALCENTRO_ID NOT IN (SELECT MATERIALCENTRO_ID FROM MIRRORING.DIM_MAT_MATERIALCENTRO_ADH)
AND SISORIGEN_ID = 'SQ1';

SELECT MATERIALCENTRO_ID, centro_id, modelo, * FROM mirroring.fct_com_adh_comercial
WHERE MATERIALCENTRO_ID NOT IN (SELECT MATERIALCENTRO_ID FROM MIRRORING.DIM_MAT_MATERIALCENTRO_ADH)
and SISORIGEN_ID = 'SQ1';   

/////aqui aqui aqui auqi auqi
SELECT DISTINCT DESTINATARIO_ID FROM mirroring.fct_com_adh_comercial
WHERE DESTINATARIO_ID NOT IN (SELECT DESTINATARIO_ID FROM MIRRORING.DIM_CLI_DESTINATARIO)
and SISORIGEN_ID = 'SQ1';   

SELECT DISTINCT SOLICITANTE_ID FROM mirroring.fct_com_adh_comercial
WHERE SOLICITANTE_ID NOT IN (SELECT SOLICITANTE_ID FROM MIRRORING.DIM_CLI_SOLICITANTE)
and SISORIGEN_ID = 'SQ1';   

SELECT MATERIAL_ID, VTA_IND_VENTA_REAL_TON, BOR_IND_BO_TOTAL_TON, PED_IND_CANT_PEDIDO_TON, MODELO FROM mirroring.fct_com_adh_comercial
WHERE MATERIAL_ID IN (SELECT 
DISTINCT MATERIAL_ID FROM MIRRORING.FCT_COM_ADH_COMERCIAL 
WHERE MATERIAL_ID NOT IN (SELECT MATERIAL_ID FROM MIRRORING.DIM_MAT_MATERIAL)
AND SISORIGEN_ID = 'SQ1')
AND SISORIGEN_ID = 'SQ1';

select * from raw.parametros_extraccion
WHERE extractor = 'EXTRACTOR_DM';

UPDATE RAW.PARAMETROS_EXTRACCION
SET R_INICIO = '400'
WHERE EXTRACTOR = 'EXTRACTOR_DM'
  AND NEGOCIO = 'GENERAL'
  AND PARAMETRO = 'MANDANTE'
  AND R_INICIO = '500';


SELECT uenadhesivos_id, * FROM mirroring.fct_com_adh_comercial
WHERE uenadhesivos_id IS NULL
AND SISORIGEN_ID = 'SQ1';


SELECT 
DISTINCT sociedad_id, centro_id, UENADHESIVOS_ID FROM MIRRORING.FCT_COM_ADH_COMERCIAL 
WHERE UENADHESIVOS_ID NOT IN (SELECT UENADHESIVOS_ID FROM MIRRORING.DIM_ORG_UENADHESIVOS)
AND SISORIGEN_ID = 'SQ1';


select MATERIAL_ID, VTA_CLASEFACTURA_ID, ofici from mirroring.fct_com_adh_comercial
where material_id like 'P%'
AND SISORIGEN_ID = 'SQ1';


SELECT MATERIAL_ID FROM MIRRORING.DIM_MAT_MATERIAL
WHERE MATERIAL_ID = '3050-04GR-PZ01';

"MATERIAL_ID"
"3050-04GR-PZ01"
"3170-0442-PZ01"
"2140-0541-PZ01"
"3170-0425-PZ01"
"3170-0353-PZ01"
"2140-0267-PZ01"
"000000000002001720"
"000000000002001397"
"3050-05GR-PZ01"
"2140-0542-PZ01"

;

  SELECT * FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA';

DELETE FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA'
and NEGOCIO = 'ADHMEXICO';

INSERT INTO RAW.PARAMETROS_EXTRACCION (
    ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN
)
VALUES
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VKORG', 'A101', 'A201'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'SPART', '00', '80'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VTWEG', 'EX', 'NA'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'FECHA_DOCUMENTO', '20250101', '20250731'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'MSEHI', 'TO', 'TO'),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'KUNAG', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'TIPO_DOCUMENTO', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VBELN', NULL, NULL),
(2, 'ZBWSD_CAPTURA_DIARIA', 'ADHMEXICO', 'VKBUR', NULL, NULL);



SELECT * FROM MIRRORING.DIM_MAT_MATERIAL_ADH
WHERE MATERIAL_ID = '3170-0442-PZ01';


SELECT 
    DISTINCT
    ATTR.MATNR,
    TEXT.MATNR AS MATERIAL_DESC,
    OMAT.*
FROM RAW.SQ1_EXT_0MATERIAL_ATTR AS ATTR
LEFT JOIN RAW.SQ1_EXT_0MATERIAL_TEXT AS TEXT 
    ON ATTR.MATNR = TEXT.MATNR
LEFT JOIN RAW.SQ1_EXT_1CL_OMAT001 AS OMAT 
    ON ATTR.MATNR = OMAT.MATNR
WHERE ATTR.MATNR IN (
    '3170-0442-PZ01',
    '3170-0353-PZ01',
    '3170-0425-PZ01',
    '3050-04GR-PZ01',
    '2140-0267-PZ01',
    '2140-0541-PZ01',
    '3050-05GR-PZ01',
    '2140-0542-PZ01'
);


SELECT distinct sociedad_id FROM CON.FCT_COM_ADH_VENTAS_ACT;


SELECT * FROM MIRRORING.FCT_COM_ADH_COMERCIAL
WHERE DESTINATARIO_ID = ''
AND SISORIGEN_ID = 'SQ1';

SELECT * FROM RAW.FILE_XLS_COM_ADH_VENTAS_PCP;

select * from raw.parametros_extraccion
where extractor = 'EXTRACTOR_DM';




-- SQ1_TBL_KNVP.SQL
-- SQ1_EXT_0CUST_CLASS_TEXT.SQL
-- SQ1_EXT_0CUSTOMER_ATTR.SQL
-- SQ1_EXT_0CUST_SALES_ATTR.SQL

-- SQ1_EXT_0CUST_SALES_TEXT.SQL
-- SQ1_EXT_0CUST_GROUP_TEXT.SQL
-- SQ1_EXT_0CUST_GRP1_TEXT.SQL
-- SQ1_EXT_0CUST_GRP2_TEXT.SQL
-- SQ1_EXT_0CUST_GRP3_TEXT.SQL
-- SQ1_EXT_0CUST_GRP4_TEXT.SQL
-- SQ1_EXT_0CUST_GRP5_TEXT.SQL
-- SQ1_EXT_0ACCNT_ASGN_TEXT.SQL
-- SQ1_EXT_ZSD_GPO_CTES_TEXT.SQL
-- SQ1_EXT_0INDUSTRY_TEXT.SQL

select distinct lineamaterial_adh_id from mirroring.DIM_MAT_MATERIAL_ADH ;

SELECT * FROM RAW.SQ1_EXT_1CL_OMAT001 AS OMAT;

-- drop table RAW.SQ1_EXT_1CL_OMAT001;


select sum(vta_ind_venta_pcp_ton), anio, aniomes
from mirroring.fct_com_adh_comercial COM
join mirroring.dim_doc_clasefactura CF on COM.vta_clasefactura_id = CF.clasefactura_id
where UENADHESIVOS_ID in ('AGD2','AMT1','AMX2','ACA1','ACH1')
and CF.clasefactura_id not in ('ZMUE','ZRB4')
group by anio, aniomes;

select vta_ind_venta_real_ton, *  
from mirroring.fct_com_adh_comercial COM
join mirroring.dim_doc_clasefactura CF on COM.vta_clasefactura_id = CF.clasefactura_id
where anio > '20';


SET FEATURE = 'AMT_PERC_ACTIVE_WEEKS';
--SET MONTHS  = 6;
 
WITH activos AS (
  SELECT DISTINCT ext_merchant_id
  FROM PROJECT_FEATURES_DEV.FEATURES_DEV.TABLE_VALIDATION
),
 
/* Vista Snowflake: calcula el feature desde el stage mensual */
vista_f AS (
  SELECT
    v.ext_merchant_id,
    IDENTIFIER($FEATURE) AS feature_val
  FROM (
    SELECT
      ext_merchant_id,
      DIV0((COUNT_IF(COALESCE(TXN_AMOUNT, 0) <> 0))/(FLOOR(DATEDIFF(DAY, MIN(WEEK_END_DATE), MAX(WEEK_END_DATE)) / 7))) AS AMT_PERC_ACTIVE_WEEKS
    FROM PROJECT_FEATURES_DEV.FEATURES_HAULMER_CL.STG_RESAMPLE_WEEKLY
    --WHERE MONTHS_WINDOW = $MONTHS
    GROUP BY ext_merchant_id
  ) v
  JOIN activos a ON v.ext_merchant_id = a.ext_merchant_id
),
 
/* Tabla Python: toma la columna dinámica indicada por $FEATURE */
tabla_f AS (
  SELECT
    t.ext_merchant_id,
    IDENTIFIER($FEATURE) AS feature_val
  FROM PROJECT_FEATURES_DEV.FEATURES_DEV.TABLE_VALIDATION_PYTHON t
  JOIN activos a ON t.ext_merchant_id = a.ext_merchant_id
)
 
SELECT
  v.ext_merchant_id,
  v.feature_val AS vista_snowflake,
  t.feature_val AS tabla_python,
 
  ABS(v.feature_val - t.feature_val) AS abs_diff,
 
  CASE
    WHEN v.feature_val = 0 THEN NULL
    ELSE ABS(v.feature_val - t.feature_val) / ABS(v.feature_val)
  END AS perc_diff,
 
  CASE
    WHEN v.feature_val = 0 THEN 'N/A'
    WHEN ABS(v.feature_val - t.feature_val) / ABS(v.feature_val) <= 0.0001 THEN 'Menor al 0.001%'
    WHEN ABS(v.feature_val - t.feature_val) / ABS(v.feature_val) <= 0.001  THEN 'Menor al 0.01%'
    WHEN ABS(v.feature_val - t.feature_val) / ABS(v.feature_val) <= 0.01   THEN 'Menor al 1%'
    WHEN ABS(v.feature_val - t.feature_val) / ABS(v.feature_val) <= 0.1    THEN 'Menor al 10%'
    ELSE 'Mayor al 10%'
  END AS error_size,
 
  IFF(v.feature_val = t.feature_val, 'OK', 'DIFERENTE') AS comparacion
 
FROM vista_f v
JOIN tabla_f t
  ON v.ext_merchant_id = t.ext_merchant_id
WHERE v.feature_val <> t.feature_val
ORDER BY v.ext_merchant_id;