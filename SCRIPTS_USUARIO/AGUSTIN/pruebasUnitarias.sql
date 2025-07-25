----------------- CONTROL EJECUCIONES ----------------
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
WHERE TO_DATE(FECHA_HORA_FINAL) = CURRENT_DATE
AND PROCESO LIKE '%INVENTARIO%' 
ORDER BY FECHA_HORA_FINAL DESC;

CALL CON.SP_CON_FACT_CARTERAHIST('202505');
CALL CON.SP_CON_FACT_CARTERAHIST;


// Conteo por capa
SELECT
    'CON',
    count(*) FROM CON.FCT_FIN_CARTERAHIST WHERE SOCIEDAD_ID = ('R401') AND ANIO_MES = '202505'
UNION ALL 
SELECT
    'PRE',
    count(*) FROM PRE.PFCT_FIN_CARTERAHIST WHERE SOCIEDAD_ID = ('R401') AND  ANIO_MES = '202505'
UNION ALL
SELECT
    'RAW',
    count(*) FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET WHERE BUKRS = ('R401')
AND CONCAT(SUBSTR(VALID_FROM,1,4),SUBSTR(VALID_FROM,6,2)) = '202505';
SELECT DISTINCT VALID_FROM FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET;
CALL CON.SP_CON_FACT_CARTERAHIST('202505');


//  Ver parametros
SELECT EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN, CONCAT(SUBSTR(R_INICIO,1,4),SUBSTR(R_INICIO,5,2)) as ANIOMES
FROM RAW.PARAMETROS_EXTRACCION
WHERE EXTRACTOR = 'ZBWFI_REP_ANTSALDET' AND PARAMETRO = 'BUDAT'; // ACT

UPDATE RAW.PARAMETROS_EXTRACCION
SET  R_INICIO =  '20250531'
WHERE EXTRACTOR = 'ZBWFI_REP_ANTSALDET' AND PARAMETRO = 'BUDAT'
;

// Suma de Indicadores

SELECT DISTINCT VALID_FROM FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET;

SELECT
    WAERS,
    sum(WRBTR) as CARTERA_MDOC,
    sum(SVENCIDO) as VENCIDO_MDOC,
    sum(SPORVENCER) as PORVENCER_MDOC 
FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET
WHERE BUKRS = ('R401')
AND CONCAT(SUBSTR(VALID_FROM,1,4),SUBSTR(VALID_FROM,6,2)) = '202505'
GROUP BY WAERS
ORDER BY WAERS ASC;

SELECT
    sum(IND_IMP_CARTERA_TOTAL_MUSD) as CARTERA_USD,
    sum(IND_IMP_VENCIDO_TOTAL_MUSD) as VENCIDO_USD,
    sum(IND_IMP_PORVENCER_TOTAL_MUSD) as PORVENCER_USD,
    sum(IND_IMP_CARTERA_TOTAL_MSOC) as CARTERA_MSOC,
    sum(IND_IMP_VENCIDO_TOTAL_MSOC) as VENCIDO_MSOC,
    sum(IND_IMP_PORVENCER_TOTAL_MSOC) as PORVENCER_MSOC
FROM CON.FCT_FIN_CARTERAHIST
WHERE SOCIEDAD_ID = ('R401') AND ANIO_MES = '202505'; 

// Columnas por capa

SELECT  RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET; 
SHOW COLUMNS ON  PRE.PFCT_FIN_CARTERAHIST; 
SELECT DISTINCT SOLICITANTE_ID FROM CON.FCT_FIN_CARTERAHIST; 

// Verificacion de nulos y duplicados por negocio

SELECT keys, count(*) as COUNTER FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET
GROUP BY 1
ORDER BY COUNTER DESC;
SELECT * FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET
WHERE key IS NULL;

SELECT ANIO_MES, SOCIEDAD_ID, count(*) as COUNTER FROM CON.FCT_FIN_CARTERAHIST
GROUP BY 1,2
ORDER BY COUNTER DESC;

SELECT *, count(*) as COUNTER FROM CON.FCT_FIN_CARTERAHIST
GROUP BY 1
ORDER BY 2 DESC;
SELECT * FROM PRE.PFCT_COM_PEDIDOS_ACT
WHERE key IS NULL;

SELECT keys, count(*) as COUNTER FROM CON.FCT_COM_ADH_PEDIDOS_ACT
GROUP BY 1
ORDER BY COUNTER DESC;
SELECT * FROM CON.FCT_COM_ADH_PEDIDOS_ACT
WHERE key IS NULL;


// Validacion de fechas fuera de rango y uso correcto de parametros de fechas

SELECT MIN(VALID_FROM) as MIN, MAX(VALID_FROM) as MAX 
FROM RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET;


SELECT DISTINCT FECHACLAVE
FROM CON.FCT_FIN_CARTERAHIST;
----------------------------------------------- OTRO -------------------------------

SELECT DISTINCT ORGVENTAS_ID, SECTOR_ID, CANALDISTRIB_ID, FECHA_DOCUMENTO, UNI_EST FROM CON.FCT_COM_ADH_PEDIDOS_ACT
ORDER BY FECHA_DOCUMENTO,ORGVENTAS_ID,SECTOR_ID,CANALDISTRIB_ID,UNI_EST ASC;

SELECT FECHA_DOCUMENTO FROM CON.FCT_COM_ADH_PEDIDOS_ACT;

SELECT EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN from "RAW"."PARAMETROS_EXTRACCION"
WHERE ORDEN = 1
AND EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA' AND NEGOCIO = 'REVMEXICO';

SELECT * FROM RAW.PARAMETROS_EXTRACCION
WHERE NEGOCIO = 'GENERAL'
AND EXTRACTOR = 'EXTRACTOR_DM'
AND ORDEN = '99';

UPDATE RAW.PARAMETROS_EXTRACCION
SET R_FIN = '20250515'
WHERE NEGOCIO = 'ADHMEXICO'
AND EXTRACTOR = 'ZBWSD_CAPTURA_DIARIA'
AND PARAMETRO = 'FECHA_DOCUMENTO';



--------- Ver ACT y ARCH -----------    
SELECT DISTINCT UNI_EST FROM CON.FCT_COM_REV_PEDIDOS_ACT;
SELECT DISTINCT VKORG FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA;
SELECT * FROM CON.FCT_COM_ADH_PEDIDOS_ACT;
SELECT
    'CON',
    count(*) FROM CON.FCT_COM_ADH_PEDIDOS_ACT
UNION ALL 
SELECT
    'PRE',
    count(*) FROM PRE.PFCT_COM_PEDIDOS_ACT
UNION ALL
SELECT
    'RAW',
    count(*) FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA;

SELECT * FROM PRE.PFCT_FIN_CARTERAHIST ;

-- Indicadores
-- VRKME,
-- sum(CANT_VENTA) as CANT_VENTA,
-- sum(KBMENG) as CantAcConf
-- GROUP BY VRKME
-- ORDER BY VRKME ASC
// RAW
SELECT
sum(CANT_ESTADISTICA) as CANT_ESTAD,
sum(KBMENG_ESTAD) as KBMENG_ESTAD 
FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA
;

SELECT count(*) FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA;

//CON
SELECT 
    CLASEDOC_CONSOLIDADO,
    sum(IND_CANT_CONFIRMADA_M2) as CANT_CONFIRMADA_M2,
  --  sum(IND_CANT_CONFIRMADA_UMV) as CANT_CONFIRMADA_UMV,
    sum(IND_CANT_ENTREGADA_M2) as CANT_ENTREGADA_M2,
   -- sum(IND_CANT_ENTREGADA_UMV) as CANT_ENTREGADA_UMV,
    sum(IND_CANT_FACTURADA_M2) as CANT_FACTURADA_M2,
   -- sum(IND_CANT_FACTURADA_UMV) as CANT_FACTURADA_UMV,
    sum(IND_CANT_PEDIDO_M2) as CANT_PEDIDO_M2,
   -- sum(IND_CANT_PEDIDO_UMV) as CANT_PEDIDO_UMV
FROM CON.FCT_COM_ADH_PEDIDOS_ACT
GROUP BY CLASEDOC_CONSOLIDADO
UNION ALL
SELECT
    'Total',
    sum(IND_CANT_CONFIRMADA_M2) as CANT_CONFIRMADA_M2,
  --  sum(IND_CANT_CONFIRMADA_UMV) as CANT_CONFIRMADA_UMV,
    sum(IND_CANT_ENTREGADA_M2) as CANT_ENTREGADA_M2,
   -- sum(IND_CANT_ENTREGADA_UMV) as CANT_ENTREGADA_UMV,
    sum(IND_CANT_FACTURADA_M2) as CANT_FACTURADA_M2,
   -- sum(IND_CANT_FACTURADA_UMV) as CANT_FACTURADA_UMV,
    sum(IND_CANT_PEDIDO_M2) as CANT_PEDIDO_M2,
   -- sum(IND_CANT_PEDIDO_UMV) as CANT_PEDIDO_UMV
FROM CON.FCT_COM_REV_PEDIDOS_ACT
ORDER BY CLASEDOC_CONSOLIDADO ASC
;

SELECT 
    *
FROM CON.FCT_COM_ADH_PEDIDOS_ACT
WHERE MES = '04' AND CLIENTE_ID = '0010100024';

-- Insercion de Parametros --

INSERT INTO RAW.PARAMETROS_EXTRACCION (ORDEN, EXTRACTOR, NEGOCIO, PARAMETRO, R_INICIO, R_FIN)
VALUES
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'PDTES', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'SPART', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'VKORG', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'VRKME', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'VTWEG', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'AUART', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'ERDAT', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'KUNNR', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'MATKL', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'VBELN', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'VKBUR', NULL, NULL),
    (1, 'ZBWSD_CAPTURA_DIARIA', '', 'NEGOCIO', NULL, NULL),
;
-- DOCUMENTO --
SHOW COLUMNS ON RAW.SQ1_EXT_0BILL_TYPE_TEXT;
SELECT * FROM CON.DIM_DOC_CLASEFACTURA
ORDER BY CLASEFACTURA_ID ASC;
SELECT * FROM CON.VW_DIM_DOC_CLASEFACTURA
ORDER BY CLASEFACTURA_ID ASC;
SELECT FKART, SPRAS, count(*) as COUNTER FROM RAW.SQ1_EXT_0BILL_TYPE_TEXT
GROUP BY FKART, SPRAS
ORDER BY COUNTER DESC;
SELECT DISTINCT SPRAS FROM RAW.SQ1_EXT_0BILL_TYPE_TEXT;
--
SHOW COLUMNS ON RAW.SQ1_EXT_0SCM_TM_SHIPTYPE;
SELECT * FROM PRE.PDIM_DOC_CONDICIONEXP
ORDER BY CONDICIONEXP_ID;
SELECT SHIPPING_TYPE FROM RAW.SQ1_EXT_0SCM_TM_SHIPTYPE
WHERE SHIPPING_TYPE IS NULL;
-- TIPOPOSICION
SELECT * FROM PRE.PDIM_DOC_TIPOPOSICION;
SELECT PSTYV, SPRAS, count(*) as COUNTER FROM RAW.SQ1_EXT_0ITEM_CATEG_TEXT
GROUP BY PSTYV, SPRAS
ORDER BY COUNTER DESC;
SELECT * FROM CON.VW_DIM_DOC_TIPOPOSICION
ORDER BY TIPOPOSICION_ID ASC;
-- INCOTERMS
SHOW COLUMNS ON CON.DIM_DOC_INCOTERMS;
SELECT INCO1 FROM RAW.SQ1_EXT_0INCOTERMS_TEXT
WHERE INCO1 IS NULL;
SELECT * FROM PRE.PDIM_DOC_INCOTERMS;
-- MOTIVOPEDIDO
SELECT * FROM PRE.PDIM_DOC_MOTIVOPEDIDO;
SELECT * FROM CON.VW_DIM_DOC_MOTIVOPEDIDO
ORDER BY MOTIVOPEDIDO_ID ASC;
SELECT AUGRU FROM RAW.SQ1_EXT_0ORD_REASON_TEXT
WHERE AUGRU IS NULL;
-- CLASEPEDIDO
SELECT * FROM PRE.PDIM_DOC_CLASEPEDIDO;
SELECT AUART FROM RAW.SQ1_EXT_0DOC_TYPE_TEXT
WHERE AUART IS NULL;
SELECT * FROM CON.VW_DIM_DOC_CLASEPEDIDO
ORDER BY CLASEPEDIDO_ID ASC;

SELECT 
    CLIENTE_ID,
    sum(IND_CANT_PEDIDO_M2) as PEDIDO,
    sum(IND_CANT_ENTREGADA_M2) as ENTERGADA,
    sum(IND_CANT_FACTURADA_M2) as FACTURADA,
    sum(IND_CANT_CONFIRMADA_M2) as CONFIRMADA
FROM CON.FCT_COM_REV_PEDIDOS_ACT
WHERE MES = '04' AND ORGVENTAS_ID = 'R313'
GROUP BY CLIENTE_ID
ORDER BY CLIENTE_ID ASC;

SELECT FECHA_CARGA FROM  RAW.SQ1_EXT_0BPARTNER_TEXT;
WHERE KUNNR = '0010102973';

-------------------------------------------------------------------------------------------------------

-- Validacion de columnas faltantes, adicionales o renombradas REV
SHOW COLUMNS ON RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA;
SHOW COLUMNS ON PRE.PFCT_COM_PEDIDOS_ACT;
SHOW COLUMNS ON CON.FCT_COM_ADH_PEDIDOS_ACT;

-- Verificacion de nulos y duplicados por negocio REV

SELECT
    *
FROM CON.FCT_COM_ADH_PEDIDOS_ACT
WHERE ORGVENTAS_ID IS NULL 
OR   CANALDISTRIB_ID IS NULL
OR    SECTOR_ID IS NULL
OR    TIPODOCUMENTO IS NULL
OR    FECHA_DOCUMENTO IS NULL
OR    CLASEDOC_CONSOLIDADO IS NULL
OR    DOCUMENTO IS NULL
OR    DOCUMENTO_POS IS NULL;

SELECT 
    CANALDISTRIB_ID,
    SECTOR_ID,
    ORGVENTAS_ID,
    TIPODOCUMENTO,
    FECHA_DOCUMENTO,
    CLASEDOC_CONSOLIDADO,
    DOCUMENTO,
    DOCUMENTO_POS,
    count(*) as COUNTER
FROM CON.FCT_COM_ADH_PEDIDOS_ACT
GROUP BY CANALDISTRIB_ID,
    SECTOR_ID,
    ORGVENTAS_ID,
    TIPODOCUMENTO,
    FECHA_DOCUMENTO,
    CLASEDOC_CONSOLIDADO,
    DOCUMENTO,
    DOCUMENTO_POS
ORDER BY COUNTER DESC;

-- Validacion de transformaciones de campos entre capas PRE - CON - REV 
SELECT * FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA;
SELECT * FROM PRE.FCT_COM_REV_PEDIDOS_ACT;
SELECT * FROM CON.VW_FCT_COM_REV_PEDIDOS;

-- Validacion de claves primarias (PEDIDO, PEDIDO_POS)
SELECT DOCUMENTO, DOCUMENTO_POS, count(*) AS COUNTER FROM CON.FCT_COM_REV_PEDIDOS_ACT
GROUP BY DOCUMENTO, DOCUMENTO_POS
ORDER BY COUNTER DESC;
SELECT * FROM CON.FCT_COM_ADH_PEDIDOS_ACT
WHERE DOCUMENTO IS NULL OR DOCUMENTO_POS IS NULL OR SECTOR_ID IS NULL OR ORGVENTAS_ID IS NULL OR TIPODOCUMENTO IS NULL
OR FECHA_DOCUMENTO IS NULL OR CLASEDOC_CONSOLIDADO IS NULL;


---------------------------------------------------------------

SELECT
    UNI_EST,
    sum(IND_CANT_PEDIDO_M2) as PEDIDO,
    sum(IND_CANT_ENTREGADA_M2) as ENTREGADA,
    sum(IND_CANT_FACTURADA_M2) as FACTURADA
FROM CON.FCT_COM_REV_PEDIDOS_ACT
WHERE MES = '04'
GROUP BY 1
ORDER BY 1 ASC;

SELECT count(DOCUMENTO) FROM CON.FCT_COM_REV_PEDIDOS_ARCH;
SELECT DISTINCT DOCUMENTO, DOCUMENTO_POS FROM CON.FCT_COM_REV_PEDIDOS_ARCH
ORDER BY 1,2 DESC;

DELETE FROM RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA
WHERE
VBELN IN ('1000000038', '1000000037')
AND POSNR = '000010';

CALL CON.SP_CON_FCT_COM_REV_PEDIDOS_ARCH();

SELECT DISTINCT SOCIEDAD_ID FROM CON.FCT_COM_ADH_PEDIDOS_ACT;

INSERT INTO CON.FCT_COM_REV_PEDIDOS_ARCH (DOCUMENTO)
VALUES
('91826');
SELECT
    MIN(FECHA_DOCUMENTO) as MIN,
    MAX(FECHA_DOCUMENTO) as MAX
FROM CON.FCT_COM_ADH_PEDIDOS_ACT;

SELECT DISTINCT ORGVENTAS_ID FROM PRE.PFCT_COM_PEDIDOS_ACT;

SELECT DISTINCT DOCUMENTO_POS FROM PRE.PFCT_COM_PEDIDOS_ACT
MINUS
SELECT DISTINCT DOCUMENTO_POS FROM CON.FCT_COM_REV_PEDIDOS_ACT
-- MINUS
-- SELECT DISTINCT PEDIDO FROM CON.FCT_COM_ADH_BACKORDER_ACT
;




CALL CON.SP_CON_FCT_COM_ADH_PEDIDOS_ARCH();

SELECT * FROM PRE.PFCT_COM_PEDIDOS_ACT
WHERE DOCUMENTO = '1000000050' AND DOCUMENTO_POS = '000020'
;

DELETE FROM PRE.PFCT_COM_PEDIDOS_ACT
WHERE DOCUMENTO = '1000000050' AND DOCUMENTO_POS = '000020';

SELECT * FROM CON.FCT_COM_ADH_PEDIDOS_ARCH
WHERE DOCUMENTO = '5004000500' OR DOCUMENTO = '1000000050' AND DOCUMENTO_POS = '000020'
;


INSERT INTO CON.FCT_COM_ADH_PEDIDOS_ARCH (FECHA_DOCUMENTO, DOCUMENTO, DOCUMENTO_POS)
VALUES
    ('2024-04-22', '5004000500', '000056');

TRUNCATE TABLE RAW.SQ1_EXT_ZBWFI_REP_ANTSALDET;
TRUNCATE TABLE PRE.PFCT_FIN_CARTERAACT;
TRUNCATE TABLE CON.FCT_FIN_CARTERAACT;