
CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_COM_PEDIDOS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04
 Creador:            Fidel Ramírez
 Descripción:        SP que transforma datos desde la capa RAW a PRE 
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

        TRUNCATE TABLE PRE.PFCT_COM_PEDIDOS_ACT;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PFCT_COM_PEDIDOS_ACT
        SELECT
            YEAR(FECHA_DOCUMENTO) AS ANIO, 
            LPAD(MONTH(FECHA_DOCUMENTO), 2, '0') AS MES, 
            CONCAT(ANIO, MES) AS ANIOMES,  
            VKORG AS SOCIEDAD_ID,  
            VKORG AS ORGVENTAS_ID, 
            VTWEG AS CANALDISTRIB_ID, 
            '00' AS DIVISION, 
            WERKS AS CENTRO_ID, 
            VKBUR AS OFICINAVENTAS_ID, 
            TRIM(BZIRK) AS ZONAVENTAS_ID,  
            VKGRP AS GRUPOVENDEDORES_ID, 
            CASE 
                WHEN VKORG = 'A101' AND WERKS IN ('A111', 'A119') THEN 'AMT1'
                WHEN VKORG = 'A101' AND WERKS IN ('A112', 'A141', 'A126') THEN 'AMX2'
                WHEN VKORG = 'A101' AND WERKS IN ('A113', 'A114') THEN 'AGD2'
                WHEN VKORG = 'A101' AND WERKS = 'A115' THEN 'ACH1'
                WHEN VKORG = 'A101' AND WERKS = 'A118' THEN 'ACA1'
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
                ELSE ''
            END AS UENADHESIVOS_ID,
            COUNTRY AS PAIS_ID,
            CONCAT( COUNTRY , '_' , REGION ) AS  REGION_ID  ,
            CONCAT( COUNTRY , '_' , TRANSPZONE )  AS ZONATRANSPORTE_ID  ,
            TIPO_DOCUMENTO AS TIPODOCUMENTO  ,
            FECHA_DOCUMENTO AS FECHA_DOCUMENTO ,
            CASE
                WHEN CLASE_DOCUMENTO IN ( 'ZS1' , 'ZS2') THEN FKART
                ELSE CLASE_DOCUMENTO 
            END AS CLASEDOC_CONSOLIDADO  ,
            VBELN  AS DOCUMENTO  ,
            POSNR  AS DOCUMENTO_POS  ,
            PSTYV  AS TIPOPOSICION_ID ,
            AUGRU  AS MOTIVOPEDIDO_ID ,
            INCO1  AS INCOTERMS_ID  ,
            ASESOR_COM AS ASESORPEDIDO_ID ,
            CONSTRUCTORA AS CONSTRUCTORA_ID ,
            CASE
                WHEN PLAN_OBRA = '' THEN 'NA'
                ELSE
                CASE 
                WHEN SUBSTRING( PLAN_OBRA, 0, 2 ) = 'IN' THEN 'IN'
                ELSE 
                    CASE 
                    WHEN SUBSTRING( PLAN_OBRA, 0, 2 ) = 'CO' THEN 'CO'
                    ELSE 'NA' 
                    END
                END
            END AS TIPO_OBRA_ID,
            CONVENIO  AS CONVENIO_OBRA  ,
            PLAN_OBRA  AS PLAN_OBRA  ,
            SEGMENTO  AS SEGMENTO_OBRA_ID  ,
            KUNNR_ZP  AS PROMOTOR_ID ,
            'NUEVO' AS NIO_OBRA, 
            CASE
                WHEN CLASE_DOCUMENTO = 'ZMEX' 
                THEN
                    CASE 
                    WHEN INCO1 IN ( 'ZF0' , 'ZF1' , 'ZF2' , 'ZF3') THEN 'MARITIMO'
                    ELSE 'TERRESTRE'
                    END 
                ELSE 
                    CASE 
                    WHEN CLASE_DOCUMENTO = 'ZEXM' THEN 'MARITIMO'
                    ELSE 'TERRESTRE'
                    END
            END AS TIPOTRANSPORTE_ID  ,
            KUNAG  AS CLIENTE_ID ,
            CONCAT( VKORG , '_' , '00', '_' , VTWEG , '_' , KUNAG ) AS SOLICITANTE_ID ,
            CONCAT( VKORG , '_' , '00' , '_' , VTWEG , '_' , KUNNR) AS DESTINATARIO_ID ,
            MATNR  AS MATERIAL_ID ,
            CONCAT( VKORG , '_' , VTWEG , '_' , MATNR )  AS MATERIALVENTAS_ID  ,
            CONCAT( WERKS , '_' , MATNR ) AS MATERIALCENTRO_ID  ,
            CHARG  AS LOTE  ,
            CASE
                WHEN CENTRO_LOTE = '' THEN WERKS
                ELSE CENTRO_LOTE 
            END AS LOTECENTRO_ID,
            CASE
                WHEN TIPO_DOCUMENTO = 'P' THEN CANT_ESTADISTICA
                ELSE 0 
            END AS IND_CANT_PEDIDO_EST,
            CASE 
                WHEN TIPO_DOCUMENTO = 'P' THEN KBMENG_ESTAD
                ELSE 0
            END AS IND_CANT_CONFIRMADA_EST,
            CASE 
                WHEN TIPO_DOCUMENTO = 'E' THEN CANT_ESTADISTICA 
                ELSE 0
            END AS IND_CANT_ENTREGADA_EST,
            CASE
                WHEN TIPO_DOCUMENTO = 'F' THEN CANT_ESTADISTICA
                ELSE 0
            END AS IND_CANT_FACTURADA_EST,
            COALESCE(U.VALOR_ESTANDARD, T.MSEHI) AS UNI_EST,
            CASE
                WHEN TIPO_DOCUMENTO = 'P' THEN CANT_VENTA
                ELSE 0
            END AS IND_CANT_PEDIDO_UMV,
            CASE 
                WHEN TIPO_DOCUMENTO = 'P' THEN KBMENG
                ELSE 0
            END AS IND_CANT_CONFIRMADA_UMV,
            CASE
                WHEN TIPO_DOCUMENTO = 'E' THEN CANT_VENTA
                ELSE 0
            END AS IND_CANT_ENTREGADA_UMV,
            CASE
                WHEN TIPO_DOCUMENTO = 'F' THEN CANT_VENTA
                ELSE 0
            END AS IND_CANT_FACTURADA_UMV,
            VRKME  AS UNI_UMV ,
            SISORIGEN_ID  AS SISORIGEN_ID  ,
            MANDANTE  AS MANDANTE  ,
            FECHA_CARGA AS FECHA_CARGA ,
            ZONA_HORARIA  AS ZONA_HORARIA  
        FROM 
            RAW.SQ1_MOF_ZBWSD_CAPTURA_DIARIA T
            LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U
                ON UPPER(TRIM(T.MSEHI)) = UPPER(TRIM(U.VALOR_ORIGINAL));

        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_COM_PEDIDOS_ACT;

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
        VALUES('SP_PRE_PFCT_COM_PEDIDOS','PRE.PFCT_COM_PEDIDOS', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;

select * from RAW.CAT_UNIDAD_ESTADISTICA U;