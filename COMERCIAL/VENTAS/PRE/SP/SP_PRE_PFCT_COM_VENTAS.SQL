CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_COM_VENTAS(
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
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Agustin Gutierrez/Fidel Ramírez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_COM_VENTAS
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);
    PARAMETRO       VARCHAR(20);
    SOCIEDAD_INICIO   VARCHAR(20);
    SOCIEDAD_FIN      VARCHAR(20);
    PREFIJO         VARCHAR(5);
    TABLA           VARCHAR(500);

BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE SOCIEDADES
    ---------------------------------------------------------------------------------
    PARAMETRO := 'VKORG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :SOCIEDAD_INICIO ;

    SOCIEDAD_FIN := SPLIT_PART(SOCIEDAD_INICIO,'-',2);
    SOCIEDAD_INICIO := SPLIT_PART(SOCIEDAD_INICIO,'-',1);    

    SELECT RAW.FN_BUSCA_PARAMETRO( 'EXTRACTOR_TX' , 'GENERAL' , 'PREFIJO') INTO :PREFIJO ;
    PREFIJO := SPLIT_PART(PREFIJO,'-',1);    
    TABLA := 'RAW.' || :PREFIJO || 'MOF_ZBWSD_CUADERNO_FINANCIERO';
    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM 
            PRE.PFCT_COM_VENTAS
        WHERE
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  ;

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

        INSERT INTO PRE.PFCT_COM_VENTAS
        (   
            ANIO ,  
            MES ,  
            ANIOMES ,  
            SOCIEDAD_ID ,  
            ORGVENTAS_ID ,  
            CANALDISTRIB_ID ,  
            SECTOR_ID ,  
            CENTRO_ID ,  
            ALMACENCENTRO_ID ,  
            ALMACEN_ID ,  
            OFICINAVENTAS_ID ,  
            ZONAVENTAS_ID ,  
            GRUPOVENDEDORES_ID ,  
            UENADHESIVOS_ID ,  
            PAIS_ID ,  
            REGION_ID ,  
            ZONATRANSPORTE_ID ,  
            TIPO_TRANSACCION ,  
            FACTURA ,  
            FACTURA_POS ,  
            FECHA_FACTURA ,  
            CLASEFACTURA_ID ,  
            CLASEFACTURA_ORIGEN_ID ,  
            CLASEFACTURA_ASOCIADO_ID ,  
            INDICADOR_ANULACION ,  
            STATUSCONTABILIDAD ,  
            CONDICIONEXP_ID ,  
            INCOTERMS_ID ,  
            ASESORFACTURA_ID ,  
            TIPOPOSICION_ID ,  
            PEDIDO ,  
            PEDIDO_POS ,  
            ORDENCOMPRA ,  
            MOTIVOPEDIDO_ID ,  
            ASESORPEDIDO_ID ,  
            TIENDARECIBO_ID ,  
            CONSTRUCTORA_ID ,  
            TIPO_OBRA_ID ,  
            CONVENIO_OBRA ,  
            PLAN_OBRA ,  
            SEGMENTO_OBRA_ID ,  
            PROMOTOR_ID ,  
            NIO_OBRA ,  
            NUM_TRANSPORTE ,  
            CLASETRANSPORTE_ID ,  
            TIPOTRANSPORTE_ID ,  
            CLASEEXPEDICION_ID ,  
            PUESTOPLANTRANSP_ID ,  
            TRANSPORTISTA_ID ,  
            RUTA_ID ,  
            TIPOVEHICULO_ID ,  
            CHOFER ,  
            FECHA_DESPACHOEXPREAL ,  
            CLIENTE_ID ,  
            SOLICITANTE_ID ,  
            DESTINATARIO_ID ,  
            DEUDOR_ID ,  
            MATERIAL_ID ,  
            MATERIALVENTAS_ID ,  
            MATERIALCENTRO_ID ,  
            LOTE ,  
            LOTECENTRO_ID ,  
            GRUPOARTICULOS_ID ,  
            TIPOMATERIAL_ID ,  
            IND_VENTA_REAL_EST ,  
            UNI_EST ,  
            IND_VENTA_REAL_UMV ,  
            UNI_UMV ,  
            IND_VENTA_PRECIOLISTA_LOC ,  
            IND_VENTA_PRECIOTEORICO_LOC ,  
            IND_VENTA_PRECIOFACTURA_LOC ,  
            IND_VENTA_PRECIONETO_LOC ,  
            IND_VENTA_REAL_LOC ,  
            IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC ,  
            IND_VENTA_REAL_IMPUESTO_LOC ,  
            IND_VENTA_PRECIOCONSTRUCTORA_LOC ,  
            IND_VENTA_FLETES_LOC ,  
            IND_VENTA_REAL_FLETES_LOC ,  
            IND_VENTA_REAL_FLETES_IMPUESTOS_LOC ,  
            IND_VENTA_DESCUENTOS_LOC ,  
            IND_VENTA_PROMOCIONES_LOC ,  
            IND_VENTA_REBATES_LOC ,  
            IND_VENTA_CORRECTIVAS_LOC ,  
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC ,  
            IND_VENTA_IMPUESTOS_LOC ,  
            IND_COSTOSINTERNOS_LOC ,  
            MON_LOC ,  
            IND_VENTA_PRECIOLISTA_USD ,  
            IND_VENTA_PRECIOTEORICO_USD ,  
            IND_VENTA_PRECIOFACTURA_USD ,  
            IND_VENTA_PRECIONETO_USD ,  
            IND_VENTA_REAL_USD ,  
            IND_VENTA_PRECIOFACTURA_IMPUESTO_USD ,  
            IND_VENTA_REAL_IMPUESTO_USD ,  
            IND_VENTA_PRECIOCONSTRUCTORA_USD ,  
            IND_VENTA_FLETES_USD ,  
            IND_VENTA_REAL_FLETES_USD ,  
            IND_VENTA_REAL_FLETES_IMPUESTOS_USD ,  
            IND_VENTA_DESCUENTOS_USD ,  
            IND_VENTA_PROMOCIONES_USD ,  
            IND_VENTA_REBATES_USD ,  
            IND_VENTA_CORRECTIVAS_USD ,  
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD ,  
            IND_VENTA_IMPUESTOS_USD ,  
            IND_COSTOSINTERNOS_USD ,  
            MON_USD ,  
            IND_VENTA_PRECIOLISTA_MXN ,  
            IND_VENTA_PRECIOTEORICO_MXN ,  
            IND_VENTA_PRECIOFACTURA_MXN ,  
            IND_VENTA_PRECIONETO_MXN ,  
            IND_VENTA_REAL_MXN ,  
            IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN ,  
            IND_VENTA_REAL_IMPUESTO_MXN ,  
            IND_VENTA_PRECIOCONSTRUCTORA_MXN ,  
            IND_VENTA_FLETES_MXN ,  
            IND_VENTA_REAL_FLETES_MXN ,  
            IND_VENTA_REAL_FLETES_IMPUESTOS_MXN ,  
            IND_VENTA_DESCUENTOS_MXN ,  
            IND_VENTA_PROMOCIONES_MXN ,  
            IND_VENTA_REBATES_MXN ,  
            IND_VENTA_CORRECTIVAS_MXN ,  
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN ,  
            IND_VENTA_IMPUESTOS_MXN ,  
            IND_COSTOSINTERNOS_MXN ,  
            MON_MXN ,  
            IND_PESO_BRUTO ,  
            IND_PESO_NETO ,  
            UNI_PESO ,  
            SISORIGEN_ID ,  
            MANDANTE ,  
            FECHA_CARGA ,  
            ZONA_HORARIA   
        )   
        SELECT   
            YEAR(FKDAT)  AS ANIO ,
            MONTH(FKDAT)  AS MES ,
            TO_CHAR(FKDAT, 'YYYYMM') AS ANIOMES , 
            VKORG AS SOCIEDAD_ID ,
            VKORG AS ORGVENTAS_ID ,
            VTWEG AS CANALDISTRIB_ID ,
            '00' AS SECTOR_ID ,
            WERKS AS CENTRO_ID ,
            WERKS || '_' || LGORT AS ALMACENCENTRO_ID ,
            LGORT AS ALMACEN_ID ,
            VKBUR AS OFICINAVENTAS_ID ,
            BZIRK AS ZONAVENTAS_ID ,
            VKGRP AS GRUPOVENDEDORES_ID ,
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
            END AS UENADHESIVOS_ID ,
            COUNTRY AS PAIS_ID ,
            COUNTRY || '_' || REGION AS REGION_ID ,
            COUNTRY || '_' || TRANSPZONE AS ZONATRANSPORTE_ID ,
            VBTYP AS TIPO_TRANSACCION ,
            VBELN_VF AS FACTURA ,
            POSNR_VF AS FACTURA_POS ,
            FKDAT AS FECHA_FACTURA ,
            FKART AS CLASEFACTURA_ID ,
            CASE    
            WHEN FKART IN ('ZS1', 'ZS2', 'ZIVS') THEN FKART_ASOCIADO    
            ELSE FKART    
            END AS CLASEFACTURA_ORIGEN_ID ,
            FKART_ASOCIADO AS CLASEFACTURA_ASOCIADO_ID ,
            '' AS INDICADOR_ANULACION ,
            RFBSK AS STATUSCONTABILIDAD ,
            VSBED AS CONDICIONEXP_ID ,
            INCO1 AS INCOTERMS_ID ,
            KUNNR_ZV AS ASESORFACTURA_ID ,
            PSTYV AS TIPOPOSICION_ID ,
            AUBEL AS PEDIDO ,
            AUPOS AS PEDIDO_POS ,
            ZUONR AS ORDENCOMPRA ,
            MOTIVO_PEDIDO AS MOTIVOPEDIDO_ID ,
            ASESOR_COM AS ASESORPEDIDO_ID ,
            TIENDA_REC AS TIENDARECIBO_ID ,
            CNSTR_ID AS CONSTRUCTORA_ID ,
            CASE    
                WHEN SUBMI IS NULL OR SUBMI = '' THEN 'NA'    
                ELSE SUBSTRING(SUBMI, 1, 2)    
            END AS TIPO_OBRA_ID ,
            ID_CONVENIO AS CONVENIO_OBRA ,
            SUBMI AS PLAN_OBRA ,
            LEFT(SEGMENTO, 2) AS SEGMENTO_OBRA_ID ,
            KUNNR_ZP AS PROMOTOR_ID ,
            'NIO' AS NIO_OBRA ,
            TKNUM AS NUM_TRANSPORTE ,
            SHTYP AS CLASETRANSPORTE_ID ,
            CASE    
                WHEN FKART = 'ZMEX' THEN    
                    CASE    
                    WHEN INCO1 IN ('ZF0', 'ZF1' ,'ZF2', 'ZF3') THEN 'MARITIMO'    
                    ELSE 'TERRESTRE'    
                    END   
                WHEN FKART = 'ZEXM' THEN 'MARITIMO'    
                ELSE 'TERRESTRE'    
            END AS TIPOTRANSPORTE_ID ,
            VSART AS CLASEEXPEDICION_ID ,
            TPLST AS PUESTOPLANTRANSP_ID ,
            TDLNR AS TRANSPORTISTA_ID ,
            ROUTE AS RUTA_ID ,
            ADD01 AS TIPOVEHICULO_ID ,
            EXTI1 AS CHOFER ,
            DTABF AS FECHA_DESPACHOEXPREAL ,
            KUNAG AS CLIENTE_ID ,
            VKORG || '_' || VTWEG || '_00_' || LTRIM(KUNAG,'0') AS SOLICITANTE_ID ,
            VKORG || '_' || VTWEG || '_00_' || LTRIM(KUNNR_WE,'0') AS DESTINATARIO_ID ,
            PAGADOR AS DEUDOR_ID ,
            LTRIM(MATNR, '0') AS MATERIAL_ID ,
            VKORG || '_' || VTWEG || '_' || LTRIM(MATNR, '0') AS MATERIALVENTAS_ID ,
            WERKS || '_' || LTRIM(MATNR, '0') AS MATERIALCENTRO_ID ,
            CHARG AS LOTE ,
            CASE    
                WHEN CENTRO_LOTE = '' THEN WERKS    
                ELSE CENTRO_LOTE    
            END AS LOTECENTRO_ID ,
            MATKL AS GRUPOARTICULOS_ID ,
            CASE    
                WHEN MTART IN ('FERT', 'ZHAW') THEN MTART    
                ELSE NULL    
            END AS TIPOMATERIAL_ID ,
            CANT_UN_EST AS IND_VENTA_REAL_EST ,
            COALESCE(U.VALOR_ESTANDARD, T.MSEHI) AS UNI_EST,
            FKIMG AS IND_VENTA_REAL_UMV ,
            VRKME AS UNI_UMV ,
            IMPORTE_VTA * KURRF AS IND_VENTA_PRECIOLISTA_LOC ,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF AS IND_VENTA_PRECIOTEORICO_LOC ,
            IMPORTE_FACTURA * KURRF AS IND_VENTA_PRECIOFACTURA_LOC ,
            (IMPORTE_FACTURA + RAPPELES ) * KURRF AS IND_VENTA_PRECIONETO_LOC ,
            VALOR_NETO * KURRF AS IND_VENTA_REAL_LOC ,
            CASE    
            WHEN PSTYV NOT IN ('B1E', 'B1N' ,'G2TX')    
            THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF    
            END AS IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC ,
            (VALOR_NETO + MWSBP) *KURRF AS IND_VENTA_REAL_IMPUESTO_LOC ,
            CANT_UN_EST * PRECIO * KURRF AS IND_VENTA_PRECIOCONSTRUCTORA_LOC ,
            FLETES * KURRF AS IND_VENTA_FLETES_LOC ,
            ( VALOR_NETO + FLETES ) * KURRF AS IND_VENTA_REAL_FLETES_LOC ,
            ( VALOR_NETO + FLETES + MWSBP ) * KURRF AS IND_VENTA_REAL_FLETES_IMPUESTOS_LOC ,
            DESCUENTO_COMERC * KURRF AS IND_VENTA_DESCUENTOS_LOC ,
            PROMOCIONES * KURRF AS IND_VENTA_PROMOCIONES_LOC ,
            RAPPELES * KURRF AS IND_VENTA_REBATES_LOC ,
            ACCIONES_CORRECT * KURRF AS IND_VENTA_CORRECTIVAS_LOC ,
            CASE    
            WHEN MOTIVO_PEDIDO = 'ZDB'    
            THEN ACCIONES_CORRECT * KURRF   
            ELSE 0   
            END AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC ,
            MWSBP * KURRF AS IND_VENTA_IMPUESTOS_LOC ,
            WAVWR * KURRF AS IND_COSTOSINTERNOS_LOC ,
            MONEDA_BASE AS MON_LOC ,
            IMPORTE_VTA * KURRF_USD AS IND_VENTA_PRECIOLISTA_USD ,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF_USD AS IND_VENTA_PRECIOTEORICO_USD ,
            IMPORTE_FACTURA * KURRF_USD AS IND_VENTA_PRECIOFACTURA_USD ,
            (IMPORTE_FACTURA + RAPPELES) * KURRF_USD AS IND_VENTA_PRECIONETO_USD ,
            VALOR_NETO * KURRF_USD AS IND_VENTA_REAL_USD ,
            CASE    
            WHEN PSTYV NOT IN ('B1E', 'B1N', 'G2TX')    
            THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF_USD   
            ELSE 0   
            END AS IND_VENTA_PRECIOFACTURA_IMPUESTO_USD ,
            (VALOR_NETO + MWSBP) * KURRF_USD AS IND_VENTA_REAL_IMPUESTO_USD ,
            CANT_UN_EST * PRECIO * KURRF_USD AS IND_VENTA_PRECIOCONSTRUCTORA_USD ,
            FLETES * KURRF_USD AS IND_VENTA_FLETES_USD ,
            (VALOR_NETO + FLETES) * KURRF_USD AS IND_VENTA_REAL_FLETES_USD ,
            (VALOR_NETO + FLETES + MWSBP) * KURRF_USD AS IND_VENTA_REAL_FLETES_IMPUESTOS_USD ,
            DESCUENTO_COMERC * KURRF_USD AS IND_VENTA_DESCUENTOS_USD ,
            PROMOCIONES * KURRF_USD AS IND_VENTA_PROMOCIONES_USD ,
            RAPPELES * KURRF_USD AS IND_VENTA_REBATES_USD ,
            ACCIONES_CORRECT * KURRF_USD AS IND_VENTA_CORRECTIVAS_USD ,
            CASE    
            WHEN MOTIVO_PEDIDO = 'ZDB'   
            THEN ACCIONES_CORRECT * KURRF_USD   
            ELSE NULL   
            END AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD ,
            MWSBP * KURRF_USD AS IND_VENTA_IMPUESTOS_USD ,
            WAVWR * KURRF_USD AS IND_COSTOSINTERNOS_USD ,
            'USD' AS MON_USD ,
            IMPORTE_VTA * KURRF_MXN AS IND_VENTA_PRECIOLISTA_MXN ,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF_MXN AS IND_VENTA_PRECIOTEORICO_MXN ,
            IMPORTE_FACTURA * KURRF_MXN AS IND_VENTA_PRECIOFACTURA_MXN ,
            (IMPORTE_FACTURA + RAPPELES) * KURRF_MXN AS IND_VENTA_PRECIONETO_MXN ,
            VALOR_NETO * KURRF_MXN AS IND_VENTA_REAL_MXN ,
            CASE    
            WHEN PSTYV NOT IN ('B1E', 'B1N', 'G2TX')    
            THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF_MXN   
            ELSE NULL   
            END AS IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN ,
            (VALOR_NETO + MWSBP) * KURRF_MXN AS IND_VENTA_REAL_IMPUESTO_MXN ,
            CANT_UN_EST * PRECIO * KURRF_MXN AS IND_VENTA_PRECIOCONSTRUCTORA_MXN ,
            FLETES * KURRF_MXN AS IND_VENTA_FLETES_MXN ,
            (VALOR_NETO + FLETES) * KURRF_MXN AS IND_VENTA_REAL_FLETES_MXN ,
            (VALOR_NETO + FLETES + MWSBP) * KURRF_MXN AS IND_VENTA_REAL_FLETES_IMPUESTOS_MXN ,
            DESCUENTO_COMERC * KURRF_MXN AS IND_VENTA_DESCUENTOS_MXN ,
            PROMOCIONES * KURRF_MXN AS IND_VENTA_PROMOCIONES_MXN ,
            RAPPELES * KURRF_MXN AS IND_VENTA_REBATES_MXN ,
            ACCIONES_CORRECT * KURRF_MXN AS IND_VENTA_CORRECTIVAS_MXN ,
            CASE    
            WHEN MOTIVO_PEDIDO = 'ZDB'    
            THEN ACCIONES_CORRECT * KURRF_MXN   
            ELSE NULL   
            END AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN ,
            MWSBP * KURRF_MXN AS IND_VENTA_IMPUESTOS_MXN ,
            WAVWR * KURRF_MXN AS IND_COSTOSINTERNOS_MXN ,
            'MXN' AS MON_MXN ,
            BRGEW AS IND_PESO_BRUTO ,
            NTGEW AS IND_PESO_NETO ,
            GEWEI AS UNI_PESO ,
            SISORIGEN_ID AS SISORIGEN_ID ,
            MANDANTE AS MANDANTE ,
            FECHA_CARGA AS FECHA_CARGA ,
            ZONA_HORARIA AS ZONA_HORARIA 
        FROM 
            TABLE( :TABLA ) T
            LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U
                ON UPPER(TRIM(T.MSEHI)) = UPPER(TRIM(U.VALOR_ORIGINAL))
            --RAW.SQ1_MOF_ZBWSD_CUADERNO_FINANCIERO
        WHERE
            VKORG BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  ;


        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED 
        FROM PRE.PFCT_COM_VENTAS
        WHERE
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  ;


        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PFCT_COM_VENTAS','PFCT_COM_VENTAS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 