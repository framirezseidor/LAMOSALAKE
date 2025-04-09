CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.PRE.SP_PFCT_COM_VENTAS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Agustin Gutierrez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_COM_VENTAS
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM LAMOSALAKE_DEV.PRE.PFCT_COM_VENTAS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en DELETE: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO LAMOSALAKE_DEV.PRE.PFCT_COM_VENTAS
        (
            ANIO,
            MES,
            ANIOMES,
            SOCIEDAD_ID,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            CENTRO_ID,
            ALMACENCENTRO_ID,
            ALMACEN_ID,
            OFICINAVENTAS_ID,
            ZONAVENTAS_ID,
            GRUPOVENDEDORES_ID,
            UENADHESIVOS_ID,
            PAIS_ID,
            REGION_ID,
            ZONATRANSPORTE_ID,
            TIPO_TRANSACCION,
            FACTURA,
            FACTURA_POS,
            FECHA_FACTURA,
            CLASEFACTURA_ID,
            CLASEFACTURA_ORIGEN_ID,
            CLASEFACTURA_ASOCIADO_ID,
            INDICADOR_ANULACION,
            STATUSCONTABILIDAD,
            CONDICIONEXP_ID,
            INCOTERMS_ID,
            ASESORFACTURA_ID,
            TIPOPOSICION_ID,
            PEDIDO,
            PEDIDO_POS,
            ORDENCOMPRA,
            MOTIVOPEDIDO_ID,
            ASESORPEDIDO_ID,
            TIENDARECIBO_ID,
            CONSTRUCTORA_ID,
            TIPO_OBRA_ID,
            CONVENIO_OBRA,
            PLAN_OBRA,
            SEGMENTO_OBRA_ID,
            PROMOTOR_ID,
            NIO_OBRA,
            NUM_TRANSPORTE,
            CLASETRANSPORTE_ID,
            TIPOTRANSPORTE_ID,
            CLASEEXPEDICION_ID,
            PUESTOPLANTRANSP_ID,
            TRANSPORTISTA_ID,
            RUTA_ID,
            TIPOVEHICULO_ID,
            CHOFER,
            FECHA_DESPACHOEXPREAL,
            CLIENTE_ID,
            SOLICITANTE_ID,
            DESTINATARIO_ID,
            DEUDOR_ID,
            MATERIAL_ID,
            MATERIALVENTAS_ID,
            MATERIALCENTRO_ID,
            LOTE,
            LOTECENTRO_ID,
            GRUPOARTICULOS_ID,
            TIPOMATERIAL_ID,
            IND_VENTA_REAL_EST,
            UNI_EST,
            IND_VENTA_REAL_UMV,
            UNI_UMV,
            IND_VENTA_PRECIOLISTA_LOC,
            IND_VENTA_PRECIOTEORICO_LOC,
            IND_VENTA_PRECIOFACTURA_LOC,
            IND_VENTA_PRECIONETO_LOC,
            IND_VENTA_REAL_LOC,
            IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC,
            IND_VENTA_REAL_IMPUESTO_LOC,
            IND_VENTA_PRECIOCONSTRUCTORA_LOC,
            IND_VENTA_FLETES_LOC,
            IND_VENTA_REAL_FLETES_LOC,
            IND_VENTA_REAL_FLETES_IMPUESTOS_LOC,
            IND_VENTA_DESCUENTOS_LOC,
            IND_VENTA_PROMOCIONES_LOC,
            IND_VENTA_REBATES_LOC,
            IND_VENTA_CORRECTIVAS_LOC,
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC,
            IND_VENTA_IMPUESTOS_LOC,
            IND_COSTOSINTERNOS_LOC,
            MON_LOC,
            IND_VENTA_PRECIOLISTA_USD,
            IND_VENTA_PRECIOTEORICO_USD,
            IND_VENTA_PRECIOFACTURA_USD,
            IND_VENTA_PRECIONETO_USD,
            IND_VENTA_REAL_USD,
            IND_VENTA_PRECIOFACTURA_IMPUESTO_USD,
            IND_VENTA_REAL_IMPUESTO_USD,
            IND_VENTA_PRECIOCONSTRUCTORA_USD,
            IND_VENTA_FLETES_USD,
            IND_VENTA_REAL_FLETES_USD,
            IND_VENTA_REAL_FLETES_IMPUESTOS_USD,
            IND_VENTA_DESCUENTOS_USD,
            IND_VENTA_PROMOCIONES_USD,
            IND_VENTA_REBATES_USD,
            IND_VENTA_CORRECTIVAS_USD,
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD,
            IND_VENTA_IMPUESTOS_USD,
            IND_COSTOSINTERNOS_USD,
            MON_USD,
            IND_VENTA_PRECIOLISTA_MXN,
            IND_VENTA_PRECIOTEORICO_MXN,
            IND_VENTA_PRECIOFACTURA_MXN,
            IND_VENTA_PRECIONETO_MXN,
            IND_VENTA_REAL_MXN,
            IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN,
            IND_VENTA_REAL_IMPUESTO_MXN,
            IND_VENTA_PRECIOCONSTRUCTORA_MXN,
            IND_VENTA_FLETES_MXN,
            IND_VENTA_REAL_FLETES_MXN,
            IND_VENTA_REAL_FLETES_IMPUESTOS_MXN,
            IND_VENTA_DESCUENTOS_MXN,
            IND_VENTA_PROMOCIONES_MXN,
            IND_VENTA_REBATES_MXN,
            IND_VENTA_CORRECTIVAS_MXN,
            IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN,
            IND_VENTA_IMPUESTOS_MXN,
            IND_COSTOSINTERNOS_MXN,
            MON_MXN,
            IND_PESO_BRUTO,
            IND_PESO_NETO,
            UNI_PESO,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            YEAR(FKDAT),
            MONTH(FKDAT),
            TO_CHAR(FKDAT, 'MMYYYY'),
            VKORG,
            VKORG,
            VTWEG,
            '00',
            WERKS,
            WERKS || '_' || LGORT,
            LGORT,
            VKBUR,
            BZIRK,
            VKGRP,
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
            END,
            COUNTRY,
            COUNTRY || '_' || REGION,
            COUNTRY || '_' || TRANSPZONE,
            VBTYP,
            VBELN_VF,
            POSNR_VF,
            FKDAT,
            FKART,
            CASE 
                WHEN FKART IN ('ZS1', 'ZS2', 'ZIVS') THEN FKART_ASOCIADO 
                ELSE FKART 
            END,
            FKART_ASOCIADO,
            '',
            RFBSK,
            VSBED,
            INCO1,
            KUNNR_ZV,
            PSTYV,
            AUBEL,
            AUPOS,
            ZUONR,
            MOTIVO_PEDIDO,
            ASESOR_COM,
            TIENDA_REC,
            CNSTR_ID,
            CASE 
                WHEN SUBMI IS NULL OR SUBMI = '' THEN 'NA' 
                ELSE SUBSTRING(SUBMI, 1, 2) 
            END,
            ID_CONVENIO,
            SUBMI,
            SEGMENTO,
            KUNNR_ZP,
            'NIO',
            TKNUM,
            SHTYP,
            CASE 
                WHEN FKART = 'ZMEX' THEN 
                    CASE 
                        WHEN INCO1 IN ('ZF0', 'ZF1', 'ZF2', 'ZF3') THEN 'MARITIMO' 
                        ELSE 'TERRESTRE' 
                    END
                WHEN FKART = 'ZEXM' THEN 'MARITIMO' 
                ELSE 'TERRESTRE' 
            END,
            VSART,
            TPLST,
            TDLNR,
            ROUTE,
            ADD01,
            EXTI1,
            DTABF,
            KUNAG,
            VKORG || '_' || VTWEG || '_00_' || KUNAG,
            VKORG || '_' || VTWEG || '_00_' || KUNNR_WE,
            PAGADOR,
            MATNR,
            VKORG || '_' || VTWEG || '_' || MATNR,
            WERKS || '_' || MATNR,
            CHARG,
            CASE 
                WHEN CENTRO_LOTE = '' THEN WERKS 
                ELSE CENTRO_LOTE 
            END,
            MATKL,
            CASE 
                WHEN MTART IN ('FERT', 'ZHAW') THEN MTART 
                ELSE NULL 
            END,
            CANT_UN_EST,
            MSEHI,
            FKIMG,
            VRKME,
            IMPORTE_VTA * KURRF,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF,
            IMPORTE_FACTURA * KURRF,
            (IMPORTE_FACTURA + RAPPELES ) * KURRF,
            VALOR_NETO * KURRF,
            CASE 
                WHEN PSTYV NOT IN ('B1E', 'B1N', 'G2TX') 
                THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF 
            END,
            (VALOR_NETO + MWSBP) *KURRF,
            CANT_UN_EST * PRECIO * KURRF,
            FLETES * KURRF,
            ( VALOR_NETO + FLETES ) * KURRF,
            ( VALOR_NETO + FLETES + MWSBP ) * KURRF,
            DESCUENTO_COMERC * KURRF,
            PROMOCIONES * KURRF,
            RAPPELES * KURRF,
            ACCIONES_CORRECT * KURRF,
            CASE 
                WHEN MOTIVO_PEDIDO = 'ZDB' 
                THEN ACCIONES_CORRECT * KURRF
                ELSE 0
            END,
            MWSBP * KURRF,
            WAVWR * KURRF,
            MONEDA_BASE,
            IMPORTE_VTA * KURRF_USD,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF_USD,
            IMPORTE_FACTURA * KURRF_USD,
            (IMPORTE_FACTURA + RAPPELES) * KURRF_USD,
            VALOR_NETO * KURRF_USD,
            CASE 
                WHEN PSTYV NOT IN ('B1E', 'B1N', 'G2TX') 
                THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF_USD
                ELSE 0
            END,
            (VALOR_NETO + MWSBP) * KURRF_USD,
            CANT_UN_EST * PRECIO * KURRF_USD,
            FLETES * KURRF_USD,
            (VALOR_NETO + FLETES) * KURRF_USD,
            (VALOR_NETO + FLETES + MWSBP) * KURRF_USD,
            DESCUENTO_COMERC * KURRF_USD,
            PROMOCIONES * KURRF_USD,
            RAPPELES * KURRF_USD,
            ACCIONES_CORRECT * KURRF_USD,
            CASE 
                WHEN MOTIVO_PEDIDO = 'ZDB'
                THEN ACCIONES_CORRECT * KURRF_USD
                ELSE NULL
            END,
            MWSBP * KURRF_USD,
            WAVWR * KURRF,
            'USD',
            IMPORTE_VTA * KURRF_MXN,
            (IMPORTE_VTA + DESCUENTO_COMERC) * KURRF_MXN,
            IMPORTE_FACTURA * KURRF_MXN,
            (IMPORTE_FACTURA + RAPPELES) * KURRF_MXN,
            VALOR_NETO * KURRF_MXN,
            CASE 
                WHEN PSTYV NOT IN ('B1E', 'B1N', 'G2TX') 
                THEN (IMPORTE_FACTURA + ACCIONES_CORRECT + MWSBP) * KURRF_MXN
                ELSE NULL
            END,
            (VALOR_NETO + MWSBP) * KURRF_MXN,
            CANT_UN_EST * PRECIO * KURRF_MXN,
            FLETES * KURRF_MXN,
            (VALOR_NETO + FLETES) * KURRF_MXN,
            (VALOR_NETO + FLETES + MWSBP) * KURRF_MXN,
            DESCUENTO_COMERC * KURRF_MXN,
            PROMOCIONES * KURRF_MXN,
            RAPPELES * KURRF_MXN,
            ACCIONES_CORRECT * KURRF_MXN,
            CASE 
                WHEN MOTIVO_PEDIDO = 'ZDB' 
                THEN ACCIONES_CORRECT * KURRF_MXN
                ELSE NULL
            END,
            MWSBP * KURRF_MXN,
            WAVWR * KURRF_MXN,
            'MXN',
            BRGEW,
            NTGEW,
            GEWEI,
            'SQ1',
            '500',
            CURRENT_TIMESTAMP(),
            TO_CHAR(CURRENT_TIMESTAMP, 'TZH:TZM')
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWSD_CUADERNO_FINANCIERO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.PRE.PFCT_COM_VENTAS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en INSERT: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 