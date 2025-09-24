    CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_VENTAS_HIST()
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    $$
    /*
    ---------------------------------------------------------------------------------
    Versión:            1.0
    Fecha de creación:  2025-04-24
    Creador:            Fernando Cuellar M
    Descripción:        SP que transforma datos desde la capa PRE A CON para VENTAS HISTORICAS
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

            TRUNCATE TABLE CON.FCT_COM_ADH_VENTAS_HIST;

            SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
            SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
        EXCEPTION
            WHEN statement_error THEN
                SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
        END;

        ---------------------------------------------------------------------------------
        -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA PRE
        ---------------------------------------------------------------------------------
        BEGIN
            SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

            INSERT INTO CON.FCT_COM_ADH_VENTAS_HIST
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
                GRUPOARTICULOS_ID,
                TIPOMATERIAL_ID,
                IND_VENTA_REAL_TON,
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
                SUBSTRING(ANIOMES_NATURAL_CLAVE, 4, 4) AS ANIO,
                SUBSTRING(ANIOMES_NATURAL_CLAVE, 1, 2) AS MES,
                RIGHT(ANIOMES_NATURAL_CLAVE, 4) || LEFT(ANIOMES_NATURAL_CLAVE, 2) AS ANIOMES,
                NULLIF(SOCIEDAD_CLAVE, '#') AS SOCIEDAD_ID,
                NULLIF(ORGANIZACION_VENTAS_CLAVE, '#') AS ORGVENTAS_ID,
                NULLIF(CANAL_DISTRIBUCION_CLAVE, '#') AS CANALDISTRIB_ID,
                '00' AS SECTOR_ID,
                NULLIF(CENTRO_CLAVE, '#') AS CENTRO_ID,
                NULL AS ALMACENCENTRO_ID, --VALIDAR CONTENIDO
                NULL AS ALMACEN_ID,
                NULLIF(OFICINA_VENTAS_CLAVE, '#') AS OFICINAVENTAS_ID,
                NULLIF(ZONA_VENTAS_CLAVE, '#') AS ZONAVENTAS_ID,
                NULLIF(GRUPO_VENDEDORES_CLAVE, '#') AS GRUPOVENDEDORES_ID,
                NULLIF(UEN_CLAVE, '#') AS UENADHESIVOS_ID,
                NULLIF(PAIS_CLAVE, '#') AS PAIS_ID,
                REPLACE(REGION_CLAVE, '/', '_') AS REGION_ID,
                REPLACE(ZONA_TRANSPORTE_CLAVE, '/', '_') AS ZONATRANSPORTE_ID,
                '' AS TIPO_TRANSACCION,
                FACTURA_CLAVE AS FACTURA,
                TRY_TO_NUMBER(POSICION_DE_FACTURA_CLAVE) AS FACTURA_POS,
                CASE 
                    WHEN dia_natural_clave LIKE '%#%' 
                        THEN TO_CHAR(LAST_DAY(TO_DATE(RIGHT(ANIOMES_NATURAL_CLAVE, 4) || LEFT(ANIOMES_NATURAL_CLAVE, 2), 'YYYYMM')), 'YYYY-MM-DD')
                    
                    WHEN LENGTH(dia_natural_clave) <= 7 
                        THEN TO_CHAR(LAST_DAY(TO_DATE(RIGHT(ANIOMES_NATURAL_CLAVE, 4) || LEFT(ANIOMES_NATURAL_CLAVE, 2), 'YYYYMM')), 'YYYY-MM-DD')
                    
                    ELSE TO_CHAR(TO_DATE(dia_natural_clave, 'DD.MM.YYYY'), 'YYYY-MM-DD')
                END AS FECHA_FACTURA, --LAST DAY OF MONTH 2025-06-11
                NULLIF(CLASE_FACTURA_ORIGINAL_CLAVE, '#') AS CLASEFACTURA_ID,
                NULLIF(CLASE_FACTURA_CLAVE, '#') AS CLASEFACTURA_ORIGEN_ID,
                NULL AS CLASEFACTURA_ASOCIADO_ID,
                NULLIF(INDICADOR_DE_ANULACION_CLAVE, '#') AS INDICADOR_ANULACION,
                NULLIF(STATUS_CONTABILIDAD_CLAVE, '#') AS STATUSCONTABILIDAD,
                NULLIF(CONDICION_EXPEDICION_CLAVE, '#') AS CONDICIONEXP_ID,
                NULLIF(INCOTERMS_CLAVE, '#') AS INCOTERMS_ID,
                NULLIF(ASESOR_COMERCIAL_FACTURA_CLAVE, '#') AS ASESORFACTURA_ID,
                NULLIF(TIPO_POSICION_CLAVE, '#') AS TIPOPOSICION_ID,
                NULLIF(PEDIDO_CLAVE, '#') AS PEDIDO,
                TRY_TO_NUMBER(POSICION_PEDIDO_CLAVE) AS PEDIDO_POS,
                NULLIF(ORDEN_DE_COMPRA_CLAVE, '#') AS ORDENCOMPRA,
                NULLIF(MOTIVO_PEDIDO_CLAVE, '#') AS MOTIVOPEDIDO_ID,
                NULLIF(ASESOR_PEDIDO_CLAVE, '#') AS ASESORPEDIDO_ID,
                NULLIF(TIENDA_RECIBO_CLAVE, '#') AS TIENDARECIBO_ID,
                NULLIF(CONSTRUCTORA_CLAVE, '#') AS CONSTRUCTORA_ID,
                NULLIF(TIPO_OBRA_CLAVE, '#') AS TIPO_OBRA_ID,
                NULLIF(CONVENIO, '#') AS CONVENIO_OBRA,
                NULL AS PLAN_OBRA,
                NULLIF(SEGMENTO_CLAVE, '#') AS SEGMENTO_OBRA_ID,
                NULLIF(PROMOTOR_CLAVE, '#') AS PROMOTOR_ID,
                NULL AS NIO_OBRA,
                NULL AS NUM_TRANSPORTE,
                NULL AS CLASETRANSPORTE_ID,
                NULL AS TIPOTRANSPORTE_ID,
                NULL AS CLASEEXPEDICION_ID,
                NULL AS PUESTOPLANTRANSP_ID,
                NULL AS TRANSPORTISTA_ID,
                NULL AS RUTA_ID,
                NULL AS TIPOVEHICULO_ID,
                NULL AS CHOFER,
                NULL AS FECHA_DESPACHOEXPREAL, 
                NULLIF(SOLICITANTE_CLAVE_NO_RELACIONADA, '#') AS CLIENTE_ID,
                --
                SPLIT_PART(SOLICITANTE_CLAVE, '/', 3) || '_' ||
                SPLIT_PART(SOLICITANTE_CLAVE, '/', 2) || '_' ||
                SPLIT_PART(SOLICITANTE_CLAVE, '/', 1) || '_' ||
                SPLIT_PART(SOLICITANTE_CLAVE, '/', 4) AS SOLICITANTE_ID,
                --
                SPLIT_PART(DESTINATARIO_MERCANCIA_CLAVE, '/', 3) || '_' ||
                SPLIT_PART(DESTINATARIO_MERCANCIA_CLAVE, '/', 2) || '_' ||
                SPLIT_PART(DESTINATARIO_MERCANCIA_CLAVE, '/', 1) || '_' ||
                SPLIT_PART(DESTINATARIO_MERCANCIA_CLAVE, '/', 4) AS DESTINATARIO_ID,
                --
                NULLIF(SOLICITANTE_CLAVE_NO_RELACIONADA, '#') AS DEUDOR_ID,
                --
                NULLIF(MATERIAL_CLAVE, '#') AS MATERIAL_ID,
                SOCIEDAD_CLAVE || '_' || CANAL_DISTRIBUCION_CLAVE || '_' || MATERIAL_CLAVE AS MATERIALVENTAS_ID,
                CENTRO_CLAVE || '_' || MATERIAL_CLAVE AS MATERIALCENTRO_ID,
                NULLIF(GRUPO_MATERIALES_SUBLINEA_CLAVE, '#') AS GRUPOARTICULOS_ID,
                NULLIF(TIPO_MATERIAL_CLAVE, '#') AS TIPOMATERIAL_ID,
                VENTA_REAL_TON AS IND_VENTA_REAL_TON,
                UNIDAD_ESTADISTICA_CLAVE AS UNI_EST,
                VENTA_REAL_UM_VENTA AS IND_VENTA_REAL_UMV,
                MEDIDA_DE_VENTA_CLAVE AS UNI_UMV,
                VENTA_PRECIO_LISTA_ML AS IND_VENTA_PRECIOLISTA_LOC,
                VENTA_PRECIO_TEORICO_ML AS IND_VENTA_PRECIOTEORICO_LOC,
                VENTA_PRECIO_FACTURA_ML AS IND_VENTA_PRECIOFACTURA_LOC,
                VENTA_PRECIO_NETO_ML AS IND_VENTA_PRECIONETO_LOC,
                VENTA_REAL_ML AS IND_VENTA_REAL_LOC,
                0 AS IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC,
                VENTA_REAL_IVA_ML AS IND_VENTA_REAL_IMPUESTO_LOC, --
                VENTA_PRECIO_CONSTRUCTORA_ML AS IND_VENTA_PRECIOCONSTRUCTORA_LOC,
                REEMBOLSO_FLETES_ML AS IND_VENTA_FLETES_LOC,
                VENTA_REAL_C_FLETES_ML AS IND_VENTA_REAL_FLETES_LOC,
                VENTA_REAL_C_FLETES_IVA_ML AS IND_VENTA_REAL_FLETES_IMPUESTOS_LOC,
                DESCUENTOS_ML AS IND_VENTA_DESCUENTOS_LOC,
                PROMOCIONES_ML AS IND_VENTA_PROMOCIONES_LOC,
                REBATES_ML AS IND_VENTA_REBATES_LOC,
                CORRECTIVAS_ML AS IND_VENTA_CORRECTIVAS_LOC,
                0 AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC,
                IMPORTE_IVA_ML AS IND_VENTA_IMPUESTOS_LOC,
                0 AS IND_COSTOSINTERNOS_LOC,
                MONEDA_LOCAL_CLAVE AS MON_LOC,
                VENTA_PRECIO_LISTA_USD AS IND_VENTA_PRECIOLISTA_USD,
                VENTA_PRECIO_TEORICO_USD AS IND_VENTA_PRECIOTEORICO_USD,
                VENTA_PRECIO_FACTURA_USD AS IND_VENTA_PRECIOFACTURA_USD,
                VENTA_PRECIO_NETO_USD AS IND_VENTA_PRECIONETO_USD,
                VENTA_REAL_USD AS IND_VENTA_REAL_USD,
                0 AS IND_VENTA_PRECIOFACTURA_IMPUESTO_USD,
                VENTA_REAL_IVA_USD AS IND_VENTA_REAL_IMPUESTO_USD,
                VENTA_PRECIO_CONSTRUCTORA_USD AS IND_VENTA_PRECIOCONSTRUCTORA_USD,
                REEMBOLSO_FLETES_USD AS IND_VENTA_FLETES_USD,
                VENTA_REAL_C_FLETES_USD AS IND_VENTA_REAL_FLETES_USD,
                VENTA_REAL_C_FLETES_IVA_USD AS IND_VENTA_REAL_FLETES_IMPUESTOS_USD,
                DESCUENTOS_USD AS IND_VENTA_DESCUENTOS_USD,
                PROMOCIONES_USD AS IND_VENTA_PROMOCIONES_USD,
                REBATES_USD AS IND_VENTA_REBATES_USD,
                CORRECTIVAS_USD AS IND_VENTA_CORRECTIVAS_USD,
                0 AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD,
                IMPORTE_IVA_USD AS IND_VENTA_IMPUESTOS_USD,
                0 AS IND_COSTOSINTERNOS_USD,
                MONEDA_USD_CLAVE AS MON_USD,
                VENTA_PRECIO_LISTA_MXN AS IND_VENTA_PRECIOLISTA_MXN,
                VENTA_PRECIO_TEORICO_MXN AS IND_VENTA_PRECIOTEORICO_MXN,
                VENTA_PRECIO_FACTURA_MXN AS IND_VENTA_PRECIOFACTURA_MXN,
                VENTA_PRECIO_NETO_MXN AS IND_VENTA_PRECIONETO_MXN,
                VENTA_REAL_MXN AS IND_VENTA_REAL_MXN,
                0 AS IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN,
                VENTA_REAL_IVA_MXN AS IND_VENTA_REAL_IMPUESTO_MXN,
                VENTA_PRECIO_CONSTRUCTORA_MXN AS IND_VENTA_PRECIOCONSTRUCTORA_MXN,
                REEMBOLSO_FLETES_MXN AS IND_VENTA_FLETES_MXN,
                VENTA_REAL_CFLETES_MXN AS IND_VENTA_REAL_FLETES_MXN,
                VENTA_REAL_CFLETES_IVA_MXN AS IND_VENTA_REAL_FLETES_IMPUESTOS_MXN,
                DESCUENTOS_MXN AS IND_VENTA_DESCUENTOS_MXN,
                PROMOCIONES_MXN AS IND_VENTA_PROMOCIONES_MXN,
                REBATES_MXN AS IND_VENTA_REBATES_MXN,
                CORRECTIVAS_MXN AS IND_VENTA_CORRECTIVAS_MXN,
                0 AS IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN,
                IMPORTE_IVA_MXN AS IND_VENTA_IMPUESTOS_MXN,
                0 AS IND_COSTOSINTERNOS_MXN,
                MONEDA_MXN_CLAVE AS MON_MXN,
                PESO_NETO_KG AS IND_PESO_BRUTO,
                PESO_BRUTO_KG AS IND_PESO_NETO,
                UNIDAD_PESO_CLAVE AS UNI_PESO,
                SISORIGEN_ID AS SISORIGEN_ID,
                MANDANTE AS MANDANTE,
                FECHA_CARGA AS FECHA_CARGA,
                ZONA_HORARIA AS ZONA_HORARIA
            FROM PRE.PFCT_COM_ADH_VENTAS_HIST;
            -- WHERE SUBSTRING(ANIOMES_NATURAL_CLAVE, 4, 4) BETWEEN '2023' AND '2024';

            -- Conteo de filas insertadas
            SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_COM_ADH_VENTAS_HIST;

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
        VALUES ('SP_CON_FCT_COM_VENTAS_HIST','CON.FCT_COM_ADH_VENTAS_HIST', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

        ---------------------------------------------------------------------------------
        -- STEP 5: FINALIZACIÓN
        ---------------------------------------------------------------------------------
        RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

    END;
    $$;


 call CON.SP_CON_FCT_COM_VENTAS_HIST();   


-- create or replace stream MIRRORING.STREAM_FCT_COM_ADH_VENTAS_HIST on view  
-- MIRRORING.VW_FCT_COM_ADH_VENTAS_HIST;