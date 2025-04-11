CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_REV_VENTAS_ACT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-07
 Creador:            Agustin Gutierrez
 Descripción:        SP que transforma datos desde la capa PRE a CON para FCT_COM_REV_VENTAS_ACT
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM LAMOSALAKE_DEV.CON.FCT_COM_REV_VENTAS_ACT;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO LAMOSALAKE_DEV.CON.FCT_COM_REV_VENTAS_ACT
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
        FROM LAMOSALAKE_DEV.PRE.PFCT_COM_VENTAS
        WHERE UPPER(ORGVENTAS_ID) IN ('R101', 'R311', 'R312', 'R313', 'R401');

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.CON.FCT_COM_REV_VENTAS_ACT;

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
    VALUES ('SP_CON_FCT_COM_REV_VENTAS_ACT','FCT_COM_REV_VENTAS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 