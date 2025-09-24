CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_REV_VENTAS_ACT(
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
 Fecha de creación:  2025-04-07
 Creador:            Fidel Ramírez
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
    RANGOFECHAS           VARCHAR(200);
    PARAMETRO       VARCHAR(20);
    SOCIEDAD_INICIO   VARCHAR(20);
    SOCIEDAD_FIN      VARCHAR(20);
    FT_INI VARCHAR(30);
    FT_FIN VARCHAR(30);

BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE SOCIEDADES
    ---------------------------------------------------------------------------------
    PARAMETRO := 'VKORG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :SOCIEDAD_INICIO ;

    SOCIEDAD_FIN := SPLIT_PART(SOCIEDAD_INICIO,'-',2);
    SOCIEDAD_INICIO := SPLIT_PART(SOCIEDAD_INICIO,'-',1);    

    --BUSCANDO VALORES DE FECHA
    PARAMETRO := 'FKDAT';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :RANGOFECHAS ;

    FT_INI := SPLIT_PART(RANGOFECHAS,'-',2);
    FT_FIN := SPLIT_PART(RANGOFECHAS,'-',1);     

    F_INICIO := (SELECT 
        CASE 
            WHEN :FT_INI = '' THEN RAW.FN_OBTENER_FECHA_INICIO(CURRENT_DATE(), 7)
            ELSE :FT_INI
        END);

    F_FIN := (SELECT 
        CASE 
            WHEN :FT_FIN = '' THEN CURRENT_DATE()
            ELSE :FT_FIN
        END);

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM CON.FCT_COM_REV_VENTAS_ACT
        WHERE
        (SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN )
        AND (FECHA_FACTURA BETWEEN :F_INICIO AND :F_FIN );        


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

        MERGE INTO CON.FCT_COM_REV_VENTAS_ACT A
        USING (SELECT ANIO,
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
                        IND_VENTA_REAL_EST AS IND_VENTA_REAL_M2, 
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
                        CURRENT_TIMESTAMP() FECHA_CARGA, 
                        ZONA_HORARIA
            FROM PRE.PFCT_COM_VENTAS 
            WHERE 
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN
            AND TIPOMATERIAL_ID IN ('ZHAW','FERT')
             ) P
        ON A.ORGVENTAS_ID = P.ORGVENTAS_ID	AND
            A.CANALDISTRIB_ID = P.CANALDISTRIB_ID	AND
            A.SECTOR_ID = P.SECTOR_ID	AND
            A.CENTRO_ID = P.CENTRO_ID	AND
            A.FECHA_FACTURA = P.FECHA_FACTURA	AND
            A.CLASEFACTURA_ID = P.CLASEFACTURA_ID	AND
            A.TIPO_TRANSACCION = P.TIPO_TRANSACCION AND
            --A.PEDIDO = P.PEDIDO	AND
            --A.PEDIDO_POS = P.PEDIDO_POS AND
            A.FACTURA = P.FACTURA	AND
            A.FACTURA_POS = P.FACTURA_POS                
        WHEN MATCHED THEN 
        UPDATE SET 
            A.ANIO = P.ANIO,
            A.MES  = P.MES ,
            A.ANIOMES  = P.ANIOMES ,
            A.SOCIEDAD_ID  = P.SOCIEDAD_ID ,
            A.ALMACENCENTRO_ID  = P.ALMACENCENTRO_ID ,
            A.ALMACEN_ID  = P.ALMACEN_ID ,
            A.OFICINAVENTAS_ID  = P.OFICINAVENTAS_ID ,
            A.ZONAVENTAS_ID  = P.ZONAVENTAS_ID ,
            A.GRUPOVENDEDORES_ID  = P.GRUPOVENDEDORES_ID ,
            A.PAIS_ID  = P.PAIS_ID ,
            A.REGION_ID  = P.REGION_ID ,
            A.ZONATRANSPORTE_ID  = P.ZONATRANSPORTE_ID ,
            A.TIPO_TRANSACCION  = P.TIPO_TRANSACCION ,
            A.FACTURA  = P.FACTURA ,
            A.FACTURA_POS  = P.FACTURA_POS ,
            A.CLASEFACTURA_ORIGEN_ID  = P.CLASEFACTURA_ORIGEN_ID ,
            A.CLASEFACTURA_ASOCIADO_ID  = P.CLASEFACTURA_ASOCIADO_ID ,
            A.INDICADOR_ANULACION  = P.INDICADOR_ANULACION ,
            A.STATUSCONTABILIDAD  = P.STATUSCONTABILIDAD ,
            A.CONDICIONEXP_ID  = P.CONDICIONEXP_ID ,
            A.INCOTERMS_ID  = P.INCOTERMS_ID ,
            A.ASESORFACTURA_ID  = P.ASESORFACTURA_ID ,
            A.TIPOPOSICION_ID  = P.TIPOPOSICION_ID ,
            A.ORDENCOMPRA  = P.ORDENCOMPRA ,
            A.MOTIVOPEDIDO_ID  = P.MOTIVOPEDIDO_ID ,
            A.ASESORPEDIDO_ID  = P.ASESORPEDIDO_ID ,
            A.TIENDARECIBO_ID  = P.TIENDARECIBO_ID ,
            A.CONSTRUCTORA_ID  = P.CONSTRUCTORA_ID ,
            A.TIPO_OBRA_ID  = P.TIPO_OBRA_ID ,
            A.CONVENIO_OBRA  = P.CONVENIO_OBRA ,
            A.PLAN_OBRA  = P.PLAN_OBRA ,
            A.SEGMENTO_OBRA_ID  = P.SEGMENTO_OBRA_ID ,
            A.PROMOTOR_ID  = P.PROMOTOR_ID ,
            A.NIO_OBRA  = P.NIO_OBRA ,
            A.NUM_TRANSPORTE  = P.NUM_TRANSPORTE ,
            A.CLASETRANSPORTE_ID  = P.CLASETRANSPORTE_ID ,
            A.TIPOTRANSPORTE_ID  = P.TIPOTRANSPORTE_ID ,
            A.CLASEEXPEDICION_ID  = P.CLASEEXPEDICION_ID ,
            A.PUESTOPLANTRANSP_ID  = P.PUESTOPLANTRANSP_ID ,
            A.TRANSPORTISTA_ID  = P.TRANSPORTISTA_ID ,
            A.RUTA_ID  = P.RUTA_ID ,
            A.TIPOVEHICULO_ID  = P.TIPOVEHICULO_ID ,
            A.CHOFER  = P.CHOFER ,
            A.FECHA_DESPACHOEXPREAL  = P.FECHA_DESPACHOEXPREAL ,
            A.CLIENTE_ID  = P.CLIENTE_ID ,
            A.SOLICITANTE_ID  = P.SOLICITANTE_ID ,
            A.DESTINATARIO_ID  = P.DESTINATARIO_ID ,
            A.DEUDOR_ID  = P.DEUDOR_ID ,
            A.MATERIAL_ID  = P.MATERIAL_ID ,
            A.MATERIALVENTAS_ID  = P.MATERIALVENTAS_ID ,
            A.MATERIALCENTRO_ID  = P.MATERIALCENTRO_ID ,
            A.LOTE  = P.LOTE ,
            A.LOTECENTRO_ID  = P.LOTECENTRO_ID ,
            A.GRUPOARTICULOS_ID  = P.GRUPOARTICULOS_ID ,
            A.TIPOMATERIAL_ID  = P.TIPOMATERIAL_ID ,
            A.IND_VENTA_REAL_M2  = P.IND_VENTA_REAL_M2 ,
            A.UNI_EST  = P.UNI_EST ,
            A.IND_VENTA_REAL_UMV  = P.IND_VENTA_REAL_UMV ,
            A.UNI_UMV  = P.UNI_UMV ,
            A.IND_VENTA_PRECIOLISTA_LOC  = P.IND_VENTA_PRECIOLISTA_LOC ,
            A.IND_VENTA_PRECIOTEORICO_LOC  = P.IND_VENTA_PRECIOTEORICO_LOC ,
            A.IND_VENTA_PRECIOFACTURA_LOC  = P.IND_VENTA_PRECIOFACTURA_LOC ,
            A.IND_VENTA_PRECIONETO_LOC  = P.IND_VENTA_PRECIONETO_LOC ,
            A.IND_VENTA_REAL_LOC  = P.IND_VENTA_REAL_LOC ,
            A.IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC  = P.IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC ,
            A.IND_VENTA_REAL_IMPUESTO_LOC  = P.IND_VENTA_REAL_IMPUESTO_LOC ,
            A.IND_VENTA_PRECIOCONSTRUCTORA_LOC  = P.IND_VENTA_PRECIOCONSTRUCTORA_LOC ,
            A.IND_VENTA_FLETES_LOC  = P.IND_VENTA_FLETES_LOC ,
            A.IND_VENTA_REAL_FLETES_LOC  = P.IND_VENTA_REAL_FLETES_LOC ,
            A.IND_VENTA_REAL_FLETES_IMPUESTOS_LOC  = P.IND_VENTA_REAL_FLETES_IMPUESTOS_LOC ,
            A.IND_VENTA_DESCUENTOS_LOC  = P.IND_VENTA_DESCUENTOS_LOC ,
            A.IND_VENTA_PROMOCIONES_LOC  = P.IND_VENTA_PROMOCIONES_LOC ,
            A.IND_VENTA_REBATES_LOC  = P.IND_VENTA_REBATES_LOC ,
            A.IND_VENTA_CORRECTIVAS_LOC  = P.IND_VENTA_CORRECTIVAS_LOC ,
            A.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC  = P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC ,
            A.IND_VENTA_IMPUESTOS_LOC  = P.IND_VENTA_IMPUESTOS_LOC ,
            A.IND_COSTOSINTERNOS_LOC  = P.IND_COSTOSINTERNOS_LOC ,
            A.MON_LOC  = P.MON_LOC ,
            A.IND_VENTA_PRECIOLISTA_USD  = P.IND_VENTA_PRECIOLISTA_USD ,
            A.IND_VENTA_PRECIOTEORICO_USD  = P.IND_VENTA_PRECIOTEORICO_USD ,
            A.IND_VENTA_PRECIOFACTURA_USD  = P.IND_VENTA_PRECIOFACTURA_USD ,
            A.IND_VENTA_PRECIONETO_USD  = P.IND_VENTA_PRECIONETO_USD ,
            A.IND_VENTA_REAL_USD  = P.IND_VENTA_REAL_USD ,
            A.IND_VENTA_PRECIOFACTURA_IMPUESTO_USD  = P.IND_VENTA_PRECIOFACTURA_IMPUESTO_USD ,
            A.IND_VENTA_REAL_IMPUESTO_USD  = P.IND_VENTA_REAL_IMPUESTO_USD ,
            A.IND_VENTA_PRECIOCONSTRUCTORA_USD  = P.IND_VENTA_PRECIOCONSTRUCTORA_USD ,
            A.IND_VENTA_FLETES_USD  = P.IND_VENTA_FLETES_USD ,
            A.IND_VENTA_REAL_FLETES_USD  = P.IND_VENTA_REAL_FLETES_USD ,
            A.IND_VENTA_REAL_FLETES_IMPUESTOS_USD  = P.IND_VENTA_REAL_FLETES_IMPUESTOS_USD ,
            A.IND_VENTA_DESCUENTOS_USD  = P.IND_VENTA_DESCUENTOS_USD ,
            A.IND_VENTA_PROMOCIONES_USD  = P.IND_VENTA_PROMOCIONES_USD ,
            A.IND_VENTA_REBATES_USD  = P.IND_VENTA_REBATES_USD ,
            A.IND_VENTA_CORRECTIVAS_USD  = P.IND_VENTA_CORRECTIVAS_USD ,
            A.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD  = P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD ,
            A.IND_VENTA_IMPUESTOS_USD  = P.IND_VENTA_IMPUESTOS_USD ,
            A.IND_COSTOSINTERNOS_USD  = P.IND_COSTOSINTERNOS_USD ,
            A.MON_USD  = P.MON_USD ,
            A.IND_VENTA_PRECIOLISTA_MXN  = P.IND_VENTA_PRECIOLISTA_MXN ,
            A.IND_VENTA_PRECIOTEORICO_MXN  = P.IND_VENTA_PRECIOTEORICO_MXN ,
            A.IND_VENTA_PRECIOFACTURA_MXN  = P.IND_VENTA_PRECIOFACTURA_MXN ,
            A.IND_VENTA_PRECIONETO_MXN  = P.IND_VENTA_PRECIONETO_MXN ,
            A.IND_VENTA_REAL_MXN  = P.IND_VENTA_REAL_MXN ,
            A.IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN  = P.IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN ,
            A.IND_VENTA_REAL_IMPUESTO_MXN  = P.IND_VENTA_REAL_IMPUESTO_MXN ,
            A.IND_VENTA_PRECIOCONSTRUCTORA_MXN  = P.IND_VENTA_PRECIOCONSTRUCTORA_MXN ,
            A.IND_VENTA_FLETES_MXN  = P.IND_VENTA_FLETES_MXN ,
            A.IND_VENTA_REAL_FLETES_MXN  = P.IND_VENTA_REAL_FLETES_MXN ,
            A.IND_VENTA_REAL_FLETES_IMPUESTOS_MXN  = P.IND_VENTA_REAL_FLETES_IMPUESTOS_MXN ,
            A.IND_VENTA_DESCUENTOS_MXN  = P.IND_VENTA_DESCUENTOS_MXN ,
            A.IND_VENTA_PROMOCIONES_MXN  = P.IND_VENTA_PROMOCIONES_MXN ,
            A.IND_VENTA_REBATES_MXN  = P.IND_VENTA_REBATES_MXN ,
            A.IND_VENTA_CORRECTIVAS_MXN  = P.IND_VENTA_CORRECTIVAS_MXN ,
            A.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN  = P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN ,
            A.IND_VENTA_IMPUESTOS_MXN  = P.IND_VENTA_IMPUESTOS_MXN ,
            A.IND_COSTOSINTERNOS_MXN  = P.IND_COSTOSINTERNOS_MXN ,
            A.MON_MXN  = P.MON_MXN ,
            A.IND_PESO_BRUTO  = P.IND_PESO_BRUTO ,
            A.IND_PESO_NETO  = P.IND_PESO_NETO ,
            A.UNI_PESO  = P.UNI_PESO ,
            A.SISORIGEN_ID  = P.SISORIGEN_ID ,
            A.MANDANTE = P.MANDANTE,
            A.FECHA_CARGA  = P.FECHA_CARGA ,
            A.ZONA_HORARIA = P.ZONA_HORARIA
        WHEN NOT MATCHED THEN INSERT
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
            IND_VENTA_REAL_M2, 
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
        VALUES(
            P.ANIO,
            P.MES, 
            P.ANIOMES, 
            P.SOCIEDAD_ID, 
            P.ORGVENTAS_ID, 
            P.CANALDISTRIB_ID, 
            P.SECTOR_ID, 
            P.CENTRO_ID, 
            P.ALMACENCENTRO_ID, 
            P.ALMACEN_ID, 
            P.OFICINAVENTAS_ID, 
            P.ZONAVENTAS_ID, 
            P.GRUPOVENDEDORES_ID, 
            P.PAIS_ID, 
            P.REGION_ID, 
            P.ZONATRANSPORTE_ID, 
            P.TIPO_TRANSACCION, 
            P.FACTURA, 
            P.FACTURA_POS, 
            P.FECHA_FACTURA, 
            P.CLASEFACTURA_ID, 
            P.CLASEFACTURA_ORIGEN_ID, 
            P.CLASEFACTURA_ASOCIADO_ID, 
            P.INDICADOR_ANULACION, 
            P.STATUSCONTABILIDAD, 
            P.CONDICIONEXP_ID, 
            P.INCOTERMS_ID, 
            P.ASESORFACTURA_ID, 
            P.TIPOPOSICION_ID, 
            P.PEDIDO, 
            P.PEDIDO_POS, 
            P.ORDENCOMPRA, 
            P.MOTIVOPEDIDO_ID, 
            P.ASESORPEDIDO_ID, 
            P.TIENDARECIBO_ID, 
            P.CONSTRUCTORA_ID, 
            P.TIPO_OBRA_ID, 
            P.CONVENIO_OBRA, 
            P.PLAN_OBRA, 
            P.SEGMENTO_OBRA_ID, 
            P.PROMOTOR_ID, 
            P.NIO_OBRA, 
            P.NUM_TRANSPORTE, 
            P.CLASETRANSPORTE_ID, 
            P.TIPOTRANSPORTE_ID, 
            P.CLASEEXPEDICION_ID, 
            P.PUESTOPLANTRANSP_ID, 
            P.TRANSPORTISTA_ID, 
            P.RUTA_ID, 
            P.TIPOVEHICULO_ID, 
            P.CHOFER, 
            P.FECHA_DESPACHOEXPREAL, 
            P.CLIENTE_ID, 
            P.SOLICITANTE_ID, 
            P.DESTINATARIO_ID, 
            P.DEUDOR_ID, 
            P.MATERIAL_ID, 
            P.MATERIALVENTAS_ID, 
            P.MATERIALCENTRO_ID, 
            P.LOTE, 
            P.LOTECENTRO_ID, 
            P.GRUPOARTICULOS_ID, 
            P.TIPOMATERIAL_ID, 
            P.IND_VENTA_REAL_M2, 
            P.UNI_EST, 
            P.IND_VENTA_REAL_UMV, 
            P.UNI_UMV, 
            P.IND_VENTA_PRECIOLISTA_LOC, 
            P.IND_VENTA_PRECIOTEORICO_LOC, 
            P.IND_VENTA_PRECIOFACTURA_LOC, 
            P.IND_VENTA_PRECIONETO_LOC, 
            P.IND_VENTA_REAL_LOC, 
            P.IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC, 
            P.IND_VENTA_REAL_IMPUESTO_LOC, 
            P.IND_VENTA_PRECIOCONSTRUCTORA_LOC, 
            P.IND_VENTA_FLETES_LOC, 
            P.IND_VENTA_REAL_FLETES_LOC, 
            P.IND_VENTA_REAL_FLETES_IMPUESTOS_LOC, 
            P.IND_VENTA_DESCUENTOS_LOC, 
            P.IND_VENTA_PROMOCIONES_LOC, 
            P.IND_VENTA_REBATES_LOC, 
            P.IND_VENTA_CORRECTIVAS_LOC, 
            P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC, 
            P.IND_VENTA_IMPUESTOS_LOC, 
            P.IND_COSTOSINTERNOS_LOC, 
            P.MON_LOC, 
            P.IND_VENTA_PRECIOLISTA_USD, 
            P.IND_VENTA_PRECIOTEORICO_USD, 
            P.IND_VENTA_PRECIOFACTURA_USD, 
            P.IND_VENTA_PRECIONETO_USD, 
            P.IND_VENTA_REAL_USD, 
            P.IND_VENTA_PRECIOFACTURA_IMPUESTO_USD, 
            P.IND_VENTA_REAL_IMPUESTO_USD, 
            P.IND_VENTA_PRECIOCONSTRUCTORA_USD, 
            P.IND_VENTA_FLETES_USD, 
            P.IND_VENTA_REAL_FLETES_USD, 
            P.IND_VENTA_REAL_FLETES_IMPUESTOS_USD, 
            P.IND_VENTA_DESCUENTOS_USD, 
            P.IND_VENTA_PROMOCIONES_USD, 
            P.IND_VENTA_REBATES_USD, 
            P.IND_VENTA_CORRECTIVAS_USD, 
            P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD, 
            P.IND_VENTA_IMPUESTOS_USD, 
            P.IND_COSTOSINTERNOS_USD, 
            P.MON_USD, 
            P.IND_VENTA_PRECIOLISTA_MXN, 
            P.IND_VENTA_PRECIOTEORICO_MXN, 
            P.IND_VENTA_PRECIOFACTURA_MXN, 
            P.IND_VENTA_PRECIONETO_MXN, 
            P.IND_VENTA_REAL_MXN, 
            P.IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN, 
            P.IND_VENTA_REAL_IMPUESTO_MXN, 
            P.IND_VENTA_PRECIOCONSTRUCTORA_MXN, 
            P.IND_VENTA_FLETES_MXN, 
            P.IND_VENTA_REAL_FLETES_MXN, 
            P.IND_VENTA_REAL_FLETES_IMPUESTOS_MXN, 
            P.IND_VENTA_DESCUENTOS_MXN, 
            P.IND_VENTA_PROMOCIONES_MXN, 
            P.IND_VENTA_REBATES_MXN, 
            P.IND_VENTA_CORRECTIVAS_MXN, 
            P.IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN, 
            P.IND_VENTA_IMPUESTOS_MXN, 
            P.IND_COSTOSINTERNOS_MXN, 
            P.MON_MXN, 
            P.IND_PESO_BRUTO, 
            P.IND_PESO_NETO, 
            P.UNI_PESO, 
            P.SISORIGEN_ID, 
            P.MANDANTE,
            P.FECHA_CARGA, 
            P.ZONA_HORARIA
        );


        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED 
        FROM 
            CON.FCT_COM_REV_VENTAS_ACT
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
    VALUES ('SP_CON_FCT_COM_REV_VENTAS_ACT','FCT_COM_REV_VENTAS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 