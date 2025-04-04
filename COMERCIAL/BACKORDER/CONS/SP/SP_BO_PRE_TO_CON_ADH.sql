CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.CON.SP_CON_FCT_ADH_BACKORDER()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Fernando Cuellar
 Descripción:        SP que transforma datos desde la capa PRE A CON para FCT_COM_ADH_BACKORDER_ACT​
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM LAMOSALAKE_DEV.CON.FCT_COM_ADH_BACKORDER_ACT;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en DELETE: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO LAMOSALAKE_DEV.CON.FCT_COM_ADH_BACKORDER_ACT (
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
            ASESORPEDIDO_ID,
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
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_TON,
            IND_BO_ENTREGA_TON,
            IND_BO_CONFIRMADO_TON,
            IND_BO_NO_CONFIRMADO_TON,
            UNI_EST,
            IND_BO_IMPORTE_DOC,
            MON_DOC,
            ID_SISORIGEN,
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
        FROM LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER
        WHERE ORGVENTAS_ID LIKE 'A%';

        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.CON.FCT_COM_ADH_BACKORDER_ACT;

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
