CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_REV_BACKORDER()
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
 Descripción:        SP que transforma datos desde la capa PRE A CON para FCT_COM_REV_BACKORDER_ACT​
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM CON.FCT_COM_REV_BACKORDER_ACT;

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

        INSERT INTO CON.FCT_COM_REV_BACKORDER_ACT (
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
            LOTE,
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_M2,
            IND_BO_ENTREGA_M2,
            IND_BO_CONFIRMADO_M2,
            IND_BO_NO_CONFIRMADO_M2,
            UNI_EST,
            IND_BO_IMPORTE_DOC,
            MON_DOC,
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
            OFICINAVENTAS_ID,
            ZONAVENTAS_ID,
            GRUPOVENDEDORES_ID,
            --UEN_ADHESIVOS_ID,
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
            LOTE,
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_EST,
            IND_BO_ENTREGA_EST,
            IND_BO_CONFIRMADO_EST,
            IND_BO_NO_CONFIRMADO_EST,
            UNI_EST,
            IND_BO_MONEDA_DOCUMENTO,
            MON_DOC,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PFCT_COM_BACKORDER
        WHERE ORGVENTAS_ID LIKE 'R%';

        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_COM_REV_BACKORDER_ACT;

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
        VALUES('SP_CON_FCT_REV_BACKORDER','FCT_COM_REV_BACKORDER_ACT', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );
    
    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
