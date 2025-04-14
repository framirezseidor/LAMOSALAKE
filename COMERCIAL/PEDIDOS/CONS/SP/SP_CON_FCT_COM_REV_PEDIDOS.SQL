
CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_REV_PEDIDOS_ACT()
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
 Descripción:        SP carga datos desde pre pedidos a con pedidos
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

        TRUNCATE TABLE CON.FCT_COM_REV_PEDIDOS_ACT;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO CON.FCT_COM_REV_PEDIDOS_ACT
        SELECT 
            ANIO	AS	ANIO	,
            MES	AS	MES	,
            ANIOMES	AS	ANIOMES	,
            SOCIEDAD_ID	AS	SOCIEDAD_ID	,
            ORGVENTAS_ID	AS	ORGVENTAS_ID	,
            CANALDISTRIB_ID	AS	CANALDISTRIB_ID	,
            DIVISION	AS	SECTOR_ID	,
            CENTRO_ID	AS	CENTRO_ID	,
            OFICINAVENTAS_ID	AS	OFICINAVENTAS_ID	,
            ZONAVENTAS_ID	AS	ZONAVENTAS_ID	,
            GRUPOVENDEDORES_ID	AS	GRUPOVENDEDORES_ID	,
            UENADHESIVOS_ID	AS	UENADHESIVOS_ID	,
            PAIS_ID	AS	PAIS_ID	,
            REGION_ID	AS	REGION_ID	,
            ZONATRANSPORTE_ID	AS	ZONATRANSPORTE_ID	,
            TIPODOCUMENTO	AS	TIPODOCUMENTO	,
            FECHA_DOCUMENTO	AS	FECHA_DOCUMENTO	,
            CLASEDOC_CONSOLIDADO	AS	CLASEDOC_CONSOLIDADO	,
            DOCUMENTO	AS	DOCUMENTO	,
            DOCUMENTO_POS	AS	DOCUMENTO_POS	,
            TIPOPOSICION_ID	AS	TIPOPOSICION_ID	,
            MOTIVOPEDIDO_ID	AS	MOTIVOPEDIDO_ID	,
            INCOTERMS_ID	AS	INCOTERMS_ID	,
            ASESORPEDIDO_ID	AS	ASESORPEDIDO_ID	,
            CONSTRUCTORA_ID	AS	CONSTRUCTORA_ID	,
            TIPO_OBRA_ID	AS	TIPO_OBRA_ID	,
            CONVENIO_OBRA	AS	CONVENIO_OBRA	,
            PLAN_OBRA	AS	PLAN_OBRA	,
            SEGMENTO_OBRA_ID	AS	SEGMENTO_OBRA_ID	,
            PROMOTOR_ID	AS	PROMOTOR_ID	,
            NIO_OBRA	AS	NIO_OBRA	,
            TIPOTRANSPORTE_ID	AS	TIPOTRANSPORTE_ID	,
            CLIENTE_ID	AS	CLIENTE_ID	,
            SOLICITANTE_ID	AS	SOLICITANTE_ID	,
            DESTINATARIO_ID	AS	DESTINATARIO_ID	,
            MATERIAL_ID	AS	MATERIAL_ID	,
            MATERIALVENTAS_ID	AS	MATERIALVENTAS_ID	,
            MATERIALCENTRO_ID	AS	MATERIALCENTRO_ID	,
            LOTE	AS	LOTE	,
            LOTECENTRO_ID	AS	LOTECENTRO_ID	,
            IND_CANT_PEDIDO_EST	AS	IND_CANT_PEDIDO_M2	,
            IND_CANT_CONFIRMADA_EST	AS	IND_CANT_CONFIRMADA_M2	,
            IND_CANT_ENTREGADA_EST	AS	IND_CANT_ENTREGADA_M2	,
            IND_CANT_FACTURADA_EST	AS	IND_CANT_FACTURADA_M2	,
            UNI_EST	AS	UNI_EST	,
            IND_CANT_PEDIDO_UMV	AS	IND_CANT_PEDIDO_UMV	,
            IND_CANT_CONFIRMADA_UMV	AS	IND_CANT_CONFIRMADA_UMV	,
            IND_CANT_ENTREGADA_UMV	AS	IND_CANT_ENTREGADA_UMV	,
            IND_CANT_FACTURADA_UMV	AS	IND_CANT_FACTURADA_UMV	,
            UNI_UMV	AS	UNI_UMV	,
            SISORIGEN_ID	AS	SISORIGEN_ID	,
            MANDANTE	AS	MANDANTE	,
            FECHA_CARGA	AS	FECHA_CARGA	,
            ZONA_HORARIA	AS	ZONA_HORARIA	
        FROM PRE.PFCT_COM_PEDIDOS_ACT;

        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_COM_REV_PEDIDOS_ACT;

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
        VALUES('SP_CON_FCT_COM_REV_PEDIDOS','PRE.PFCT_COM_PEDIDOS_ACT', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
