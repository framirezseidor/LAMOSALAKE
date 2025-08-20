CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_ADH_BACKORDER(
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
    TEXTO           VARCHAR(200);
    TOTAL_FILAS NUMBER(38,0);
    PARAMETRO       VARCHAR(20);
    SOCIEDAD_INICIO   VARCHAR(20);
    SOCIEDAD_FIN      VARCHAR(20);
    CANAL_INICIO   VARCHAR(20);
    CANAL_FIN      VARCHAR(20);
    UNI_EST_INICIO   VARCHAR(20);
    UNI_EST_FIN      VARCHAR(20);
    UNI_EST_INICIO_HOMOLOGADO VARCHAR(20);
    UNI_EST_FIN_HOMOLOGADO VARCHAR(20);


BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE SOCIEDADES
    ---------------------------------------------------------------------------------
    PARAMETRO := 'VKORG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :SOCIEDAD_INICIO ;

    SOCIEDAD_FIN := SPLIT_PART(SOCIEDAD_INICIO,'-',2);
    SOCIEDAD_INICIO := SPLIT_PART(SOCIEDAD_INICIO,'-',1);    

    PARAMETRO := 'VTWEG';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :CANAL_INICIO ;

    CANAL_FIN := SPLIT_PART(CANAL_INICIO,'-',2);
    CANAL_INICIO := SPLIT_PART(CANAL_INICIO,'-',1); 

    PARAMETRO := 'VRKME';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :UNI_EST_INICIO ;

    UNI_EST_FIN := SPLIT_PART(UNI_EST_INICIO,'-',2);
    UNI_EST_INICIO := SPLIT_PART(UNI_EST_INICIO,'-',1); 

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
    --     RETURN 'Soc ini: ' || SOCIEDAD_INICIO || ', Soc fin: ' || SOCIEDAD_FIN || 
    --    ', Canal ini: ' || UNI_EST_INICIO || ', Canal fin: ' || UNI_EST_FIN;
        DELETE FROM CON.FCT_COM_ADH_BACKORDER_ACT
        WHERE
            SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
            AND CANALDISTRIB_ID BETWEEN :CANAL_INICIO AND :CANAL_FIN
            AND UNI_EST BETWEEN 
            COALESCE((
                SELECT VALOR_ESTANDARD FROM RAW.CAT_UNIDAD_ESTADISTICA 
                WHERE UPPER(TRIM(VALOR_ORIGINAL)) = UPPER(TRIM(:UNI_EST_INICIO))
            ), :UNI_EST_INICIO)
            AND
            COALESCE((
                SELECT VALOR_ESTANDARD FROM RAW.CAT_UNIDAD_ESTADISTICA 
                WHERE UPPER(TRIM(VALOR_ORIGINAL)) = UPPER(TRIM(:UNI_EST_FIN))
            ), :UNI_EST_FIN);

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

        INSERT INTO CON.FCT_COM_ADH_BACKORDER_ACT (
            FECHA_BACKORDER,
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
            UENADHESIVOS_ID,
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
            -- NEGOCIO,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            TO_VARCHAR(CURRENT_DATE) AS FECHA_BACKORDER,
            TO_VARCHAR(CURRENT_DATE, 'YYYY')       AS ANIO,
            TO_VARCHAR(CURRENT_DATE, 'MM')         AS MES,
            TO_VARCHAR(CURRENT_DATE, 'YYYYMM')     AS ANIOMES,
            SOCIEDAD_ID,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            CENTRO_ID,
            OFICINAVENTAS_ID,
            ZONAVENTAS_ID,
            GRUPOVENDEDORES_ID,
            UENADHESIVOS_ID,
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
            --LOTE,
            TIPOMATERIAL_ID,
            IND_BO_TOTAL_EST,
            IND_BO_ENTREGA_EST,
            IND_BO_CONFIRMADO_EST,
            IND_BO_NO_CONFIRMADO_EST,
            COALESCE(U.VALOR_ESTANDARD, T.UNI_EST),
            IND_BO_MONEDA_DOCUMENTO,
            MON_DOC,
            -- NEGOCIO,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PFCT_COM_BACKORDER T
        LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U
            ON UPPER(TRIM(T.UNI_EST)) = UPPER(TRIM(U.VALOR_ORIGINAL))
        WHERE ORGVENTAS_ID LIKE 'A%'
            AND SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
            AND CANALDISTRIB_ID BETWEEN :CANAL_INICIO AND :CANAL_FIN
            AND UNI_EST BETWEEN :UNI_EST_INICIO AND :UNI_EST_FIN
            AND TIPOMATERIAL_ID IN ('ZHAW', 'FERT');

        -- Conteo con valores homologados
        SELECT COUNT(*) 
        INTO :ROWS_INSERTED
        FROM CON.FCT_COM_ADH_BACKORDER_ACT
        WHERE ORGVENTAS_ID LIKE 'A%'
        AND SOCIEDAD_ID BETWEEN :SOCIEDAD_INICIO AND :SOCIEDAD_FIN  
        AND CANALDISTRIB_ID BETWEEN :CANAL_INICIO AND :CANAL_FIN
        AND UNI_EST BETWEEN 
        COALESCE((
            SELECT VALOR_ESTANDARD FROM RAW.CAT_UNIDAD_ESTADISTICA 
            WHERE UPPER(TRIM(VALOR_ORIGINAL)) = UPPER(TRIM(:UNI_EST_INICIO))
        ), :UNI_EST_INICIO)
        AND
        COALESCE((
            SELECT VALOR_ESTANDARD FROM RAW.CAT_UNIDAD_ESTADISTICA 
            WHERE UPPER(TRIM(VALOR_ORIGINAL)) = UPPER(TRIM(:UNI_EST_FIN))
        ), :UNI_EST_FIN);

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
        VALUES('SP_CON_FCT_ADH_BACKORDER','FCT_COM_ADH_BACKORDER_ACT', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );
    
    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    SELECT COUNT(*) INTO TOTAL_FILAS FROM PRE.PFCT_COM_BACKORDER;
    RETURN CASE
        WHEN TEXTO IS NULL OR TEXTO = 'EJECUCION CORRECTA' THEN 
            CONCAT('✅ EJECUCION CORRECTA - Filas insertadas: ', ROWS_INSERTED, 
                ', Total en tabla PRE: ', TOTAL_FILAS)
        
        WHEN ROWS_INSERTED IS NULL THEN 
            '❌ Error: El SP no insertó ni detectó filas'

        ELSE 
            CONCAT('⚠️ Finalizado con advertencia: ', TEXTO)
    END;

END;
$$;
 