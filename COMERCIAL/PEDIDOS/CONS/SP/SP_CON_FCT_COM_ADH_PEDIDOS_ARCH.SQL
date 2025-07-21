CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_ADH_PEDIDOS_ARCH()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Fidel Ramírez / Agustin Gutierrez
 Descripción:        SP carga/actualiza la historia
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
    -- STEP 1: DELETE EN ARCH DE PEDIDOS CON POSICION INEXISTENTE EN ACT
    ---------------------------------------------------------------------------------
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
  BEGIN

    DELETE FROM CON.FCT_COM_ADH_PEDIDOS_ARCH a
    WHERE a.DOCUMENTO IN (
        SELECT DISTINCT p.DOCUMENTO
        FROM PRE.PFCT_COM_PEDIDOS_ACT p
    )
    AND NOT EXISTS (
        SELECT 1
        FROM PRE.PFCT_COM_PEDIDOS_ACT p
        WHERE p.DOCUMENTO = a.DOCUMENTO
        AND p.DOCUMENTO_POS = a.DOCUMENTO_POS
    )
    AND a.FECHA_DOCUMENTO BETWEEN (
        SELECT TO_DATE(R_INICIO, ''YYYYMMDD'')
        FROM RAW.PARAMETROS_EXTRACCION
        WHERE NEGOCIO = ''REVMEXICO''
        AND EXTRACTOR = ''ZBWSD_CAPTURA_DIARIA''
        AND PARAMETRO = ''FECHA_DOCUMENTO''
    )
    AND (
        SELECT TO_DATE(R_FIN, ''YYYYMMDD'')
        FROM RAW.PARAMETROS_EXTRACCION
        WHERE NEGOCIO = ''REVMEXICO''
        AND EXTRACTOR = ''ZBWSD_CAPTURA_DIARIA''
        AND PARAMETRO = ''FECHA_DOCUMENTO''
    );




    ---------------------------------------------------------------------------------
    -- STEP 2: UPSERT
    ---------------------------------------------------------------------------------

        MERGE INTO CON.FCT_COM_ADH_PEDIDOS_ARCH F
        USING PRE.PFCT_COM_PEDIDOS_ACT P
        ON  F.ORGVENTAS_ID = P.ORGVENTAS_ID AND
            F.CANALDISTRIB_ID = P.CANALDISTRIB_ID AND
            F.SECTOR_ID = P.DIVISION AND
            F.TIPODOCUMENTO = P.TIPODOCUMENTO AND
            F.FECHA_DOCUMENTO = P.FECHA_DOCUMENTO AND
            F.CLASEDOC_CONSOLIDADO = P.CLASEDOC_CONSOLIDADO AND
            F.DOCUMENTO = P.DOCUMENTO AND
            F.DOCUMENTO_POS = P.DOCUMENTO_POS
        WHEN MATCHED THEN 
            UPDATE SET 
                    F.ANIO = P.ANIO,
                    F.MES = P.MES,
                    F.ANIOMES = P.ANIOMES,
                    F.SOCIEDAD_ID = P.SOCIEDAD_ID,
                    F.CENTRO_ID = P.CENTRO_ID,
                    F.OFICINAVENTAS_ID = P.OFICINAVENTAS_ID,
                    F.ZONAVENTAS_ID = P.ZONAVENTAS_ID,
                    F.GRUPOVENDEDORES_ID = P.GRUPOVENDEDORES_ID,
                    F.PAIS_ID = P.PAIS_ID,
                    F.REGION_ID = P.REGION_ID,
                    F.ZONATRANSPORTE_ID = P.ZONATRANSPORTE_ID,
                    F.TIPOPOSICION_ID = P.TIPOPOSICION_ID,
                    F.INCOTERMS_ID = P.INCOTERMS_ID,
                    F.ASESORPEDIDO_ID = P.ASESORPEDIDO_ID,
                    F.CONSTRUCTORA_ID = P.CONSTRUCTORA_ID,
                    F.TIPO_OBRA_ID = P.TIPO_OBRA_ID,
                    F.CONVENIO_OBRA = P.CONVENIO_OBRA,
                    F.PLAN_OBRA = P.PLAN_OBRA,
                    F.SEGMENTO_OBRA_ID = P.SEGMENTO_OBRA_ID,
                    F.PROMOTOR_ID = P.PROMOTOR_ID,
                    F.NIO_OBRA = P.NIO_OBRA,
                    F.TIPOTRANSPORTE_ID = P.TIPOTRANSPORTE_ID,
                    F.CLIENTE_ID = P.CLIENTE_ID,
                    F.SOLICITANTE_ID = P.SOLICITANTE_ID,
                    F.DESTINATARIO_ID = P.DESTINATARIO_ID,
                    F.MATERIAL_ID = P.MATERIAL_ID,
                    F.MATERIALVENTAS_ID = P.MATERIALVENTAS_ID,
                    F.MATERIALCENTRO_ID = P.MATERIALCENTRO_ID,
                    F.IND_CANT_PEDIDO_TON = P.IND_CANT_PEDIDO_EST,
                    F.IND_CANT_CONFIRMADA_TON = P.IND_CANT_CONFIRMADA_EST,
                    F.IND_CANT_ENTREGADA_TON = P.IND_CANT_ENTREGADA_EST,
                    F.IND_CANT_FACTURADA_TON = P.IND_CANT_FACTURADA_EST,
                    F.UNI_EST = P.UNI_EST,
                    F.IND_CANT_PEDIDO_UMV = P.IND_CANT_PEDIDO_UMV,
                    F.IND_CANT_CONFIRMADA_UMV = P.IND_CANT_CONFIRMADA_UMV,
                    F.IND_CANT_ENTREGADA_UMV = P.IND_CANT_ENTREGADA_UMV,
                    F.IND_CANT_FACTURADA_UMV = P.IND_CANT_FACTURADA_UMV,
                    F.UNI_UMV = P.UNI_UMV,
                    F.SISORIGEN_ID = P.SISORIGEN_ID,
                    F.MANDANTE = P.MANDANTE,
                    F.FECHA_CARGA = P.FECHA_CARGA,
                    F.ZONA_HORARIA = P.ZONA_HORARIA
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
                    OFICINAVENTAS_ID,
                    ZONAVENTAS_ID,
                    GRUPOVENDEDORES_ID,
                    UENADHESIVOS_ID,
                    PAIS_ID,
                    REGION_ID,
                    ZONATRANSPORTE_ID,
                    TIPODOCUMENTO,
                    FECHA_DOCUMENTO,
                    CLASEDOC_CONSOLIDADO,
                    DOCUMENTO,
                    DOCUMENTO_POS,
                    TIPOPOSICION_ID,
                    MOTIVOPEDIDO_ID,
                    INCOTERMS_ID,
                    ASESORPEDIDO_ID,
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
                    IND_CANT_PEDIDO_TON,
                    IND_CANT_CONFIRMADA_TON,
                    IND_CANT_ENTREGADA_TON,
                    IND_CANT_FACTURADA_TON,
                    UNI_EST,
                    IND_CANT_PEDIDO_UMV,
                    IND_CANT_CONFIRMADA_UMV,
                    IND_CANT_ENTREGADA_UMV,
                    IND_CANT_FACTURADA_UMV,
                    UNI_UMV,
                    SISORIGEN_ID,
                    MANDANTE,
                    FECHA_CARGA,
                    ZONA_HORARIA
                )
                VALUES (
                    P.ANIO,
                    P.MES,
                    P.ANIOMES,
                    P.SOCIEDAD_ID,
                    P.ORGVENTAS_ID,
                    P.CANALDISTRIB_ID,
                    P.DIVISION,
                    P.CENTRO_ID,
                    P.OFICINAVENTAS_ID,
                    P.ZONAVENTAS_ID,
                    P.GRUPOVENDEDORES_ID,
                    P.UENADHESIVOS_ID,
                    P.PAIS_ID,
                    P.REGION_ID,
                    P.ZONATRANSPORTE_ID,
                    P.TIPODOCUMENTO,
                    P.FECHA_DOCUMENTO,
                    P.CLASEDOC_CONSOLIDADO,
                    P.DOCUMENTO,
                    P.DOCUMENTO_POS,
                    P.TIPOPOSICION_ID,
                    P.MOTIVOPEDIDO_ID,
                    P.INCOTERMS_ID,
                    P.ASESORPEDIDO_ID,
                    P.CONSTRUCTORA_ID,
                    P.TIPO_OBRA_ID,
                    P.CONVENIO_OBRA,
                    P.PLAN_OBRA,
                    P.SEGMENTO_OBRA_ID,
                    P.PROMOTOR_ID,
                    P.NIO_OBRA,
                    P.TIPOTRANSPORTE_ID,
                    P.CLIENTE_ID,
                    P.SOLICITANTE_ID,
                    P.DESTINATARIO_ID,
                    P.MATERIAL_ID,
                    P.MATERIALVENTAS_ID,
                    P.MATERIALCENTRO_ID,
                    P.IND_CANT_PEDIDO_EST,
                    P.IND_CANT_CONFIRMADA_EST,
                    P.IND_CANT_ENTREGADA_EST,
                    P.IND_CANT_FACTURADA_EST,
                    P.UNI_EST,
                    P.IND_CANT_PEDIDO_UMV,
                    P.IND_CANT_CONFIRMADA_UMV,
                    P.IND_CANT_ENTREGADA_UMV,
                    P.IND_CANT_FACTURADA_UMV,
                    P.UNI_UMV,
                    P.SISORIGEN_ID,
                    P.MANDANTE,
                    P.FECHA_CARGA,
                    P.ZONA_HORARIA
                );


        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_COM_ADH_PEDIDOS_ARCH;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT (''Error en INSERT: '' || :sqlerrm) INTO :TEXTO;
    END;


    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
        SELECT COALESCE(:TEXTO,''EJECUCION CORRECTA'') INTO :TEXTO;

        INSERT INTO LOGS.HISTORIAL_EJECUCIONES 
        VALUES(''CON.SP_CON_FCT_COM_ADH_PEDIDOS_ARCH'',''CON.FCT_COM_ADH_PEDIDOS_ARCH'', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT(''Complete - Filas insertadas: '', ROWS_INSERTED);

END;
';