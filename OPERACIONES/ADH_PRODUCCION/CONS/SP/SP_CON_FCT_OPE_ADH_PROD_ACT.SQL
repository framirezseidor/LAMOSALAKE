CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_OPE_ADH_PROD_ACT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-08
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa PRE a CON para FCT_OPE_ADH_PROD_ACT
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
/* 
        DELETE FROM CON.FCT_OPE_ADH_PROD_ACT
        WHERE xxx IN (SELECT DISTINCT MBLNR FROM RAW.SQ1_EXT_2LIS_03_BF 
        WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) ------------ULTIMOS REGISTROS--------
                       FROM RAW.SQ1_EXT_2LIS_03_BF));
*/
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

        INSERT INTO CON.FCT_OPE_ADH_PROD_ACT
        (
            -- Lista de columnas destino en tabla CON
            -- Ejemplo: ID, FECHA, CLIENTE_ID, MONTO
        ANIO ,
        MES,
        ANIOMES ,
        FECHA_CONTABILIZACION,
        --DIA,
        SOCIEDAD_ID,
        CENTRO_ID, 
        ALMACEN_ID,
        ALMACENCENTRO_ID,
        --CENTROCOSTO_ID , 
        --SOCIEDADCO_ID,
        MATERIALCENTRO_ID,
        MATERIAL_ID ,
        LOTE ,
        MATERIAL_ENTREGADO_ID ,
        TEXTO_CABECERA_DOCUMENTO,
        --DOCMATERIAL,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO_ID,
        ORDENFABRICACION_ID ,
        PUESTOTRABAJO_ID,
        CLIENTE_ID,
        MON_LOC,
        IND_IMPTE_ML,
        IND_IMPTE_COMPRA_ML,
        TIPOCAMBIO_USD,
        MON_USD,
        IND_IMPTE_USD,
        IND_IMPTE_COMPRA_USD,
        IND_CANTIDAD_UMB,
        IND_CANTIDAD_UME,
        IND_CONSUMO_UMB,
        IND_CONSUMO_UME,
        IND_PRODENTREGADA_UMB,
        IND_PRODENTREGADA_UME,
        IND_PRODTERMINADA_UMB,
        IND_PRODTERMINADA_UME,
        IND_EMBARCADO_UMB,
        IND_EMBARCADO_UME,
        IND_AJUSTEINVENTARIO_UMB,
        IND_AJUSTEINVENTARIO_UME,
        UNI_UMB,
        UNI_EST,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde PRE
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
            ANIO ,
            MES,
            ANIOMES ,
            FECHA_CONTABILIZACION,
            --DIA,
            SOCIEDAD_ID,
            CENTRO_ID, 
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            --CENTROCOSTO_ID , 
            --SOCIEDADCO_ID,
            MATERIALCENTRO_ID,
            MATERIAL_ID ,
            LOTE ,
            MATERIAL_ENTREGADO_ID,
            TEXTO_CABECERA_DOCUMENTO,
            --DOCMATERIAL,
            CLASEORDFAB_ID,
            CLASEMOVIMIENTO_ID,
            ORDENFABRICACION_ID ,
            PUESTOTRABAJO_ID,
            CLIENTE_ID,
            MON_LOC,
            IND_IMPTE_ML,
            IND_IMPTE_COMPRA_ML,
            TIPOCAMBIO_USD,
            MON_USD,
            IND_IMPTE_USD,
            IND_IMPTE_COMPRA_USD,
            IND_CANTIDAD_UMB,
            IND_CANTIDAD_UME,
            IND_CONSUMO_UMB,
            IND_CONSUMO_UME,
            IND_PRODENTREGADA_UMB,
            IND_PRODENTREGADA_UME,
            IND_PRODTERMINADA_UMB,
            IND_PRODTERMINADA_UME,
            IND_EMBARCADO_UMB,
            IND_EMBARCADO_UME,
            IND_AJUSTEINVENTARIO_UMB,
            IND_AJUSTEINVENTARIO_UME,
            UNI_UMB,
            UNI_EST,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA::TIMESTAMP_NTZ AS FECHA_CARGA,
            ZONA_HORARIA
        FROM PRE.PFCT_OPE_ADH_PROD;
        --WHERE FECHA_CARGA:DATE = CURRENT_DATE;
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_OPE_ADH_PROD_ACT;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOGS
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_FCT_OPE_ADH_PROD_ACT','CON.FCT_OPE_ADH_PROD_ACT', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );
    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------    
    DELETE FROM MIRRORING.FCT_OPE_ADH_PRODUCCION;

    --INSERT INTO MIRRORING.FCT_OPE_ADH_PROD_ACT
    INSERT INTO MIRRORING.FCT_OPE_ADH_PRODUCCION
    (
        ANIO ,
        MES,
        ANIOMES ,
        FECHA_CONTABILIZACION,
        SOCIEDAD_ID,
        CENTRO_ID, 
        ALMACEN_ID,
        ALMACENCENTRO_ID,
        MATERIALCENTRO_ID,
        MATERIAL_ID ,
        MATERIAL_ENTREGADO_ID ,
        TEXTO_CABECERA_DOCUMENTO,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO_ID,
        ORDENFABRICACION_ID ,
        PUESTOTRABAJO_ID,
        CLIENTE_ID,
        MON_LOC,
        IND_IMPTE_ML,
        IND_IMPTE_COMPRA_ML,
        TIPOCAMBIO_USD,
        MON_USD,
        IND_IMPTE_USD,
        IND_IMPTE_COMPRA_USD,
        IND_CANTIDAD_UMB,
        IND_CANTIDAD_UME,
        IND_CONSUMO_UMB,
        IND_CONSUMO_UME,
        IND_PRODENTREGADA_UMB,
        IND_PRODENTREGADA_UME,
        IND_PRODTERMINADA_UMB,
        IND_PRODTERMINADA_UME,
        IND_EMBARCADO_UMB,
        IND_EMBARCADO_UME,
        IND_AJUSTEINVENTARIO_UMB,
        IND_AJUSTEINVENTARIO_UME,
        UNI_UMB,
        UNI_EST,
        MODELO,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA    
    )
    SELECT
            ANIO ,
            MES,
            ANIOMES ,
            FECHA_CONTABILIZACION,
            SOCIEDAD_ID,
            CENTRO_ID, 
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            MATERIALCENTRO_ID,
            MATERIAL_ID ,
            MATERIAL_ENTREGADO_ID,
            TEXTO_CABECERA_DOCUMENTO,
            CLASEORDFAB_ID,
            CLASEMOVIMIENTO_ID,
            ORDENFABRICACION_ID ,
            PUESTOTRABAJO_ID,
            CLIENTE_ID,
            MON_LOC,
            IND_IMPTE_ML,
            IND_IMPTE_COMPRA_ML,
            TIPOCAMBIO_USD,
            MON_USD,
            IND_IMPTE_USD,
            IND_IMPTE_COMPRA_USD,
            IND_CANTIDAD_UMB,
            IND_CANTIDAD_UME,
            IND_CONSUMO_UMB,
            IND_CONSUMO_UME,
            IND_PRODENTREGADA_UMB,
            IND_PRODENTREGADA_UME,
            IND_PRODTERMINADA_UMB,
            IND_PRODTERMINADA_UME,
            IND_EMBARCADO_UMB,
            IND_EMBARCADO_UME,
            IND_AJUSTEINVENTARIO_UMB,
            IND_AJUSTEINVENTARIO_UME,
            UNI_UMB,
            UNI_EST,
            'PRODUCCION' as MODELO,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA ,
            ZONA_HORARIA
        FROM CON.FCT_OPE_ADH_PROD_ACT
        WHERE FECHA_CARGA = (SELECT MAX(FECHA_CARGA) FROM CON.FCT_OPE_ADH_PROD_ACT );

        --CREATE OR REPLACE STREAM MIRRORING.STREAM_FCT_OPE_ADH_PROD_ACT ON TABLE MIRRORING.FCT_OPE_ADH_PROD_ACT;
        CREATE OR REPLACE STREAM MIRRORING.STREAM_FCT_OPE_ADH_PRODUCCION ON TABLE MIRRORING.FCT_OPE_ADH_PRODUCCION;
        
        --Actualización tabla calendario
        CALL MIRRORING.SP_ACTUALIZAR_CALENDARIO('FCT_OPE_ADH_PRODUCCION', 'DIM_CAL_ADH_PRODUCCION', 'FECHA_CONTABILIZACION');
 
    ---------------------------------------------------------------------------------
    -- STEP FCT APIC
    ---------------------------------------------------------------------------------
        CALL CON.SP_CON_FCT_OPE_ADH_PROD_FCT_APIC();
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 