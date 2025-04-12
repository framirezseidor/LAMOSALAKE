CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_LOG_ADH_INVENTARIO_FOTO()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-07
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa PRE a CON para FCT_LOG_ADH_INVENTARIO_FOTO
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

        DELETE FROM CON.FCT_LOG_ADH_INVENTARIO_FOTO 
        --restricción que valida que solo borre la fecha que se encuentra en la tabla inventario_act
        WHERE FECHA_FOTO_INVENTARIO IN (SELECT DISTINCT FECHA_FOTO_INVENTARIO FROM CON.FCT_LOG_ADH_INVENTARIO_ACT)
        --restricción que valida que la fecha solo sea de fin de mes
        AND   FECHA_FOTO_INVENTARIO=TO_NUMBER(REPLACE(LAST_DAY(TO_DATE(TO_VARCHAR(FECHA_FOTO_INVENTARIO),'YYYYMMDD')),'-',''));

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

        INSERT INTO CON.FCT_LOG_ADH_INVENTARIO_FOTO
        (
            ANIO,
            MES,
            ANIOMES,
            FECHA_FOTO_INVENTARIO,
            CENTRO_ID,
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            MATERIAL_ID,
            MATERIALCENTRO_ID,
            TIPOMATERIAL_ID,
            CONTROL_PRECIOS,
            STOCKESPECIAL,
            NUMSTOCKESPECIAL,
            LOTE,
            LOTECENTRO_ID,
            IND_LIBRE_UTILIZACION_UMB,
            IND_BLOQUEADO_UMB,
            IND_CONTROL_CALIDAD_UMB,
            IND_TRANSITO_UMB,
            IND_INVENTARIO_FISICO_UMB,
            IND_STOCK_SEGURIDAD_UMB,
            UNI_UMB,
            IND_LIBRE_UTILIZACION_TON,
            IND_BLOQUEADO_TON,
            IND_CONTROL_CALIDAD_TON,
            IND_TRANSITO_TON,
            IND_INVENTARIO_FISICO_TON,
            IND_STOCK_SEGURIDAD_TON,
            UNI_EST,
            IND_COSTO_INV_MXN,
            MON_MXN,
            IND_COSTO_INV_LOC,
            MON_LOC,
            IND_COSTO_INV_USD,
            MON_USD,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            ANIO,
            MES,
            ANIOMES,
            FECHA_FOTO_INVENTARIO,
            CENTRO_ID,
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            MATERIAL_ID,
            MATERIALCENTRO_ID,
            TIPOMATERIAL_ID,
            CONTROL_PRECIOS,
            STOCKESPECIAL,
            NUMSTOCKESPECIAL,
            LOTE,
            LOTECENTRO_ID,
            IND_LIBRE_UTILIZACION_UMB,
            IND_BLOQUEADO_UMB,
            IND_CONTROL_CALIDAD_UMB,
            IND_TRANSITO_UMB,
            IND_INVENTARIO_FISICO_UMB,
            --IND_LIBRE_VENTA_UMB,
            IND_STOCK_SEGURIDAD_UMB,
            UNI_UMB,
            IND_LIBRE_UTILIZACION_TON,
            IND_BLOQUEADO_TON,
            IND_CONTROL_CALIDAD_TON,
            IND_TRANSITO_TON,
            IND_INVENTARIO_FISICO_TON,
            --IND_LIBRE_VENTA_TON,
            IND_STOCK_SEGURIDAD_TON,
            UNI_EST,
            IND_COSTO_INV_MXN,
            MON_MXN,
            IND_COSTO_INV_LOC,
            MON_LOC,
            IND_COSTO_INV_USD,
            MON_USD,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM CON.FCT_LOG_ADH_INVENTARIO_ACT WHERE CENTRO_ID LIKE 'A%'
        --restricción que valida que la fecha solo sea de fin de mes
        AND FECHA_FOTO_INVENTARIO=TO_NUMBER(REPLACE(LAST_DAY(TO_DATE(TO_VARCHAR(FECHA_FOTO_INVENTARIO),'YYYYMMDD')),'-','')) ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_LOG_ADH_INVENTARIO_FOTO;

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
 