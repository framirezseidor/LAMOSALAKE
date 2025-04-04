CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.PRE.SP_PRE_PFCT_LOG_INVENTARIO()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS LAMOSALAKE_DEV_DEVELOPER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_LOG_INVENTARIO
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        --SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM LAMOSALAKE_DEV.PRE.PFCT_LOG_INVENTARIO;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en DELETE: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO LAMOSALAKE_DEV.PRE.PFCT_LOG_INVENTARIO
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
            IND_LIBRE_VENTA_UMB, 
            IND_STOCK_SEGURIDAD_UMB, 
            UNI_UMB, 
            IND_LIBRE_UTILIZACION_EST, 
            IND_BLOQUEADO_EST, 
            IND_CONTROL_CALIDAD_EST, 
            IND_TRANSITO_EST, 
            IND_INVENTARIO_FISICO_EST, 
            IND_LIBRE_VENTA_EST, 
            IND_STOCK_SEGURIDAD_EST, 
            UNI_EST, 
            IND_COSTO_INV_MXN, 
            MON_MXN, 
            IND_COSTO_INV_LOC, 
            MON_LOC, 
            IND_COSTO_INV_USD, 
            MON_USD, 
            ID_SISORIGEN, 
            MANDANTE, 
            FECHA_CARGA, 
            ZONA_HORARIA 
        )
        SELECT
            SUBSTR(FECHA,1,4),-- ANIO,
            SUBSTR(FECHA,6,2),-- MES, 
            CONCAT(SUBSTR(FECHA,1,4),SUBSTR(FECHA,6,2)),-- ANIOMES, 
            FECHA,-- FECHA_FOTO_INVENTARIO, 
            PLANT,-- CENTRO_ID, 
            STORAGELOCATION,-- ALMACEN_ID, 
            CONCAT(PLANT,'_',STORAGELOCATION),-- ALMACENCENTRO_ID, 
            MATERIAL,-- MATERIAL_ID, 
            CONCAT(PLANT,'_',MATERIAL),-- MATERIALCENTRO_ID, 
            MATERIALTYPE,-- TIPOMATERIAL_ID, 
            PRICE_CTRL_MSOCIEDAD,-- CONTROL_PRECIOS, 
            INVENTORYSPECIALSTOCKTYPE,-- STOCKESPECIAL, 
            SPECIALSTOCKIDFGCUSTOMER,-- NUMSTOCKESPECIAL, 
            STOCKIDENTIFYINGBATCH,-- LOTE, 
            '',-- LOTECENTRO_ID, 
            CASE WHEN INVENTORYSTOCKTYPE = 01 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END AS IND_LIBRE_UTILIZACION_UMB,-- IND_LIBRE_UTILIZACION_UMB, 
            CASE WHEN INVENTORYSTOCKTYPE = 07 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END AS IND_BLOQUEADO_UMB,-- IND_BLOQUEADO_UMB, 
            CASE WHEN INVENTORYSTOCKTYPE = 02 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END AS IND_CONTROL_CALIDAD_UMB,-- IND_CONTROL_CALIDAD_UMB, 
            CASE WHEN INVENTORYSTOCKTYPE IN (04,05)  THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END AS IND_TRANSITO_UMB,-- IND_TRANSITO_UMB, 
            CASE WHEN INVENTORYSTOCKTYPE = 01 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END+
            CASE WHEN INVENTORYSTOCKTYPE = 07 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END+
            CASE WHEN INVENTORYSTOCKTYPE = 02 THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END+ 
            CASE WHEN INVENTORYSTOCKTYPE IN (04,05)  THEN MATLWRHSSTKQTYINMATLBASEUNIT ELSE 0 END AS IND_INVENTARIO_FISICO_UMB,-- IND_INVENTARIO_FISICO_UMB, 
            COM_QTY,-- IND_LIBRE_VENTA_UMB, 
            --EISBE,-- IND_STOCK_SEGURIDAD_UMB, 
            MATERIALBASEUNIT,-- UNI_UMB, 
            -- IND_LIBRE_UTILIZACION_EST, 
            -- IND_BLOQUEADO_EST, 
            -- IND_CONTROL_CALIDAD_EST, 
            -- IND_TRANSITO_EST, 
            -- IND_INVENTARIO_FISICO_EST, 
            -- IND_LIBRE_VENTA_EST, 
            -- IND_STOCK_SEGURIDAD_EST, 
            -- UNI_EST, 
            -- IND_COSTO_INV_MXN, 
            -- MON_MXN, 
            -- IND_COSTO_INV_LOC, 
            -- MON_LOC, 
            -- IND_COSTO_INV_USD, 
            -- MON_USD, 
            -- ID_SISORIGEN, 
            -- MANDANTE, 
            -- FECHA_CARGA, 
            -- ZONA_HORARIA            
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZMM_INVENTARIO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.PRE.PFCT_LOG_INVENTARIO;

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