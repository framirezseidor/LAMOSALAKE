CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_LOG_ADH_INV_FCT_ADH_APIC()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-07
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa PRE a CON para FCT_LOG_ADH_INVENTARIO_ACT
---------------------------------------------------------------------------------
*/    
BEGIN
    -- 1. BORRAR registros eliminados
    DELETE FROM MIRRORING.FCT_ADH_APIC 
    WHERE
    MODELO = 'INVENTARIO';


   -- 2. INSERTAR o ACTUALIZAR registros nuevos o modificados
    INSERT INTO MIRRORING.FCT_ADH_APIC 
            SELECT
                CONCAT(ANIOMES, ALMACENCENTRO_ID,MATERIAL_ID) AS APIC_ID,
                ANIO AS ANIO,
                MES AS MES,
                ANIOMES AS ANIOMES,
                FECHA_FOTO_INVENTARIO AS FECHA_LOGISTICA,
                ALMACENCENTRO_ID AS ALMACENCENTRO_ID,
                ALMACEN_ID AS ALMACEN_ID,
                CENTRO_ID AS CENTRO_ID,
                '' AS SOCIEDAD_ID,
                '' AS GRUPOCOMPRAS_ID,
                '' AS ORGCOMPRAS_ID,
                '' AS PROVEEDOR_ID,
                MATERIALCENTRO_ID AS MATERIALCENTRO_ID,
                MATERIAL_ID AS MATERIAL_ID,
                '' AS CLASEMOVIMIENTO_ID,
                '' AS CLAVEOPERACION,
                '' AS GRUPO_OPERACION,
                '' AS BOR_CLASEPEDIDO_ID,
                '' AS VTA_CLASEFACTURA_ID,
                '' AS VTA_TIPO_TRANSACCION,
                0 AS BOR_IND_BO_TOTAL_TON,
                0 AS VTA_IND_VENTA_PCP_LOC,
                0 AS VTA_IND_VENTA_PCP_TON,
                0 AS VTA_IND_VENTA_REAL_LOC,
                0 AS VTA_IND_VENTA_REAL_TON,
                SUM(IND_BLOQUEADO_TON) AS INV_IND_BLOQUEADO_TON,
                SUM(IND_BLOQUEADO_UMB) AS INV_IND_BLOQUEADO_UMB,
                SUM(IND_CONTROL_CALIDAD_TON) AS INV_IND_CONTROL_CALIDAD_TON,
                SUM(IND_CONTROL_CALIDAD_UMB) AS INV_IND_CONTROL_CALIDAD_UMB,
                SUM(IND_COSTO_INV_LOC) AS INV_IND_COSTO_INV_LOC,
                SUM(IND_INVENTARIO_FISICO_TON) AS INV_IND_INVENTARIO_FISICO_TON,
                SUM(IND_INVENTARIO_FISICO_UMB) AS INV_IND_INVENTARIO_FISICO_UMB,
                SUM(IND_LIBRE_UTILIZACION_TON) AS INV_IND_LIBRE_UTILIZACION_TON,
                SUM(IND_LIBRE_UTILIZACION_UMB) AS INV_IND_LIBRE_UTILIZACION_UMB,
                SUM(IND_LIBRE_VENTA_TON) AS INV_IND_LIBRE_VENTA_TON,
                SUM(IND_LIBRE_VENTA_UMB) AS INV_IND_LIBRE_VENTA_UMB,
                SUM(IND_STOCK_SEGURIDAD_TON) AS INV_IND_STOCK_SEGURIDAD_TON,
                SUM(IND_STOCK_SEGURIDAD_UMB) AS INV_IND_STOCK_SEGURIDAD_UMB,
                SUM(IND_TRANSITO_TON) AS INV_IND_TRANSITO_TON,
                SUM(IND_TRANSITO_UMB) AS INV_IND_TRANSITO_UMB,
                0 AS ABS_IND_CANTIDAD_TOTAL,
                0 AS ABS_IND_CANT_DIAS_COLOCACION,
                0 AS ABS_IND_CANT_POSICIONES_PEDIDO,
                0 AS ABS_IND_IMPTE_COMPRA_LOC,
                0 AS ABS_IND_IMPTE_COMPRA_PED,
                0 AS ABS_IND_IMPTE_COMPRA_USD,
                0 AS ABS_IND_IMPTE_COMPRA_SIN_FLETE_LOC,
                0 AS ABS_IND_IMPTE_COMPRA_SIN_FLETE_PED,
                0 AS ABS_IND_IMPTE_COMPRA_SIN_FLETE_USD,
                0 AS ABS_IND_IMPTE_FLETE_LOC,
                0 AS ABS_IND_IMPTE_FLETE_PED,
                0 AS ABS_IND_IMPTE_FLETE_USD,
                0 AS ABS_PRECIO_ESTANDAR_MXN ,
                0 AS  ABS_PRECIO_ESTANDAR_ML,
                0 AS ABS_PRECIO_ESTANDAR_USD ,
                '' UEN_ID ,                
                0 AS PRD_IND_IMPTE_ML,
                0 AS PRD_IND_IMPTE_COMPRA_ML,
                0 AS PRD_IND_IMPTE_USD,
                0 AS PRD_IND_IMPTE_COMPRA_USD,
                0 AS PRD_IND_CANTIDAD_UMB,
                0 AS PRD_IND_CANTIDAD_UME,
                MON_LOC AS MON_LOC,
                '' AS MON_PED,
                MON_USD AS MON_USD,
                '' AS UNI_PED,
                UNI_EST AS UNI_EST,
                UNI_UMB AS UNI_UMB,
                FECHA_CARGA AS FECHA_CARGA,
                MANDANTE AS MANDANTE,
                MODELO AS MODELO,
                SISORIGEN_ID AS SISORIGEN_ID,
                ZONA_HORARIA AS ZONA_HORARIA,
        CASE MONTH(FECHA_LOGISTICA)
            WHEN 1 THEN 'ENE'
            WHEN 2 THEN 'FEB'
            WHEN 3 THEN 'MAR'
            WHEN 4 THEN 'ABR'
            WHEN 5 THEN 'MAY'
            WHEN 6 THEN 'JUN'
            WHEN 7 THEN 'JUL'
            WHEN 8 THEN 'AGO'
            WHEN 9 THEN 'SEP'
            WHEN 10 THEN 'OCT'
            WHEN 11 THEN 'NOV'
            WHEN 12 THEN 'DIC'
        END AS MES_TEXTO                
            FROM
                MIRRORING.FCT_LOG_ADH_INVENTARIO
            GROUP BY
                APIC_ID,
                ANIO,
                MES,
                ANIOMES,
                FECHA_LOGISTICA,
                ALMACENCENTRO_ID,
                ALMACEN_ID,
                CENTRO_ID,
                SOCIEDAD_ID,
                GRUPOCOMPRAS_ID,
                ORGCOMPRAS_ID,
                PROVEEDOR_ID,
                MATERIALCENTRO_ID,
                MATERIAL_ID,
                CLASEMOVIMIENTO_ID,
                CLAVEOPERACION,
                GRUPO_OPERACION,
                BOR_CLASEPEDIDO_ID,
                VTA_CLASEFACTURA_ID,
                VTA_TIPO_TRANSACCION,
                UEN_ID,
                MON_LOC,
                MON_PED,
                MON_USD,
                UNI_PED,
                UNI_EST,
                UNI_UMB,
                MANDANTE,
                FECHA_CARGA,
                MODELO,
                SISORIGEN_ID,
                ZONA_HORARIA   
;
        RETURN CONCAT('Ejecución correcta');
END;

$$;