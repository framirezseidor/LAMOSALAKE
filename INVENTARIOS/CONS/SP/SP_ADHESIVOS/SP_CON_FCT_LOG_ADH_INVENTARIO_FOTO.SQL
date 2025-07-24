CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_LOG_ADH_INVENTARIO_FOTO(
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
    TEXTO           VARCHAR(200);
    PARAMETRO       VARCHAR(20);
    CENTRO_INICIO   VARCHAR(20);
    CENTRO_FIN      VARCHAR(20);

BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE CENTROS
    ---------------------------------------------------------------------------------
    PARAMETRO := 'PLANT';
    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , :PARAMETRO) INTO :CENTRO_INICIO ;
        
    CENTRO_FIN := SPLIT_PART(CENTRO_INICIO,'-',2);
    CENTRO_INICIO := SPLIT_PART(CENTRO_INICIO,'-',1);   

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM CON.FCT_LOG_ADH_INVENTARIO_FOTO 
        --restricción que valida que solo borre la fecha que se encuentra en la tabla PFCT_LOG_INVENTARIO
        WHERE FECHA_FOTO_INVENTARIO IN (SELECT DISTINCT FECHA_FOTO_INVENTARIO FROM PRE.PFCT_LOG_INVENTARIO)
        --restricción que valida que la fecha solo sea de fin de mes
        --AND   FECHA_FOTO_INVENTARIO=TO_NUMBER(REPLACE(LAST_DAY(TO_DATE(TO_VARCHAR(FECHA_FOTO_INVENTARIO),'YYYYMMDD')),'-',''))
        AND   FECHA_FOTO_INVENTARIO=LAST_DAY(FECHA_FOTO_INVENTARIO)
        --borrado selectivo
        AND   CENTRO_ID BETWEEN :CENTRO_INICIO AND :CENTRO_FIN
        AND   TIPOMATERIAL_ID IN (
                                SELECT R_INICIO FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'
                                UNION
                                SELECT R_FIN FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'   
                                );

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
            COALESCE(B.VALOR_ESTANDARD, T.UNI_UMB),
            IND_LIBRE_UTILIZACION_EST,
            IND_BLOQUEADO_EST,
            IND_CONTROL_CALIDAD_EST,
            IND_TRANSITO_EST,
            IND_INVENTARIO_FISICO_EST,
            --IND_LIBRE_VENTA_EST,
            IND_STOCK_SEGURIDAD_EST,
            COALESCE(U.VALOR_ESTANDARD, T.UNI_EST),
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
        FROM PRE.PFCT_LOG_INVENTARIO T
        LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA U ON UPPER(TRIM(T.UNI_EST)) = UPPER(TRIM(U.VALOR_ORIGINAL))
        LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA B ON UPPER(TRIM(T.UNI_UMB)) = UPPER(TRIM(B.VALOR_ORIGINAL))
        WHERE CENTRO_ID LIKE 'A%'
        --restricción que valida que la fecha solo sea de fin de mes
        --AND   FECHA_FOTO_INVENTARIO=TO_NUMBER(REPLACE(LAST_DAY(TO_DATE(TO_VARCHAR(FECHA_FOTO_INVENTARIO),'YYYYMMDD')),'-',''))
        AND   FECHA_FOTO_INVENTARIO=LAST_DAY(FECHA_FOTO_INVENTARIO)
        AND   CENTRO_ID BETWEEN :CENTRO_INICIO AND :CENTRO_FIN
        AND   TIPOMATERIAL_ID IN (
                                SELECT R_INICIO FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'
                                UNION
                                SELECT R_FIN FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'    
                                ) ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED 
        FROM CON.FCT_LOG_ADH_INVENTARIO_FOTO
        WHERE CENTRO_ID BETWEEN :CENTRO_INICIO AND :CENTRO_FIN
        AND   TIPOMATERIAL_ID IN (
                                SELECT R_INICIO FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'
                                UNION
                                SELECT R_FIN FROM RAW.PARAMETROS_EXTRACCION
                                WHERE EXTRACTOR = :EXTRACTOR AND NEGOCIO = :NEGOCIO AND PARAMETRO LIKE 'MATERIALTYPE%'    
                                );

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
        VALUES('SP_CON_FCT_LOG_ADH_INVENTARIO_FOTO','FCT_LOG_ADH_INVENTARIO_FOTO', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 