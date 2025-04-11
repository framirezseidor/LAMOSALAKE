CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.CON.SP_CON_FCT_OPE_REV_PROD_ACT()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para FCT_OPE_REV_PROD_ACT
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

        DELETE FROM LAMOSALAKE_DEV.CON.FCT_OPE_REV_PROD_ACT;

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

        INSERT INTO LAMOSALAKE_DEV.CON.FCT_OPE_REV_PROD_ACT
        (
            -- Lista de columnas destino en tabla CON
            -- Ejemplo: ID, FECHA, CLIENTE_ID, MONTO
            ANIO,
            MES,
            ANIOMES,
            CENTRO_ID,
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            GRUPOCLIENTES2_ID,
            FECHA_CONTABILIZACION ,
            CLASEMOVIMIENTO_ID,
            DOCMATERIAL ,
            DOCMATERIAL_POS,
            CLASEORDFAB_ID,
            ORDENFABRICACION ,
            NOTIFICACION,
            NOTIFICACION_CONTADOR_LLAVE ,
            NOTIFICACION_OPERACION ,
            NOTIFICACION_CONTADOR,
            PERFIL_CONTROL_FABRICACION,
            PERFILCF_CENTRO_ID ,
            MATERIAL_ID ,
            MATERIALCENTRO_ID ,
            LOTE ,
            MATERIAL_NOTIF ,
            CALIDAD_NOTIF,
            MARCAPLANEACION_ID,
            IND_PRODUCCION_TOTAL_M2 ,
            IND_PRODUCCION_1A_M2 ,
            IND_PRODUCCION_2A_M2 ,
            UNI_EST,
            ID_SISORIGEN,--CAMBIAR POR SISORIGEN_ID
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde PRE
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
            ANIO,
            MES,
            ANIOMES,
            CENTRO_ID,
            ALMACEN_ID,
            ALMACENCENTRO_ID,
            GRUPOCLIENTES2_ID,
            FECHA_CONTABILIZACION ,
            CLASEMOVIMIENTO_ID,
            DOCMATERIAL ,
            DOCMATERIAL_POS,
            CLASEORDFAB_ID,
            ORDENFABRICACION ,
            NOTIFICACION,
            NOTIFICACION_CONTADOR_LLAVE ,
            NOTIFICACION_OPERACION ,
            NOTIFICACION_CONTADOR,
            PERFIL_CONTROL_FABRICACION,
            PERFILCF_CENTRO_ID ,
            MATERIAL_ID ,
            MATERIALCENTRO_ID ,
            LOTE ,
            MATERIAL_NOTIF ,
            CALIDAD_NOTIF,
            MARCAPLANEACION_ID,
            IND_PRODUCCION_TOTAL_M2 ,
            IND_PRODUCCION_1A_M2 ,
            IND_PRODUCCION_2A_M2 ,
            UNI_EST,
            ID_SISORIGEN,--CAMBIAR POR SISORIGEN_ID
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.CON.FCT_OPE_REV_PROD_ACT;

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

    INSERT INTO LOGS.HISTORICAL_EJECUCIONES;
    VALUES ('SP_CON_FCT_OPE_REV_PROD_ACT','FCT_OPE_REV_PROD_ACT', :F_INICIO, :F:FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );


    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 