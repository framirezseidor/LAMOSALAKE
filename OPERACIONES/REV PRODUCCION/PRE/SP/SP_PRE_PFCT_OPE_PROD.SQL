CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_OPE_PROD()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-03
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_OPE_REV_PROD
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

        DELETE FROM PRE.PFCT_OPE_REV_PROD;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PFCT_OPE_REV_PROD
        (
            -- Lista de columnas destino en tabla PRE
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
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA ,
            CEBECENTRO_ID
        )
        SELECT
            -- Transformaciones desde RAW
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
        YEAR(CALDISENO.BUDAT1) AS ANIO,
        MONTH(CALDISENO.BUDAT1) AS MES,
        TO_CHAR(CALDISENO.BUDAT1,'YYYYMM') AS ANIOMES, 
        CALDISENO.WERKS AS CENTRO_ID,
        CALDISENO.LGORT AS ALMACEN_ID,
        CONCAT(CALDISENO.WERKS,'_',CALDISENO.LGORT) AS ALMACENCENTRO_ID,
        CASE 
            WHEN CALDISENO.LGORT = 'PTUS' THEN 'ZUS'
            WHEN MAT.MARCAPLANEACION_ID = '001' THEN 'ZLA' 
            WHEN MAT.MARCAPLANEACION_ID = '002' THEN 'ZPO' 
            WHEN MAT.MARCAPLANEACION_ID = '003' THEN 'ZPI' 
        END AS GRUPOCLIENTES2_ID,
        CALDISENO.BUDAT1 AS FECHA_CONTABILIZACION,
        CALDISENO.BWART AS CLASEMOVIMIENTO_ID,
        CALDISENO.MBLNR AS DOCMATERIAL,
        CALDISENO.ZEILE AS DOCMATERIAL_POS,
        CALDISENO.AUART AS CLASEORDFAB_ID,
        CALDISENO.AUFNR AS ORDENFABRICACION,
        CASE 
            WHEN CALDISENO.RUECK = '0000000000' THEN 'B'
            ELSE 'A'
        END AS NOTIFICACION,
        CALDISENO.NOTIF_CONTADOR AS NOTIFICACION_CONTADOR_LLAVE,
        CALDISENO.RUECK AS NOTIFICACION_OPERACION,
        CALDISENO.RMZHL AS NOTIFICACION_CONTADOR,
        CALDISENO.SFCPF AS PERFIL_CONTROL_FABRICACION,
        CONCAT(CALDISENO.WERKS,'_',CALDISENO.SFCPF) AS PERFILCF_CENTRO_ID,
        CASE 
            WHEN CALDISENO.ATWRT = '001' THEN CALDISENO.MATNR
            WHEN CALDISENO.ATWRT3 = '001' THEN CALDISENO.PLNBEZ2
            WHEN CALDISENO.ATWRT2 = '001' THEN CALDISENO.MATNRZ
            ELSE CALDISENO.MATNR
        END AS MATERIAL_ID,
        CONCAT(CALDISENO.WERKS,'_',CALDISENO.MATNR) AS MATERIALCENTRO_ID,
        CALDISENO.CHARG AS LOTE,
        CALDISENO.MATNR AS MATERIAL_NOTIF,
        CALDISENO.ATWRT AS CALIDAD_NOTIF,
        MAT.MARCAPLANEACION_ID AS MARCAPLANEACION_ID,
        CALDISENO.MENGE AS IND_PRODUCCION_TOTAL_M2,
        CASE 
            WHEN CALDISENO.ATWRT = '001' THEN CALDISENO.MENGE
            ELSE 0
        END AS IND_PRODUCCION_1A_M2,
        CASE 
            WHEN CALDISENO.ATWRT <> '001' THEN CALDISENO.MENGE
            ELSE 0
        END AS IND_PRODUCCION_2A_M2,
        CALDISENO.MEINS AS UNI_EST,
        CALDISENO.SISORIGEN_ID,
        CALDISENO.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA,
        CASE 
            WHEN CALDISENO.PRCTR = 'SC01090001' THEN 'R231'
            WHEN CALDISENO.PRCTR = 'SC02090001' THEN 'R232'
            WHEN CALDISENO.PRCTR = 'SC03090001' THEN 'R233'
            ELSE CALDISENO.WERKS
    END AS CEBECENTRO_ID
    FROM RAW.SQ1_MOF_ZBWPP_CALDISENO AS CALDISENO
    LEFT JOIN CON.DIM_MAT_MATERIAL AS MAT
        ON 
            CASE 
                WHEN CALDISENO.ATWRT = '001' THEN CALDISENO.MATNR
                WHEN CALDISENO.ATWRT3 = '001' THEN CALDISENO.PLNBEZ2
                WHEN CALDISENO.ATWRT2 = '001' THEN CALDISENO.MATNRZ
                ELSE CALDISENO.MATNR
            END = MAT.MATERIAL_ID;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_OPE_REV_PROD;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PFCT_OPE_PROD','PRE.PFCT_OPE_PROD', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 