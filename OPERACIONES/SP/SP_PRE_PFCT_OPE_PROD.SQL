CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.PRE.SP_PRE_PFCT_OPE_PROD()
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

        DELETE FROM LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD;

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

        INSERT INTO LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD
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
            ID_SISORIGEN,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA ,
            CEBECENTRO_ID
        )
        SELECT
            -- Transformaciones desde RAW
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
            YEAR(BUDAT1) AS ANIO,
            MONTH(BUDAT1) AS MES,
            TO_CHAR(BUDAT1,'YYYYMM') AS ANIOMES, 
            WERKS AS CENTRO_ID,
            LGORT AS ALMACEN_ID,
            CONCAT(WERKS,'_',LGORT) AS ALMACENCENTRO_ID,
            --CASE 
              --  WHEN LGORT= 'PTUS' THEN 'ZUS' 
              --  WHEN MARCAPLANEACION_ID = '001' THEN 'ZLA'
              --  WHEN MARCAPLANEACION_ID = '002' THEN 'ZPO'
              --  WHEN MARCAPLANEACION_ID = '003' THEN 'ZFI'
            --END AS GRUPOCLIENTES2_ID,
            --CASE 
              --  WHEN LGORT= 'PTUS' THEN 'ZUS'  --WHEN ZCSTORLOC = 'PTUS' THEN 'ZUS'
              --  WHEN ZIOMARPL = '001' THEN 'ZLA'
              --  WHEN ZIOMARPL = '002' THEN 'ZPO'
              --  WHEN ZIOMARPL = '003' THEN 'ZFI'
              --  ELSE ''
            --END AS GRUPOCLIENTES2_ID, -- logica pendiente 
            CASE 
                WHEN LGORT = 'PTUS' THEN 'ZUS' -- ZCSTORLOC es almacen
                WHEN 
                    CASE 
                        WHEN ATWRT = '001' THEN MATNR
                        WHEN ATWRT3 = '001' THEN PLNBEZ2
                        WHEN ATWRT2 = '001' THEN MATNRZ
                        ELSE MATNR
                    END = '001' THEN 'ZLA'
                WHEN 
                    CASE 
                        WHEN ATWRT = '001' THEN MATNR
                        WHEN ATWRT3 = '001' THEN PLNBEZ2
                        WHEN ATWRT2 = '001' THEN MATNRZ
                        ELSE MATNR
                    END = '002' THEN 'ZPO'
                WHEN 
                    CASE 
                        WHEN ATWRT = '001' THEN MATNR
                        WHEN ATWRT3 = '001' THEN PLNBEZ2
                        WHEN ATWRT2 = '001' THEN MATNRZ
                        ELSE MATNR
                    END = '003' THEN 'ZFI'
                ELSE ''
            END AS GRUPOCLIENTES2_ID ,

            BUDAT1 AS FECHA_CONTABILIZACION ,
            BWART AS CLASEMOVIMIENTO_ID,
            MBLNR AS DOCMATERIAL ,
            ZEILE AS DOCMATERIAL_POS,
            AUART AS CLASEORDFAB_ID,
            AUFNR AS ORDENFABRICACION ,
            CASE 
                WHEN RUECK = '0000000000' THEN 'B'
                ELSE 'A'
            END AS NOTIFICACION,
            NOTIF_CONTADOR AS NOTIFICACION_CONTADOR_LLAVE ,
            RUECK AS NOTIFICACION_OPERACION ,
            RMZHL AS NOTIFICACION_CONTADOR,
            SFCPF AS PERFIL_CONTROL_FABRICACION,
            CONCAT(WERKS,'_',SFCPF) AS PERFILCF_CENTRO_ID ,
            CASE 
                WHEN ATWRT= '001' THEN MATNR
                WHEN ATWRT3 = '001' THEN PLNBEZ2
                WHEN ATWRT2 = '001' THEN MATNRZ
                ELSE MATNR
            END MATERIAL_ID,
            CONCAT(WERKS,'_',MATNR) AS PERFILCF_CENTRO_ID ,
            CHARG AS LOTE ,
            MATNR AS MATERIAL_NOTIF ,
            ATWRT AS CALIDAD_NOTIF,
            CASE 
                WHEN ATWRT= '001' THEN MATNR
                WHEN ATWRT3 = '001' THEN PLNBEZ2
                WHEN ATWRT2 = '001' THEN MATNRZ
                ELSE MATNR
            END AS MARCAPLANEACION_ID, --logica pendiente
            MENGE AS IND_PRODUCCION_TOTAL_M2 ,
            CASE 
                WHEN ATWRT = '001' THEN MENGE
                ELSE 0
            END AS IND_PRODUCCION_1A_M2 ,
            CASE 
                WHEN ATWRT <> '001' THEN MENGE
                ELSE 0
            END AS IND_PRODUCCION_2A_M2 ,
            MEINS AS UNI_EST,
            ID_SISORIGEN,
            MANDANTE,
            CURRENT_TIMESTAMP AS FECHA_CARGA,
            RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA ,
            CASE 
                WHEN PRCTR = 'SC01090001' THEN 'R231'
                WHEN PRCTR = 'SC02090001' THEN 'R232'
                WHEN PRCTR = 'SC03090001' THEN 'R233'
                ELSE WERKS
            END AS CEBECENTRO_ID
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWPP_CALDISENO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD;

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

    INSERT INTO LOGS.HISTORICAL_EJECUCIONES;
    VALUES ('SP_PRE_PFCT_OPE_PROD','PFCT_OPE_PROD', :F_INICIO, :F:FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 