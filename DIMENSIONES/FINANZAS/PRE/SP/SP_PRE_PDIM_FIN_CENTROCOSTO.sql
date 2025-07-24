CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_FIN_CENTROCOSTO()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-15
 Creador:            Juan Pedreros
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_FIN_CENTROCOSTO
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

        DELETE FROM PRE.PDIM_FIN_CENTROCOSTO;

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

        INSERT INTO PRE.PDIM_FIN_CENTROCOSTO
        (
            CENTROCOSTO_ID,
            CENTROCOSTO,
            CENTROCOSTO_TEXT,
            SOCIEDADCO_ID,
            SOCIEDAD_ID,
            CENTROBENEF_ID,
            DIRECCION_ID,
            DIRECCION_TEXT,
            GERENCIA_ID,
            GERENCIA_TEXT,
            SUCURSAL_ID,
            SUCURSAL_TEXT,
            CCAGRUPADOR_ID,
            CCAGRUPADOR_TEXT,
            TIPOGASTO_ID,
            TIPOGASTO_TEXT,
            ENCARGADO_CC_ID,
            ENCARGADO_CC_TEXT,
            ENCARGADO_GERENCIA_ID,
            ENCARGADO_GERENCIA_TEXT,
            ENCARGADO_DIRECCION_ID,
            ENCARGADO_DIRECCION_TEXT,
            SOCIEDADCECOASOC_ID,
            CENTROCOSASOC_ID,
            CENTROCOSASOC_TEXT,
            ENCARGADO_DIRECAREA_ID,
            ENCARGADO_DIRECAREA_TEXT,
            ZTPGTOBPC,
            FECHA_DESDE,
            FECHA_HASTA,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA,
            TIPO
        )
        
        SELECT  CONCAT(CECO.KOKRS,'_',CECO.KOSTL) AS "CENTROCOSTO_ID",
                CECO.KOSTL AS "CENTROCOSTO",
                CECOTEXT.TXTMD AS "CENTROCOSTO_TEXT",
                CECO.KOKRS AS "SOCIEDADCO_ID",
                CECO.BUKRS AS "SOCIEDAD_ID",
                CONCAT(CECO.KOKRS,'_',CECO.PRCTR) AS "CENTROBENEF_ID",
                ZCECO.ZZCOD_DIREC_NEG AS "DIRECCION_ID",
                CECO.ZZDIREC_NEG AS "DIRECCION_TEXT",
                ZCECO.ZZCOD_GERENCIA AS "GERENCIA_ID",
                CECO.ZZGERENCIA AS "GERENCIA_TEXT",
                ZCECO.ZZCOD_SUCURSAL AS "SUCURSAL_ID",
                CECO.ZZSUCURSAL AS "SUCURSAL_TEXT",
                ZCECO.ZZCOD_ENC_AGRUP AS "CCAGRUPADOR_ID",
                CECO.ZZENC_AGRUP AS "CCAGRUPADOR_TEXT",
                ZCECO.ZZCOD_ENC_GASTO AS "TIPOGASTO_ID",
                CECO.ZZENC_GASTO AS "TIPOGASTO_TEXT",
                ZCECO.ZZENC_CC AS "ENCARGADO_CC_ID",
                ZCECO.ZZENC_CC_DES AS "ENCARGADO_CC_TEXT",
                ZCECO.ZZENC_GC AS "ENCARGADO_GERENCIA_ID",
                ZCECO.ZZENC_GC_DES AS "ENCARGADO_GERENCIA_TEXT",
                ZCECO.ZZENC_DC AS "ENCARGADO_DIRECCION_ID",
                ZCECO.ZZENC_DC_DES AS "ENCARGADO_DIRECCION_TEXT",
                CECO.BUKRS AS "SOCIEDADCECOASOC_ID",
                CASE WHEN ZCECO.ZZCO_ASOCIADO IS NULL OR ZCECO.ZZCO_ASOCIADO = '' THEN CECO.KOSTL ELSE ZCECO.ZZCO_ASOCIADO END AS "CENTROCOSASOC_ID",
                CECOTEXT.TXTMD AS "CENTROCOSASOC_TEXT", --LOGICA
                ZCECO.ZZCO_ENC_DIR_AREA AS "ENCARGADO_DIRECAREA_ID",
                ZCECO.ZZCO_ENC_DIR_DES AS "ENCARGADO_DIRECAREA_TEXT",
                ZCECO.ZZCO_GASTO_BPC AS "ZTPGTOBPC",
                CECO.DATEFROM AS "FECHA_DESDE",
                CECO.DATETO AS "FECHA_HASTA",
                CECO."SISORIGEN_ID" AS "SISORIGEN_ID",
                CECO."MANDANTE" AS "MANDANTE",
                CECO."FECHA_CARGA" AS "FECHA_CARGA",
                CECO."ZONA_HORARIA" AS "ZONA_HORARIA",
                CECO."TIPO" AS "TIPO"
        FROM RAW.SQ1_EXT_0COSTCENTER_ATTR AS CECO

        LEFT JOIN RAW.SQ1_EXT_ZCSKS AS ZCECO
        ON CECO.KOSTL = ZCECO.KOSTL
        AND CECO.KOKRS = ZCECO.KOKRS
        AND CECO.DATETO = ZCECO.DATBI

        LEFT JOIN RAW.SQ1_EXT_0COSTCENTER_TEXT AS CECOTEXT
        ON CECOTEXT.LANGU = 'S' 
        AND CECOTEXT.KOKRS = CECO.KOKRS 
        AND CECOTEXT.KOSTL = CECO.KOSTL 
        AND CURRENT_DATE() BETWEEN CECOTEXT.DATEFROM AND CECOTEXT.DATETO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_FIN_CENTROCOSTO;

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
    VALUES ('SP_PRE_PDIM_FIN_CENTROCOSTO','PRE.PDIM_FIN_CENTROCOSTO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;