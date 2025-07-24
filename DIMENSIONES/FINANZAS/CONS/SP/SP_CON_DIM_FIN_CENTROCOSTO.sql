CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_FIN_CENTROCOSTO()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_FIN_CENTROCOSTO
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

        DELETE FROM CON.DIM_FIN_CENTROCOSTO;

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

        INSERT INTO CON.DIM_FIN_CENTROCOSTO
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
        
        SELECT  CENTROCOSTO_ID,
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
        FROM PRE.PDIM_FIN_CENTROCOSTO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_FIN_CENTROCOSTO;

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
    VALUES ('SP_CON_DIM_FIN_CENTROCOSTO','CON.DIM_FIN_CENTROCOSTO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );


    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
        DELETE FROM MIRRORING.DIM_FIN_CENTROCOSTO;

        INSERT INTO MIRRORING.DIM_FIN_CENTROCOSTO(
            CENTROCOSTO_ID,
            CENTROCOSTO,
            CENTROCOSTO_TEXT,
            CENTROCOSTO_ID_TEXT,
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

        SELECT  CENTROCOSTO_ID,
                CENTROCOSTO,
                CENTROCOSTO_TEXT,
                CONCAT(CENTROCOSTO_ID,' - ',CENTROCOSTO_TEXT) AS CENTROCOSTO_ID_TEXT,
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
        FROM CON.DIM_FIN_CENTROCOSTO;

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;