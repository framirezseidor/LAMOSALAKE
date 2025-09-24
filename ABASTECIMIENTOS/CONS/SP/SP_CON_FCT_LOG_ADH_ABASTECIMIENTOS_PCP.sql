CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_LOG_ADH_ABASTECIMIENTOS_PCP()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-09-16
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa RAW a PRE para FCT_LOG_ADH_ABASTECIMIENTOS_PCP
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

        DELETE FROM CON.FCT_LOG_ADH_ABASTECIMIENTOS_PCP;

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

        INSERT INTO CON.FCT_LOG_ADH_ABASTECIMIENTOS_PCP
        (
            ANIO,
            MES,
            ANIOMES,
            FECHA_ABASTECIMIENTOS_PCP,
            SOCIEDAD_ID,
            CENTRO_ID,
            MATERIAL_ID,
            MATERIALCENTRO_ID,
            IND_PRECIOCOMPRA_PCP_LOC,
            MON_LOC,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
       SELECT
            ANIO,
            MES,
            ANIOMES,
            FECHA_ABASTECIMIENTOS_PCP,
            SOCIEDAD_ID,
            CENTRO_ID,
            MATERIAL_ID,
            MATERIALCENTRO_ID,
            IND_PRECIOCOMPRA_PCP_LOC,
            MON_LOC,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM PRE.PFCT_LOG_ADH_ABASTECIMIENTOS_PCP;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_LOG_ADH_ABASTECIMIENTOS_PCP;

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
        VALUES('SP_CON_FCT_LOG_ADH_ABASTECIMIENTOS_PCP','CON.FCT_LOG_ADH_ABASTECIMIENTOS_PCP', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
        CREATE OR REPLACE TABLE MIRRORING.FCT_LOG_ADH_ABASTECIMIENTOS_PCP
        CLONE CON.FCT_LOG_ADH_ABASTECIMIENTOS_PCP;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_FCT_LOG_ADH_ABASTECIMIENTOS_PCP ON TABLE MIRRORING.FCT_LOG_ADH_ABASTECIMIENTOS_PCP;
        
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
