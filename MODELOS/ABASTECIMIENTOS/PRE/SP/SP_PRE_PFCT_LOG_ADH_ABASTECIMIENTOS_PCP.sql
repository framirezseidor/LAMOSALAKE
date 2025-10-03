CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_LOG_ADH_ABASTECIMIENTOS_PCP()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_LOG_ADH_ABASTECIMIENTOS_PCP
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

        DELETE FROM PRE.PFCT_LOG_ADH_ABASTECIMIENTOS_PCP;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PFCT_LOG_ADH_ABASTECIMIENTOS_PCP
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
            LEFT(ANIOMES,4),-- ANIO,
            RIGHT(ANIOMES,2),-- MES,
            CONCAT(LEFT(ANIOMES,4),'-',RIGHT(ANIOMES,2)),-- ANIOMES,
            CONCAT(LEFT(ANIOMES,4),'-',RIGHT(ANIOMES,2),'-01'),-- FECHA_ABASTECIMIENTOS_PCP,
            SOCIEDAD_ID,-- SOCIEDAD_ID,
            CENTRO_ID,-- CENTRO_ID,
            MATERIAL_ID,-- MATERIAL_ID,
            CONCAT(CENTRO_ID,'_',MATERIAL_ID),-- MATERIALCENTRO_ID,
            REPLACE(IND_PRECIOCOMPRA_PCP_LOC,',',''),-- IND_PRECIOCOMPRA_PCP_LOC,
            MON_LOC,-- MON_LOC,
            'File',-- SISORIGEN_ID, 
            '',-- MANDANTE, 
            CURRENT_TIMESTAMP,-- FECHA_CARGA, 
            RIGHT(CURRENT_TIMESTAMP,5)-- ZONA_HORARIA  

        FROM RAW.FILE_XLS_LOG_ADH_ABASTECIMIENTOS_PCP
        ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_LOG_ADH_ABASTECIMIENTOS_PCP;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PFCT_LOG_ADH_ABASTECIMIENTOS_PCP','PRE.PFCT_LOG_ADH_ABASTECIMIENTOS_PCP', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 