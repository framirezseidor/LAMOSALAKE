CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MAT_MATERIALCENTRO()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-28
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_MAT_MATERIALCENTRO
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

        DELETE FROM PRE.PDIM_MAT_MATERIALCENTRO;

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

        INSERT INTO PRE.PDIM_MAT_MATERIALCENTRO
        (
            CENTRO_ID,
            PLANIFNECESIDADES_ID,
            INDICADOR_ABC_ID,
            CATEGORIAVALORACION_ID,
            STATUSMAT_ESPCENTRO_ID,
            MATERIAL_ID,
            MATERIALCENTRO_ID,
            MATERIALCENTRO_TEXT,
            SISORIGEN_ID,
            MANDANTE ,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde RAW
            ATTR.WERKS AS CENTRO_ID,
            ATTR.DISMM AS PLANIFNECESIDADES_ID,
            ATTR.MAABC AS INDICADOR_ABC_ID,
            ATTR.ZZBKLAS AS CATEGORIAVALORACION_ID,
            ATTR.MMSTA AS STATUSMAT_ESPCENTRO_ID,
            ATTR.MATNR AS MATERIAL_ID,
            CONCAT(ATTR.WERKS, '_', LTRIM(ATTR.MATNR, '0')) AS MATERIALCENTRO_ID,
            TEXT.TXTMD AS MATERIALCENTRO_TEXT,
            ATTR.SISORIGEN_ID,
            ATTR.MANDANTE,
            CURRENT_TIMESTAMP AS FECHA_CARGA,
            RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA 
        FROM RAW.SQ1_EXT_0MAT_PLANT_ATTR AS ATTR 
        LEFT JOIN RAW.SQ1_EXT_0MAT_PLANT_TEXT AS TEXT 
        ON ATTR.MATNR = TEXT.MATNR 
        AND ATTR.WERKS = TEXT.WERKS 
        WHERE TEXT.SPRAS= 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MAT_MATERIALCENTRO;

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
    VALUES ('SP_PRE_PDIM_MAT_MATERIALCENTRO','PRE.PDIM_MAT_MATERIALCENTRO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
