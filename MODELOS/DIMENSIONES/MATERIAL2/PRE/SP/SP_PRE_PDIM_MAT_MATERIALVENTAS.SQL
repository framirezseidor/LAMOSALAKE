CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MAT_MATERIALVENTAS()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_MAT_MATERIALVENTAS
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

        DELETE FROM PRE.PDIM_MAT_MATERIALVENTAS;

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

        INSERT INTO PRE.PDIM_MAT_MATERIALVENTAS
        (
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            GRUPOIMPUTACION_ID,
            GRUPOMATERIALES_ID,
            GRUPOMATERIALES1_ID,
            GRUPOMATERIALES2_ID,
            GRUPOMATERIALES3_ID,
            GRUPOMATERIALES4_ID,
            GRUPOMATERIALES5_ID,
            STATUSMAT_CANDIS_ID,
            MATERIAL_ID,
            MATERIALVENTAS_ID,
            MATERIALVENTAS_TEXT,
            SISORIGEN_ID,
            MANDANTE ,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde RAW
            ATTR.VKORG AS ORGVENTAS_ID,
            ATTR.VTWEG AS CANALDISTRIB_ID,
            ATTR.KTGRM AS GRUPOIMPUTACION_ID,
            ATTR.KONDM AS GRUPOMATERIALES_ID,
            ATTR.MVGR1 AS GRUPOMATERIALES1_ID,
            ATTR.MVGR2 AS GRUPOMATERIALES2_ID,
            ATTR.MVGR3 AS GRUPOMATERIALES3_ID,
            ATTR.MVGR4 AS GRUPOMATERIALES4_ID,
            ATTR.MVGR5 AS GRUPOMATERIALES5_ID,
            ATTR.VMSTA AS STATUSMAT_CANDIS_ID,
            LTRIM(ATTR.MATNR,'0') AS MATERIAL_ID,
            CONCAT(ATTR.VKORG,'_',ATTR.VTWEG,'_',LTRIM(ATTR.MATNR,'0')) AS MATERIALVENTAS_ID,
            TEXT.TXTMD AS MATERIALVENTAS_TEXT,
            ATTR.SISORIGEN_ID,
            ATTR.MANDANTE,
            CURRENT_TIMESTAMP AS FECHA_CARGA,
            RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA 
        FROM RAW.SQ1_EXT_0MAT_SALES_ATTR AS ATTR 
        LEFT JOIN RAW.SQ1_EXT_0MAT_SALES_TEXT AS TEXT 
        ON ATTR.VKORG = TEXT.VKORG 
        AND ATTR.VTWEG = TEXT.VTWEG 
        AND ATTR.MATNR = TEXT.MATNR
        WHERE TEXT.SPRAS= 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MAT_MATERIALVENTAS;

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
    VALUES ('SP_PRE_PDIM_MAT_MATERIALVENTAS','PRE.PDIM_MAT_MATERIALVENTAS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 