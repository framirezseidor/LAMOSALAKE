CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MAT_FORMATOGRUPO_REV()
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-30
 Creador:            Fidel Ramírez
 Descripción:        PDIM_MAT_FORMATOGRUPO_REV
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

        TRUNCATE TABLE PRE.PDIM_MAT_FORMATOGRUPO_REV;

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

        INSERT INTO PRE.PDIM_MAT_FORMATOGRUPO_REV
        (
            FORMATOGRUPO_ID	,
            FORMATOGRUPO_TEXT	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA	
        )
            SELECT			
                ATWRT	AS	FORMATOGRUPO_ID	,
                TEXT	AS	FORMATOGRUPO_TEXT	,
                SISORIGEN_ID	AS	SISORIGEN_ID	,
                MANDANTE	AS	MANDANTE	,
                FECHA_CARGA	AS	FECHA_CARGA	,
                ZONA_HORARIA	AS	ZONA_HORARIA	
            FROM			
                RAW.SQ1_EXT_1CL_AR_F001			
            WHERE LANGU = 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MAT_FORMATOGRUPO_REV;

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
    VALUES ('SP_PRE_PDIM_MAT_FORMATOGRUPO_REV','PRE.PDIM_MAT_FORMATOGRUPO_REV', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 