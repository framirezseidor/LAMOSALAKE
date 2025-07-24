CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MAT_INDICADOR_ABC()
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
 Descripción:        PDIM_MAT_INDICADOR_ABC
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

        TRUNCATE TABLE PRE.PDIM_MAT_INDICADOR_ABC;

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

        INSERT INTO PRE.PDIM_MAT_INDICADOR_ABC
        (
            INDICADOR_ABC_ID	,
            INDICADOR_ABC_TEXT	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA	
        )
            SELECT			
                INDICADOR_ABC_ID	AS	INDICADOR_ABC_ID	,
                INDICADOR_ABC_TEXT	AS	INDICADOR_ABC_TEXT	,
                SISORIGEN_ID	AS	SISORIGEN_ID	,
                MANDANTE	AS	MANDANTE	,
                FECHA_CARGA	AS	FECHA_CARGA	,
                ZONA_HORARIA	AS	ZONA_HORARIA	
            FROM			
                PRE.FILE_CSV_MAT_INDICADOR_ABC    ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MAT_INDICADOR_ABC;

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
    VALUES ('SP_PRE_PDIM_MAT_INDICADOR_ABC','PRE.PDIM_MAT_INDICADOR_ABC', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 