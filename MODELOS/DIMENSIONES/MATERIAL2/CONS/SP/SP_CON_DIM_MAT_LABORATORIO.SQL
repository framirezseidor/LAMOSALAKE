CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_MAT_LABORATORIO()
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-10-01
 Creador:            Juan Diego Bonifacio
 Descripción:        DIM_MAT_LABORATORIO
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

        TRUNCATE TABLE CON.DIM_MAT_LABORATORIO;

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

        INSERT INTO CON.DIM_MAT_LABORATORIO
        (
            LABORATORIO_ID	,
            LABORATORIO_TEXT	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA	
        )
            SELECT			
                LABORATORIO_ID	,
                LABORATORIO_TEXT	,
                SISORIGEN_ID	,
                MANDANTE	,
                FECHA_CARGA	,
                ZONA_HORARIA		
            FROM			
                PRE.PDIM_MAT_LABORATORIO;
                
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_MAT_LABORATORIO;

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
    VALUES ('SP_CON_DIM_MAT_LABORATORIO','CON.DIM_MAT_LABORATORIO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
        CREATE OR REPLACE TABLE MIRRORING.DIM_MAT_LABORATORIO
        CLONE CON.DIM_MAT_LABORATORIO;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_MAT_LABORATORIO ON TABLE MIRRORING.DIM_MAT_LABORATORIO;
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);
 
END;
$$;
 
