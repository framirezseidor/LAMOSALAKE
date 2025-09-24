CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_FIN_PROGRAMAPRES()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_FIN_PROGRAMAPRES
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
        
        // Process TRUNCATE
        DELETE FROM PRE.PDIM_FIN_PROGRAMAPRES;
		
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
        
        // Process INSERT
        INSERT INTO PRE.PDIM_FIN_PROGRAMAPRES
        SELECT  PROGRAMAFILT.PROGRAMAPRES_ID,
                PROGRAMAFILT.PROGRAMAPRES,
                TEXTO.DESCRIPTION AS PROGRAMAPRES_TEXT,
                PROGRAMAFILT.ENTIDADCP_ID,
                PROGRAMAFILT.SISORIGEN_ID,
                PROGRAMAFILT.MANDANTE,
                PROGRAMAFILT.FECHA_CARGA,
                PROGRAMAFILT.ZONA_HORARIA
        FROM (
                SELECT  CONCAT(PROGRAMA.FMAREA,'_',PROGRAMA.MEASURE) AS PROGRAMAPRES_ID,
                        PROGRAMA.MEASURE AS PROGRAMAPRES,
                        PROGRAMA.FMAREA AS ENTIDADCP_ID,
                        PROGRAMA.SISORIGEN_ID,
                        PROGRAMA.MANDANTE,
                        PROGRAMA.FECHA_CARGA,
                        PROGRAMA.ZONA_HORARIA
                FROM    (SELECT DISTINCT *
                        FROM RAW.SQ1_EXT_0PU_MEASURE_ATTR) AS PROGRAMA
                WHERE ENTIDADCP_ID = 'FMGL'
        ) AS PROGRAMAFILT

        LEFT JOIN RAW.SQ1_EXT_0PU_MEASURE_TEXT AS TEXTO
        ON PROGRAMAFILT.PROGRAMAPRES_ID = CONCAT(TEXTO.FMAREA,'_',TEXTO.MEASURE)

        WHERE TEXTO.LANGUAGE = 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_FIN_PROGRAMAPRES;

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
    VALUES ('SP_PRE_PDIM_FIN_PROGRAMAPRES','PRE.PDIM_FIN_PROGRAMAPRES', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;