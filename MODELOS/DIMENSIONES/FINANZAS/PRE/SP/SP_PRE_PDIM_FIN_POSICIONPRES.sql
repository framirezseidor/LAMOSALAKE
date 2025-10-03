CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_FIN_POSICIONPRES()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_FIN_POSICIONPRES
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
        DELETE FROM PRE.PDIM_FIN_POSICIONPRES;
		
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
        INSERT INTO PRE.PDIM_FIN_POSICIONPRES
        SELECT  CONCAT(POSICIONPRES.FIKRS,'_',POSICIONPRES.FIPEX) AS POSICIONPRES_ID,
                POSICIONPRES.FIPEX AS POSICIONPRES,
                TEXTO.TXTLG AS POSICIONPRES_TEXT,
                POSICIONPRES.FIKRS AS ENTIDADCP_ID,
                POSICIONPRES.SISORIGEN_ID,
                POSICIONPRES.MANDANTE,
                POSICIONPRES.FECHA_CARGA,
                POSICIONPRES.ZONA_HORARIA
        FROM RAW.SQ1_EXT_0CMMT_ITEM_PU_ATTR AS POSICIONPRES

        LEFT JOIN RAW.SQ1_EXT_0CMMT_ITEM_PU_TEXT AS TEXTO
        ON POSICIONPRES.FIPEX = TEXTO.FIPEX
        WHERE LANGU = 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_FIN_POSICIONPRES;

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
    VALUES ('SP_PRE_PDIM_FIN_POSICIONPRES','PRE.PDIM_FIN_POSICIONPRES', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;