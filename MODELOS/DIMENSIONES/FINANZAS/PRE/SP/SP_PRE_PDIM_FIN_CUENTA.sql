CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_FIN_CUENTA()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_FIN_CUENTA
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
        DELETE FROM PRE.PDIM_FIN_CUENTA;
		
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
        INSERT INTO PRE.PDIM_FIN_CUENTA
        SELECT
            CONCAT(A.KOKRS,'_',LTRIM(A.KSTAR, '0')) AS CUENTA_ID,
            A.KOKRS AS SOCIEDADCO_ID,
            LTRIM(A.KSTAR, '0') AS CUENTA,
            A.TXTSH AS CUENTA_TEXT,
            LPAD(B.TEXTO, 3, '0') AS SUBDIVISION_ID,
            C.SUBDIVISION_TEXT AS SUBDIVISION_TEXT,
            A.SISORIGEN_ID,
            A.MANDANTE,
            A.FECHA_CARGA,
            A.ZONA_HORARIA
        FROM  RAW.SQ1_EXT_0COSTELMNT_TEXT AS A
        LEFT JOIN RAW.SQ1_EXT_ZSTXH AS B
        ON A.KSTAR = LEFT(TDNAME,10)
        AND A.KOKRS = 'CAGL'
        LEFT JOIN RAW.FILE_CSV_SUBDIVISION AS C
        ON LPAD(B.TEXTO, 3, '0') = LPAD(C.SUBDIVISION_ID, 3, '0')
        WHERE A.LANGU = 'S'
        GROUP BY ALL;

        INSERT INTO PRE.PDIM_FIN_CUENTA (CUENTA_ID, CUENTA_TEXT)
        VALUES ('DUMMY', 'DUMMY');

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_FIN_CUENTA;

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
    VALUES ('SP_PRE_PDIM_FIN_CUENTA','PRE.PDIM_FIN_CUENTA', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;