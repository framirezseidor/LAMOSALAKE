CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_GEO_ZONATRANSPORTE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-15
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_GEO_ZONATRANSPORTE
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

        DELETE FROM CON.DIM_GEO_ZONATRANSPORTE;

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

        INSERT INTO CON.DIM_GEO_ZONATRANSPORTE
        (
            ZONATRANSPORTE_ID,
            PAIS_ID,
            ZONATRANSPORTE,
            ZONATRANSPORTE_TEXT,
            SISORIGEN_ID, 
            MANDANTE, 
            FECHA_CARGA, 
            ZONA_HORARIA 
        )
       SELECT
            ZONATRANSPORTE_ID,-- ZONATRANSPORTE_ID,
            PAIS_ID,-- PAIS_ID,
            ZONATRANSPORTE,-- ZONATRANSPORTE,
            ZONATRANSPORTE_TEXT,-- ZONATRANSPORTE_TEXT,
            Z.SISORIGEN_ID,-- SISORIGEN_ID, 
            Z.MANDANTE,-- MANDANTE, 
            CURRENT_TIMESTAMP,-- FECHA_CARGA, 
            RIGHT(CURRENT_TIMESTAMP,5)-- ZONA_HORARIA  
        FROM PRE.PDIM_GEO_ZONATRANSPORTE Z;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_GEO_ZONATRANSPORTE;

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
        VALUES('SP_CON_DIM_GEO_ZONATRANSPORTE','DIM_GEO_ZONATRANSPORTE', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
        DELETE FROM MIRRORING.DIM_GEO_ZONATRANSPORTE;

       INSERT INTO MIRRORING.DIM_GEO_ZONATRANSPORTE
        (
            ZONATRANSPORTE_ID,
            PAIS_ID,
            ZONATRANSPORTE,
            ZONATRANSPORTE_TEXT,
            ZONATRANSPORTE_ID_TEXT,
            PAIS_TEXT,            
            SISORIGEN_ID, 
            MANDANTE, 
            FECHA_CARGA, 
            ZONA_HORARIA 
        )
       SELECT
            Z.ZONATRANSPORTE_ID,
            Z.PAIS_ID,
            Z.ZONATRANSPORTE,
            Z.ZONATRANSPORTE_TEXT,
            CONCAT(Z.ZONATRANSPORTE_ID, ' - ', Z.ZONATRANSPORTE_TEXT) ZONATRANSPORTE_ID_TEXT,
            P.PAIS_TEXT,                  
            Z.SISORIGEN_ID, 
            Z.MANDANTE, 
            Z.FECHA_CARGA, 
            Z.ZONA_HORARIA 
        FROM CON.DIM_GEO_ZONATRANSPORTE Z
        LEFT JOIN CON.DIM_GEO_PAIS P
        ON Z.PAIS_ID = P.PAIS_ID;

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;