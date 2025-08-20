CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_FIN_SOCIEDADGLASOC()
RETURNS VARCHAR(1000)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-28
 Creador:            Juan Pedreros
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_FIN_SOCIEDADGLASOC
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

        DELETE FROM CON.DIM_FIN_SOCIEDADGLASOC;

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

        INSERT INTO CON.DIM_FIN_SOCIEDADGLASOC
        (
            SOCIEDADGLASOC_ID,
            PAIS_ID,
            NEGOCIOGRUPO_ID,
            SOCIEDADGLASOC_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
       SELECT
            SOCIEDAD_ID AS SOCIEDADGLASOC_ID,
            PAIS_ID,
            NEGOCIOGRUPO_ID,
            SOCIEDAD_TEXT AS SOCIEDADGLASOC_TEXT,
            Z.SISORIGEN_ID, 
            Z.MANDANTE, 
            CURRENT_TIMESTAMP,
            RIGHT(CURRENT_TIMESTAMP,5)  
        FROM PRE.PDIM_ORG_SOCIEDAD Z;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_FIN_SOCIEDADGLASOC;

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
        VALUES('SP_CON_DIM_FIN_SOCIEDADGLASOC','DIM_FIN_SOCIEDADGLASOC', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_FIN_SOCIEDADGLASOC;

    INSERT INTO MIRRORING.DIM_FIN_SOCIEDADGLASOC
    (
        SOCIEDADGLASOC_ID,
        PAIS_ID,
        NEGOCIOGRUPO_ID,
        SOCIEDADGLASOC_TEXT,
        SOCIEDADGLASOC_ID_TEXT,
        PAIS_TEXT,
        NEGOCIOGRUPO_TEXT,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA
    )
    SELECT 
        S.SOCIEDADGLASOC_ID,
        S.PAIS_ID,
        S.NEGOCIOGRUPO_ID,
        S.SOCIEDADGLASOC_TEXT,
        CONCAT(S.SOCIEDADGLASOC_ID, ' - ', S.SOCIEDADGLASOC_TEXT) AS SOCIEDADGLASOC_ID_TEXT,
        P.PAIS_TEXT,
        COALESCE(NG.NEGOCIOGRUPO_TEXT,'') NEGOCIOGRUPO_TEXT,        
        S.SISORIGEN_ID,
        S.MANDANTE,
        S.FECHA_CARGA,
        S.ZONA_HORARIA
    FROM CON.DIM_FIN_SOCIEDADGLASOC S
        LEFT JOIN CON.DIM_GEO_PAIS P
        ON  S.PAIS_ID = P.PAIS_ID
        LEFT JOIN CON.DIM_ORG_NEGOCIOGRUPO NG
        ON S.NEGOCIOGRUPO_ID = NG.NEGOCIOGRUPO_ID
        ;

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;