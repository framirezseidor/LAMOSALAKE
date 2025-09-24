CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_ABA_GRUPOPROVEEDOR()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-24
 Creador:            Juan Méndez
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_ABA_GRUPOPROVEEDOR
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

        DELETE FROM CON.DIM_ABA_GRUPOPROVEEDOR;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO CON.DIM_ABA_GRUPOPROVEEDOR
        (
            GRUPOPROVEEDOR_ID,
            GRUPOPROVEEDOR_TEXT,
    
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            GRUPOPROVEEDOR_ID,
            GRUPOPROVEEDOR_TEXT,

            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA

        FROM PRE.PDIM_ABA_GRUPOPROVEEDOR;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_ABA_GRUPOPROVEEDOR;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: CLONNING
    ---------------------------------------------------------------------------------
 
    --CONDICION EXPEDICION
        CREATE OR REPLACE TABLE MIRRORING.DIM_ABA_GRUPOPROVEEDOR
        AS SELECT CONCAT(GRUPOPROVEEDOR_ID,' - ',GRUPOPROVEEDOR_TEXT) AS GRUPOPROVEEDOR_ID_TEXT,* 
        FROM CON.DIM_ABA_GRUPOPROVEEDOR;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_ABA_GRUPOPROVEEDOR ON TABLE MIRRORING.DIM_ABA_GRUPOPROVEEDOR;

    ---------------------------------------------------------------------------------
    -- STEP 4: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_DIM_ABA_GRUPOPROVEEDOR','CON.DIM_ABA_GRUPOPROVEEDOR', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 