CREATE OR REPLACE PROCEDURE LAMOSALAKE_DEV.PRE.SP_PRE_PFCT_OPE_REV_PROD()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-02
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_OPE_REV_PROD
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en DELETE: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD
        (
            -- Lista de columnas destino en tabla PRE
            -- Ejemplo: ID, FECHA, CLIENTE_ID, MONTO
        )
        SELECT
            -- Transformaciones desde RAW
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
        FROM LAMOSALAKE_DEV.RAW.SQ1_MOF_ZBWPP_CALDISENO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM LAMOSALAKE_DEV.PRE.PFCT_OPE_REV_PROD;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            RETURN 'Error en INSERT: ' || :sqlerrm;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 