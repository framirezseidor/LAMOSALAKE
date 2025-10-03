CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_ABA_TASACAMBIO_ANT()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_ABA_TASACAMBIO_ANT
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

        DELETE FROM CON.DIM_ABA_TASACAMBIO_ANT;

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

        INSERT INTO CON.DIM_ABA_TASACAMBIO_ANT
        (
            CLAVEMONEDA,
            FECHA_TIPOCAMBIO,
            MON_DES,
            MON_ORG,
            TASACAMBIO_ANT,
            TIPO_TASACAMBIO_ANT,
    
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        WITH FECHAS_ULTIMAS AS (
            SELECT 
                MAX(GDATU) AS FECHA_TIPOCAMBIO,
                DATE_TRUNC('MONTH', GDATU) AS MES,
                FCURR,
                TCURR
            FROM PRE.PDIM_ABA_TASACAMBIO
            WHERE KURST = 'M'
            GROUP BY DATE_TRUNC('MONTH', GDATU), FCURR, TCURR
        )
        SELECT 
            CONCAT(p.FCURR, '_', p.TCURR,'_',p.GDATU) AS CLAVEMONEDA,
            p.GDATU AS FECHA_TIPOCAMBIO,
            p.TCURR AS MON_DES,
            p.FCURR AS MON_ORG,
            p.RATE AS TASACAMBIO_ANT,
            p.KURST AS TIPO_TASACAMBIO_ANT,
            p.SISORIGEN_ID,
            p.MANDANTE,
            p.FECHA_CARGA,
            p.ZONA_HORARIA
        FROM PRE.PDIM_ABA_TASACAMBIO p
        JOIN FECHAS_ULTIMAS f
        ON p.GDATU = f.FECHA_TIPOCAMBIO
        AND p.FCURR = f.FCURR
        AND p.TCURR = f.TCURR
        WHERE p.KURST = 'M'
        ORDER BY p.FCURR, p.TCURR, p.GDATU DESC
        ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_ABA_TASACAMBIO_ANT;

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
        CREATE OR REPLACE TABLE MIRRORING.DIM_ABA_TASACAMBIO_ANT
        CLONE CON.DIM_ABA_TASACAMBIO_ANT;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_ABA_TASACAMBIO_ANT ON TABLE MIRRORING.DIM_ABA_TASACAMBIO_ANT;

    ---------------------------------------------------------------------------------
    -- STEP 4: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_DIM_ABA_TASACAMBIO_ANT','CON.DIM_ABA_TASACAMBIO_ANT', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 