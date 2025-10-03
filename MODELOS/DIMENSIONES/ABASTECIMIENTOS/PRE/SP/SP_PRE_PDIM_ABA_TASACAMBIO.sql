CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_ABA_TASACAMBIO()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para DIM_ABA_TASACAMBIO
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

        DELETE FROM PRE.PDIM_ABA_TASACAMBIO;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PDIM_ABA_TASACAMBIO
        (
            KURST,
            FCURR,
            TCURR,
            GDATU,
            SISORIGEN_ID,
            MANDANTE,
            ZONA_HORARIA,
            FECHA_CARGA,
            RATE,
            FFACT,
            TFACT
        )
        WITH FECHAS AS (
        SELECT 
            SEQ4() AS SEQ, 
            TO_DATE('2000-01-01') + SEQ4() AS GDATU 
        FROM 
            TABLE(GENERATOR(ROWCOUNT => 365*30)) 
        ),
        COMBINACIONES AS (
        SELECT DISTINCT 
            KURST, 
            FCURR, 
            TCURR,
            SISORIGEN_ID,
            MANDANTE,
            ZONA_HORARIA,
            FECHA_CARGA
        FROM 
            RAW.SQ1_EXT_Z_TCURR
        ),
        FECHAS_CON_COMBINACIONES AS (
        SELECT 
            C.KURST,
            C.FCURR,
            C.TCURR,
            C.SISORIGEN_ID,
            C.MANDANTE,
            C.ZONA_HORARIA,
            C.FECHA_CARGA,
            F.GDATU
        FROM 
            FECHAS F
        CROSS JOIN 
            COMBINACIONES C
        ),
        TASA_PREVIA AS (
        SELECT 
            KURST, 
            FCURR, 
            TCURR, 
            SISORIGEN_ID,
            MANDANTE,
            ZONA_HORARIA,
            FECHA_CARGA,
            TO_DATE(TO_VARCHAR(99999999 - TO_NUMBER(GDATU)), 'YYYYMMDD') AS GDATU,
            CASE 
                WHEN UKURS < 0 THEN (-10000000 / UKURS)/10000000
                ELSE UKURS
            END AS RATE, 
            FFACT, 
            TFACT
        FROM 
            RAW.SQ1_EXT_Z_TCURR
        ),
        TASA_COMPLETA AS (
        SELECT 
            F.GDATU,
            F.KURST, 
            F.FCURR, 
            F.TCURR, 
            F.SISORIGEN_ID,
            F.MANDANTE,
            F.ZONA_HORARIA,
            F.FECHA_CARGA,
            T.RATE,
            T.FFACT, 
            T.TFACT
        FROM 
            FECHAS_CON_COMBINACIONES F
        LEFT JOIN 
            TASA_PREVIA T
        ON 
            F.GDATU = T.GDATU
            AND F.KURST = T.KURST
            AND F.FCURR = T.FCURR
            AND F.TCURR = T.TCURR
        ),
        TASA_LIMPIA AS (
        SELECT 
            KURST, 
            FCURR, 
            TCURR, 
            GDATU, 
            SISORIGEN_ID,
            MANDANTE,
            ZONA_HORARIA,
            FECHA_CARGA,
            LAST_VALUE(RATE IGNORE NULLS) OVER (
            PARTITION BY KURST, FCURR, TCURR 
            ORDER BY GDATU 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS RATE, 
            FFACT, 
            TFACT
        FROM 
            TASA_COMPLETA
        ),
        MONEDAS_DISTINTAS AS (
        SELECT DISTINCT FCURR AS MONEDA FROM RAW.SQ1_EXT_Z_TCURR
        ),
        KURSTS AS (
        SELECT DISTINCT KURST FROM RAW.SQ1_EXT_Z_TCURR
        ),
        IDENTICAS AS (
        SELECT 
            K.KURST,
            M.MONEDA AS FCURR,
            M.MONEDA AS TCURR,
            F.GDATU,
            NULL AS SISORIGEN_ID,
            NULL AS MANDANTE,
            NULL AS ZONA_HORARIA,
            NULL AS FECHA_CARGA,
            1.0 AS RATE,
            1 AS FFACT,
            1 AS TFACT
        FROM 
            FECHAS F
        CROSS JOIN MONEDAS_DISTINTAS M
        CROSS JOIN KURSTS K
        )
        -- Unión de tasas reales y tasas 1:1
        SELECT 
        KURST,
        FCURR,
        TCURR,
        GDATU,
        SISORIGEN_ID,
        MANDANTE,
        ZONA_HORARIA,
        FECHA_CARGA,
        RATE,
        FFACT,
        TFACT
        FROM TASA_LIMPIA

        UNION ALL

        SELECT 
        KURST,
        FCURR,
        TCURR,
        GDATU,
        SISORIGEN_ID,
        MANDANTE,
        ZONA_HORARIA,
        FECHA_CARGA,
        RATE,
        FFACT,
        TFACT
        FROM IDENTICAS

        ORDER BY GDATU;
        ;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_ABA_TASACAMBIO;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
            RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_PRE_PDIM_ABA_TASACAMBIO','PRE.PRE.PDIM_ABA_TASACAMBIO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 