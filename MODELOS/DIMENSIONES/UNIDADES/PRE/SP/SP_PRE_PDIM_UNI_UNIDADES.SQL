CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_UNI_UNIDADES()
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-08-19
 Creador:            Fidel Ramírez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_MON_MONEDA
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

        DELETE FROM PRE.PDIM_UNI_UNIDADES;

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

        INSERT INTO PRE.PDIM_UNI_UNIDADES
        (
            UNIDAD_ID,
            UNIDAD_TEXTO,
            UNIDAD_ID_TEXTO,
            MANDANTE,
            SISORIGEN_ID,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT DISTINCT
            COALESCE(U.VALOR_ESTANDARD, T."T006A~MSEHI") AS UNIDAD_ID,
            CASE
                WHEN "T006A~MSEHL" IS NOT NULL AND LENGTH(TRIM("T006A~MSEHL")) > 0
                    THEN "T006A~MSEHL"
                ELSE UPPER("T006A~MSEH3")
            END AS UNIDAD_TEXTO,
            COALESCE(COALESCE(U.VALOR_ESTANDARD, T."T006A~MSEHI"), '') || ' - ' ||
            COALESCE(
                CASE
                    WHEN "T006A~MSEHL" IS NOT NULL AND LENGTH(TRIM("T006A~MSEHL")) > 0
                        THEN TRIM("T006A~MSEHL")
                    ELSE UPPER(TRIM("T006A~MSEH3"))
                END
            ,'') AS UNIDAD_ID_TEXTO,
            "T006A~MANDT" AS MANDANTE,
            SISORIGEN_ID,
            FECHA_CARGA,
            ZONA_HORARIA
        FROM RAW.SQ1_TBL_T006A T
        LEFT JOIN (
            SELECT VALOR_ORIGINAL, VALOR_ESTANDARD
            FROM RAW.CAT_UNIDAD_ESTADISTICA
            WHERE UPPER(TRIM(VALOR_ORIGINAL)) = 'TO'     -- solo mapeamos TO → T
        ) U
        ON UPPER(TRIM(T."T006A~MSEHI")) = UPPER(TRIM(U.VALOR_ORIGINAL));
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_UNI_UNIDADES;

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
        VALUES('SP_PRE_PDIM_UNI_UNIDADES','PDIM_UNI_UNIDADES', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;

-- CALL PRE.SP_PRE_PDIM_UNI_UNIDADES();

-- select * from PRE.PDIM_UNI_UNIDADES;

-- select * from RAW.SQ1_TBL_T006A T;

-- select * from  RAW.CAT_UNIDAD_ESTADISTICA;