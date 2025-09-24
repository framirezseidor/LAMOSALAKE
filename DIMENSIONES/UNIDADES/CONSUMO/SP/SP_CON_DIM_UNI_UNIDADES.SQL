CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_UNI_UNIDADES()
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

        DELETE FROM CON.DIM_UNI_UNIDADES;

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

        INSERT INTO CON.DIM_UNI_UNIDADES
        (
            UNIDAD_ID ,
            UNIDAD_TEXTO ,
            UNIDAD_ID_TEXTO,
            MANDANTE ,
            SISORIGEN_ID ,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
       SELECT 
            UNIDAD_ID ,
            UNIDAD_TEXTO ,
            UNIDAD_ID_TEXTO,
            MANDANTE ,
            SISORIGEN_ID ,
            FECHA_CARGA ,
            ZONA_HORARIA
        FROM PRE.PDIM_UNI_UNIDADES ;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_UNI_UNIDADES;

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
        VALUES('SP_CON_DIM_UNI_UNIDADES','DIM_UNI_UNIDADES', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );



    ---------------------------------------------------------------------------------
    -- STEP 5: CLONNING
    ---------------------------------------------------------------------------------
    CREATE OR REPLACE TABLE MIRRORING.DIM_UNI_EST
    CLONE CON.DIM_UNI_UNIDADES;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_UNI_EST ON TABLE MIRRORING.DIM_UNI_EST;


    CREATE OR REPLACE TABLE MIRRORING.DIM_UNI_VTA
    CLONE CON.DIM_UNI_UNIDADES;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_UNI_VTA ON TABLE MIRRORING.DIM_UNI_VTA;


    CREATE OR REPLACE TABLE MIRRORING.DIM_UNI_VTAPESO
    CLONE CON.DIM_UNI_UNIDADES;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_UNI_VTAPESO ON TABLE MIRRORING.DIM_UNI_VTAPESO;


    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;

-- TRUNCATE TABLE CON.DIM_UNI_UNIDADES;
-- CALL CON.SP_CON_DIM_UNI_UNIDADES();

-- SELECT * FROM CON.DIM_UNI_UNIDADES;


select * from CON.DIM_UNI_UNIDADES
where UNIDAD_ID IN ('BTO','CJ');