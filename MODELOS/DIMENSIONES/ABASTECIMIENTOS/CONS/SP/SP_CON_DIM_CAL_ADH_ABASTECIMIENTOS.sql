CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_CAL_ADH_ABASTECIMIENTOS()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_CAL_ADH_ABASTECIMIENTOS
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

        DELETE FROM CON.DIM_CAL_ADH_ABASTECIMIENTOS;

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

        INSERT INTO CON.DIM_CAL_ADH_ABASTECIMIENTOS
        (
        FECHA,
        ANIO,
        MES,
        DIA,
        ANIOMES,
        DIA_NOMBRE,
        MES_NOMBRE,
        TRIMESTRE,
        SEMANA_NUMERO,
        FECHA_TEXTO,
        ULTIMO_DIA_MES
        )
        SELECT DISTINCT
        FECHA_ORDEN AS FECHA,
        TO_VARCHAR(EXTRACT(YEAR FROM FECHA_ORDEN)) AS ANIO,
        TO_VARCHAR(EXTRACT(MONTH FROM FECHA_ORDEN)) AS MES,
        TO_VARCHAR(EXTRACT(DAY FROM FECHA_ORDEN)) AS DIA,
        TO_CHAR(FECHA_ORDEN, 'YYYY-MM') AS ANIOMES,

        CASE DAYOFWEEK(FECHA_ORDEN)
            WHEN 1 THEN 'Dom'
            WHEN 2 THEN 'Lun'
            WHEN 3 THEN 'Mar'
            WHEN 4 THEN 'Mié'
            WHEN 5 THEN 'Jue'
            WHEN 6 THEN 'Vie'
            WHEN 7 THEN 'Sáb'
        END AS DIA_NOMBRE,

        CASE EXTRACT(MONTH FROM FECHA_ORDEN)
            WHEN 1 THEN 'Ene'
            WHEN 2 THEN 'Feb'
            WHEN 3 THEN 'Mar'
            WHEN 4 THEN 'Abr'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'Jun'
            WHEN 7 THEN 'Jul'
            WHEN 8 THEN 'Ago'
            WHEN 9 THEN 'Sep'
            WHEN 10 THEN 'Oct'
            WHEN 11 THEN 'Nov'
            WHEN 12 THEN 'Dic'
        END AS MES_NOMBRE,

        CONCAT('Q', EXTRACT(QUARTER FROM FECHA_ORDEN)) AS TRIMESTRE,

        TO_VARCHAR(WEEK(FECHA_ORDEN)) AS SEMANA_NUMERO,

        CONCAT(
            LPAD(DAY(FECHA_ORDEN), 2, '0'), ' ',
            CASE EXTRACT(MONTH FROM FECHA_ORDEN)
                WHEN 1 THEN 'Ene'
                WHEN 2 THEN 'Feb'
                WHEN 3 THEN 'Mar'
                WHEN 4 THEN 'Abr'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'Jun'
                WHEN 7 THEN 'Jul'
                WHEN 8 THEN 'Ago'
                WHEN 9 THEN 'Sep'
                WHEN 10 THEN 'Oct'
                WHEN 11 THEN 'Nov'
                WHEN 12 THEN 'Dic'
            END, ' ',
            EXTRACT(YEAR FROM FECHA_ORDEN)
        ) AS FECHA_TEXTO,

        LAST_DAY(FECHA_ORDEN) AS ULTIMO_DIA_MES

        FROM CON.FCT_LOG_ADH_ABASTECIMIENTOS
        WHERE FECHA_ORDEN IS NOT NULL;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_CAL_ADH_ABASTECIMIENTOS;

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
        CREATE OR REPLACE TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS
        CLONE CON.DIM_CAL_ADH_ABASTECIMIENTOS;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CAL_ADH_ABASTECIMIENTOS ON TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS;
    --FECHA ACTUAL
        CREATE OR REPLACE TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS_ACT AS
        
        SELECT DISTINCT
            ANIO,
            MES,
            ANIOMES,
            MES_NOMBRE,
            TRIMESTRE,
            TO_DATE(TO_CHAR(FECHA, 'YYYY-MM') || '-01') AS FECHA
        FROM CON.DIM_CAL_ADH_ABASTECIMIENTOS;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CAL_ADH_ABASTECIMIENTOS_ACT ON TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS_ACT;
    
    --FECHA REFERENCIA
        CREATE OR REPLACE TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS_REF 
        CLONE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS_ACT;

        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CAL_ADH_ABASTECIMIENTOS_REF ON TABLE MIRRORING.DIM_CAL_ADH_ABASTECIMIENTOS_REF;
    
    ---------------------------------------------------------------------------------
    -- STEP 4: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_DIM_CAL_ADH_ABASTECIMIENTOS','CON.DIM_CAL_ADH_ABASTECIMIENTOS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
 