CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_CLI_ASESORCOMERCIAL()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-24
 Creador:            Fernando Cuellar M
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_CLI_ASESORCOMERCIAL
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

        DELETE FROM CON.DIM_CLI_ASESORCOMERCIAL;

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

        INSERT INTO CON.DIM_CLI_ASESORCOMERCIAL
        (
            ASESORCOMERCIAL_ID,
            FUNCION_INTERLOCUTOR,
            ASESORCOMERCIAL_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            ASESORCOMERCIAL_ID,
            FUNCION_INTERLOCUTOR,
            ASESORCOMERCIAL_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PDIM_CLI_ASESORCOMERCIAL;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_CLI_ASESORCOMERCIAL;

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
    VALUES ('SP_CON_DIM_CLI_ASESORCOMERCIAL','CON.DIM_CLI_ASESORCOMERCIAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
     --
        DELETE FROM MIRRORING.DIM_CLI_ASESORCOMERCIAL;

        INSERT INTO MIRRORING.DIM_CLI_ASESORCOMERCIAL
        (
            ASESORCOMERCIAL_ID,
            FUNCION_INTERLOCUTOR,
            ASESORCOMERCIAL_TEXT,
            ASESORCOMERCIAL_ID_TEXT,            
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            ASESORCOMERCIAL_ID,
            FUNCION_INTERLOCUTOR,
            ASESORCOMERCIAL_TEXT,
            CONCAT(ASESORCOMERCIAL_ID, ' - ', COALESCE(ASESORCOMERCIAL_TEXT,'') ) ASESORCOMERCIAL_ID_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM CON.DIM_CLI_ASESORCOMERCIAL;


    -------------------------------------------------------------------------------
    ---CLONNING

    CREATE OR ALTER TABLE MIRRORING.DIM_CLI_ASESORFACTURA
    CLONE MIRRORING.DIM_CLI_ASESORCOMERCIAL;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CLI_ASESORFACTURA ON TABLE MIRRORING.DIM_CLI_ASESORFACTURA;


    CREATE OR ALTER TABLE MIRRORING.DIM_CLI_ASESORPEDIDO
    CLONE MIRRORING.DIM_CLI_ASESORCOMERCIAL;

    CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_DIM_CLI_ASESORPEDIDO ON TABLE MIRRORING.DIM_CLI_ASESORPEDIDO;    
    ---------------------------------------------------------------------------------

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 

-- select * from  MIRRORING.DIM_CLI_ASESORCOMERCIAL
-- where length(ASESORCOMERCIAL_ID) <8;