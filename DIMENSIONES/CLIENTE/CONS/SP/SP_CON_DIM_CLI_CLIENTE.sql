CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_CLI_CLIENTE()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_CLI_CLIENTE
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

        DELETE FROM CON.DIM_CLI_CLIENTE;

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

        INSERT INTO CON.DIM_CLI_CLIENTE
        (
            CLIENTE_ID,
            RAMO_ID,
            GRUPOSOLICITANTE_ID,
            PAIS_ID,
            REGION_ID,
            LOCALIDAD,
            RFC_ID,
            CLASIFICACIONCLIENTE_ID,
            CODIGOPOSTAL,
            CALLE,
            GRUPOCUENTAS_ID,
            CLIENTE_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            LTRIM(CLIENTE_ID,'0') CLIENTE_ID,
            RAMO_ID,
            GRUPOSOLICITANTE_ID,
            PAIS_ID,
            CONCAT(PAIS_ID, '_', REGION_ID) REGION_ID,
            LOCALIDAD,
            RFC_ID,
            CLASIFICACIONCLIENTE_ID,
            CODIGOPOSTAL,
            CALLE,
            GRUPOCUENTAS_ID,
            CLIENTE_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PDIM_CLI_CLIENTE;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_CLI_CLIENTE;

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
    VALUES ('SP_CON_DIM_CLI_CLIENTE','CON.DIM_CLI_CLIENTE', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_CLI_CLIENTE;

    INSERT INTO MIRRORING.DIM_CLI_CLIENTE
    (
        CLIENTE_ID,
        RAMO_ID,
        GRUPOSOLICITANTE_ID,
        PAIS_ID,
        REGION_ID,
        LOCALIDAD,
        RFC_ID,
        CLASIFICACIONCLIENTE_ID,
        CODIGOPOSTAL,
        CALLE,
        GRUPOCUENTAS_ID,
        CLIENTE_TEXT,
        CLIENTE_ID_TEXT,
        RAMO_TEXT,
        GRUPOSOLICITANTE_TEXT,
        GRUPOSOLICITANTE_ID_TEXT,
        PAIS_TEXT,
        CLASIFICACIONCLIENTE_TEXT,
        GRUPOCUENTAS_TEXT,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA
    )
        SELECT DISTINCT
            CLI.CLIENTE_ID,
            CLI.RAMO_ID,
            CLI.GRUPOSOLICITANTE_ID,
            CLI.PAIS_ID,
            CLI.REGION_ID,
            CLI.LOCALIDAD,
            CLI.RFC_ID,
            CLI.CLASIFICACIONCLIENTE_ID,
            CLI.CODIGOPOSTAL,
            CLI.CALLE,
            CLI.GRUPOCUENTAS_ID,
            CLI.CLIENTE_TEXT,
            CONCAT(CLI.CLIENTE_ID,' - ', CLI.CLIENTE_TEXT) CLIENTE_ID_TEXT,
            R.RAMO_TEXT,
            COALESCE(GS.GRUPOSOLICITANTE_TEXT,'') GRUPOSOLICITANTE_TEXT,
            CONCAT( COALESCE(CLI.GRUPOSOLICITANTE_ID,'') , ' - ', COALESCE(GS.GRUPOSOLICITANTE_TEXT,'') ) GRUPOSOLICITANTE_ID_TEXT,
            P.PAIS_TEXT,
            CC.CLASIFICACIONCLIENTE_TEXT,
            '' GRUPOCUENTAS_TEXT,
            CLI.SISORIGEN_ID,
            CLI.MANDANTE,
            CLI.FECHA_CARGA,
            CLI.ZONA_HORARIA
        FROM
            CON.DIM_CLI_CLIENTE CLI
            LEFT JOIN CON.DIM_CLI_RAMO R
            ON CLI.RAMO_ID = R.RAMO_ID 
            LEFT JOIN CON.DIM_CLI_GRUPOSOLICITANTE GS
            ON CLI.GRUPOSOLICITANTE_ID = GS.GRUPOSOLICITANTE_ID
            LEFT JOIN CON.DIM_GEO_PAIS P
            ON CLI.PAIS_ID = P.PAIS_ID
            LEFT JOIN CON.DIM_CLI_CLASIFICACIONCLIENTE CC
            ON CLI.CLASIFICACIONCLIENTE_ID = CC.CLASIFICACIONCLIENTE_ID
        ;



    --tabla cliente para deudor
        CREATE OR ALTER TABLE MIRRORING.DIM_CLI_CLIENTE_DEUDOR
        CLONE MIRRORING.DIM_CLI_CLIENTE;
 
        CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CLI_CLIENTE_DEUDOR ON TABLE MIRRORING.DIM_CLI_CLIENTE_DEUDOR;
    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);
END;
$$;
 
