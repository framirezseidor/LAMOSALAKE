CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_CLI_DESTINATARIO()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_CLI_DESTINATARIO
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

        DELETE FROM CON.DIM_CLI_DESTINATARIO;

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

        INSERT INTO CON.DIM_CLI_DESTINATARIO
        (
            DESTINATARIO_ID,
            DESTINATARIO,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            COORDINADORDEST_ID,
            ASESORDEST_ID,
            DESTINATARIO_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            DESTINATARIO_ID,
            LTRIM(DESTINATARIO,'0') DESTINATARIO,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            LTRIM(COALESCE(COORDINADORDEST_ID,''),'0') COORDINADORDEST_ID,
            LTRIM(COALESCE(ASESORDEST_ID,''),'0') ASESORDEST_ID,
            DESTINATARIO_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PDIM_CLI_DESTINATARIO;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_CLI_DESTINATARIO;

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
    VALUES ('SP_CON_DIM_CLI_DESTINATARIO','CON.DIM_CLI_DESTINATARIO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_CLI_DESTINATARIO;

    INSERT INTO MIRRORING.DIM_CLI_DESTINATARIO
        (
            DESTINATARIO_ID	,
            DESTINATARIO	,
            ORGVENTAS_ID	,
            CANALDISTRIB_ID	,
            SECTOR_ID	,
            COORDINADORDEST_ID	,
            ASESORDEST_ID	,
            DESTINATARIO_TEXT	,
            DESTINATARIO_ID_TEXT	,
            COORDINADORDEST_TEXT	,
            COORDINADORDEST_ID_TEXT	,
            ASESORDEST_TEXT	,
            ASESORDEST_ID_TEXT	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA	
        )
        SELECT
            DES.DESTINATARIO_ID	,
            DES.DESTINATARIO	,
            DES.ORGVENTAS_ID	,
            DES.CANALDISTRIB_ID	,
            DES.SECTOR_ID	,
            DES.COORDINADORDEST_ID	,
            DES.ASESORDEST_ID	,
            DES.DESTINATARIO_TEXT	,
            CONCAT(DES.DESTINATARIO_ID, ' - ', DES.DESTINATARIO_TEXT ) DESTINATARIO_ID_TEXT	,
            CC.COORDINADORCOMERCIAL_TEXT COORDINADORDEST_TEXT	,
            CONCAT(COALESCE(DES.COORDINADORDEST_ID,'') , ' - ',  COALESCE(CC.COORDINADORCOMERCIAL_TEXT,'') )  COORDINADORDEST_ID_TEXT	,
            AC.ASESORCOMERCIAL_TEXT ASESORDEST_TEXT	,
            CONCAT(COALESCE(DES.ASESORDEST_ID,'') , ' - ', COALESCE(AC.ASESORCOMERCIAL_TEXT,'') ) ASESORDEST_ID_TEXT	,
            DES.SISORIGEN_ID	,
            DES.MANDANTE	,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM CON.DIM_CLI_DESTINATARIO DES
            LEFT JOIN CON.DIM_CLI_COORDINADORCOMERCIAL CC
            ON DES.COORDINADORDEST_ID = CC.COORDINADORCOMERCIAL_ID
            LEFT JOIN CON.DIM_CLI_ASESORCOMERCIAL AC
            ON DES.ASESORDEST_ID = AC.ASESORCOMERCIAL_ID
        ;

    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
--  call CON.SP_CON_DIM_CLI_DESTINATARIO();

--  SELECT * FROM MIRRORING.DIM_CLI_DESTINATARIO;