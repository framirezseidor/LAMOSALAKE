CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_CLI_SOLICITANTE()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para DIM_CLI_SOLICITANTE
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

        DELETE FROM PRE.PDIM_CLI_SOLICITANTE;

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

        INSERT INTO PRE.PDIM_CLI_SOLICITANTE
        (
            SOLICITANTE_ID,
            SOLICITANTE,
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            SECTOR_ID,
            GRUPOCLIENTES_ID,
            ZONAVENTAS_ID,
            GRUPOVENDEDORES_ID,
            OFICINAVENTAS_ID,
            GRUPOCLIENTES1_ID,
            GRUPOCLIENTES2_ID,
            GRUPOCLIENTES3_ID,
            GRUPOCLIENTES4_ID,
            GRUPOCLIENTES5_ID,
            GRUPOIMPUTCLIENTE_ID,
            INDICADOR_ABC_ID,
            SOLICITANTE_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            A.VKORG || '_' || A.VTWEG || '_' || A.SPART || '_' || LTRIM(A.KUNNR, '0') AS SOLICITANTE_ID,
            LTRIM(A.KUNNR, '0') AS SOLICITANTE,
            A.VKORG AS ORGVENTAS_ID,
            A.VTWEG AS CANALDISTRIB_ID,
            A.SPART AS SECTOR_ID,
            A.KDGRP AS GRUPOCLIENTES_ID,
            A.BZIRK AS ZONAVENTAS_ID,
            A.VKGRP AS GRUPOVENDEDORES_ID,
            A.VKBUR AS OFICINAVENTAS_ID,
            A.KVGR1 AS GRUPOCLIENTES1_ID,
            A.KVGR2 AS GRUPOCLIENTES2_ID,
            A.KVGR3 AS GRUPOCLIENTES3_ID,
            A.KVGR4 AS GRUPOCLIENTES4_ID,
            A.KVGR5 AS GRUPOCLIENTES5_ID,
            A.KTGRD AS GRUPOIMPUTCLIENTE_ID,
            A.KLABC AS INDICADOR_ABC_ID,
            T.TXTMD AS SOLICITANTE_TEXT,
            A.SISORIGEN_ID,
            A.MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM RAW.SQ1_EXT_0CUST_SALES_ATTR A
        LEFT JOIN RAW.SQ1_EXT_0CUST_SALES_TEXT T
            ON T.VKORG = A.VKORG
        AND T.VTWEG = A.VTWEG
        AND T.SPART = A.SPART
        AND T.KUNNR = A.KUNNR
        WHERE A.SPART = '00';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_CLI_SOLICITANTE;

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
    VALUES ('SP_PRE_PDIM_CLI_SOLICITANTE','PRE.PDIM_CLI_SOLICITANTE', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
