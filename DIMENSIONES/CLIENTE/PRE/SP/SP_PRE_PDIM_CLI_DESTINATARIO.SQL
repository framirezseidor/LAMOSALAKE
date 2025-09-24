CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_CLI_DESTINATARIO()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para DIM_CLI_DESTINATARIO
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

        DELETE FROM PRE.PDIM_CLI_DESTINATARIO;

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

        INSERT INTO PRE.PDIM_CLI_DESTINATARIO
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
            A.VKORG || '_' || A.VTWEG || '_' || A.SPART || '_' || LTRIM(A.KUNNR, '0') AS DESTINATARIO_ID,
            LTRIM(A.KUNNR, '0') AS DESTINATARIO,
            A.VKORG AS ORGVENTAS_ID,
            A.VTWEG AS CANALDISTRIB_ID,
            A.SPART AS SECTOR_ID,
            C.LIFNR AS COORDINADORDEST_ID, --LIFNR del asesor comercial
            V.LIFNR AS ASESORDEST_ID,
            T.TXTMD AS DESTINATARIO_TEXT,
            A.SISORIGEN_ID,
            A.MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM RAW.SQ1_EXT_0CUST_SALES_ATTR A
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY VKORG, VTWEG, SPART, KUNNR
                        ORDER BY LIFNR
                    ) AS rn
                FROM RAW.SQ1_TBL_KNVP
                WHERE PARVW = 'ZJ'
            )
            WHERE rn = 1
        ) C
            ON C.VKORG = A.VKORG
        AND C.VTWEG = A.VTWEG
        AND C.SPART = A.SPART
        AND LTRIM(C.KUNNR, '0') = LTRIM(A.KUNNR, '0')
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY VKORG, VTWEG, SPART, KUNNR
                        ORDER BY LIFNR
                    ) AS rn
                FROM RAW.SQ1_TBL_KNVP
                WHERE PARVW = 'ZV'
            )
            WHERE rn = 1
        ) V
            ON V.VKORG = A.VKORG
        AND V.VTWEG = A.VTWEG
        AND V.SPART = A.SPART
        AND LTRIM(V.KUNNR, '0') = LTRIM(A.KUNNR, '0')
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY VKORG, VTWEG, SPART, KUNNR
                        ORDER BY TXTMD
                    ) AS rn
                FROM RAW.SQ1_EXT_0CUST_SALES_TEXT
            )
            WHERE rn = 1
        ) T
            ON T.VKORG = A.VKORG
        AND T.VTWEG = A.VTWEG
        AND T.SPART = A.SPART
        AND LTRIM(T.KUNNR, '0') = LTRIM(A.KUNNR, '0')
        WHERE A.SPART = '00';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_CLI_DESTINATARIO;

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
    VALUES ('SP_PRE_PDIM_CLI_DESTINATARIO','PRE.PDIM_CLI_DESTINATARIO', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 



