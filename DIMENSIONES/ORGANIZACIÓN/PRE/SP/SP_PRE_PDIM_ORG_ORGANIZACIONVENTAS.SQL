CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_ORG_ORGANIZACIONVENTAS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-28
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_ORG_ORGANIZACIONVENTAS
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

        DELETE FROM PRE.PDIM_ORG_ORGANIZACIONVENTAS;

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

        INSERT INTO PRE.PDIM_ORG_ORGANIZACIONVENTAS
        (
            ORGVENTAS_ID,
            SOCIEDAD_ID,
            PAIS_ID,
            NEGOCIOGRUPO_ID,
            ORGVENTAS_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
       SELECT
            IFNULL(ATR.VKORG,''),-- ORGVENTAS_ID,
            IFNULL(ATR.BUKRS,''),-- SOCIEDAD_ID,
            IFNULL(LAND1,''),-- PAIS_ID,
            IFNULL(CASE WHEN ATR.BUKRS LIKE 'A%' THEN 'A000'
                        WHEN ATR.BUKRS LIKE 'C%' THEN 'C000'
                        WHEN ATR.BUKRS LIKE 'R%' THEN 'R000' END,'' ),-- NEGOCIO_ID,
            IFNULL(TXT.VTEXT,''),-- ORGVENTAS_TEXT,
            ATR.SISORIGEN_ID,-- SISORIGEN_ID, 
            ATR.MANDANTE,-- MANDANTE, 
            CURRENT_TIMESTAMP,-- FECHA_CARGA, 
            RIGHT(CURRENT_TIMESTAMP,5)-- ZONA_HORARIA  
        FROM RAW.SQ1_EXT_0SALESORG_ATTR ATR
        LEFT JOIN RAW.SQ1_EXT_0SALESORG_TEXT TXT ON ATR.VKORG=TXT.VKORG
        WHERE TXT.SPRAS='S' ;
        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_ORG_ORGANIZACIONVENTAS;

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
        VALUES('SP_PRE_PDIM_ORG_ORGANIZACIONVENTAS','PDIM_ORG_ORGANIZACIONVENTAS', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;