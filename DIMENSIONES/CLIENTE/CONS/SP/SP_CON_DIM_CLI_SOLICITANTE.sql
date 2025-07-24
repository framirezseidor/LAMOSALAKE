CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_CLI_SOLICITANTE()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_CLI_SOLICITANTE
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

        DELETE FROM CON.DIM_CLI_SOLICITANTE;

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

        INSERT INTO CON.DIM_CLI_SOLICITANTE
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
            SOLICITANTE_ID,
            LTRIM(SOLICITANTE,'0') SOLICITANTE,
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
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM PRE.PDIM_CLI_SOLICITANTE;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_CLI_SOLICITANTE;

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
    VALUES ('SP_CON_DIM_CLI_SOLICITANTE','CON.DIM_CLI_SOLICITANTE', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_CLI_SOLICITANTE;

            INSERT INTO MIRRORING.DIM_CLI_SOLICITANTE
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
            SOLICITANTE_ID_TEXT,
            ORGVENTAS_TEXT,
            CANALDISTRIB_TEXT,
            SECTOR_TEXT,
            GRUPOCLIENTES_TEXT,
            ZONAVENTAS_TEXT,
            GRUPOVENDEDORES_TEXT,
            OFICINAVENTAS_TEXT,
            GRUPOCLIENTES1_TEXT,
            GRUPOCLIENTES2_TEXT,
            GRUPOCLIENTES3_TEXT,
            GRUPOCLIENTES4_TEXT,
            GRUPOCLIENTES5_TEXT,
            GRUPOIMPUTCLIENTE_TEXT,
            INDICADOR_ABC_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA,
            ZONA_HORARIA
        )
        SELECT
            SOL.SOLICITANTE_ID,
            SOL.SOLICITANTE,
            SOL.ORGVENTAS_ID,
            SOL.CANALDISTRIB_ID,
            SOL.SECTOR_ID,
            SOL.GRUPOCLIENTES_ID,
            SOL.ZONAVENTAS_ID,
            SOL.GRUPOVENDEDORES_ID,
            SOL.OFICINAVENTAS_ID,
            SOL.GRUPOCLIENTES1_ID,
            SOL.GRUPOCLIENTES2_ID,
            SOL.GRUPOCLIENTES3_ID,
            SOL.GRUPOCLIENTES4_ID,
            SOL.GRUPOCLIENTES5_ID,
            SOL.GRUPOIMPUTCLIENTE_ID,
            SOL.INDICADOR_ABC_ID,
            SOL.SOLICITANTE_TEXT,
            CONCAT(SOL.SOLICITANTE_ID, ' - ', SOL.SOLICITANTE_TEXT ) SOLICITANTE_ID_TEXT,
            OV.ORGVENTAS_TEXT,
            CD.CANALDISTRIB_TEXT,
            SC.SECTOR_TEXT,
            GC.GRUPOCLIENTES_TEXT,
            ZV.ZONAVENTAS_TEXT,
            GV.GRUPOVENDEDORES_TEXT,
            OFV.OFICINAVENTAS_TEXT,
            GC1.GRUPOCLIENTES1_TEXT,
            GC2.GRUPOCLIENTES2_TEXT,
            GC3.GRUPOCLIENTES3_TEXT,
            GC4.GRUPOCLIENTES4_TEXT,
            GC5.GRUPOCLIENTES5_TEXT,
            GI.GRUPOIMPUTCLIENTE_TEXT,
            ABC.INDICADOR_ABC_TEXT,
            SOL.SISORIGEN_ID,
            SOL.MANDANTE,
            CURRENT_TIMESTAMP() AS FECHA_CARGA,
            TO_CHAR(CURRENT_TIMESTAMP(), 'TZH:TZM') AS ZONA_HORARIA
        FROM CON.DIM_CLI_SOLICITANTE SOL
            LEFT JOIN CON.DIM_ORG_ORGANIZACIONVENTAS OV
            ON SOL.ORGVENTAS_ID = OV.ORGVENTAS_ID
            LEFT JOIN CON.DIM_ORG_CANALDISTRIBUCION CD
            ON SOL.CANALDISTRIB_ID = CD.CANALDISTRIB_ID
            LEFT JOIN CON.DIM_ORG_SECTOR SC
            ON SOL.SECTOR_ID = SC.SECTOR_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES GC
            ON SOL.GRUPOCLIENTES_ID = GC.GRUPOCLIENTES_ID
            LEFT JOIN CON.DIM_ORG_ZONAVENTAS ZV
            ON SOL.ZONAVENTAS_ID = ZV.ZONAVENTAS_ID
            LEFT JOIN CON.DIM_ORG_GRUPOVENDEDORES GV
            ON SOL.GRUPOVENDEDORES_ID = GV.GRUPOVENDEDORES_ID
            LEFT JOIN CON.DIM_ORG_OFICINAVENTAS OFV
            ON SOL.OFICINAVENTAS_ID = OFV.OFICINAVENTAS_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES1 GC1
            ON SOL.GRUPOCLIENTES1_ID = GC1.GRUPOCLIENTES1_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES2 GC2
            ON SOL.GRUPOCLIENTES2_ID = GC2.GRUPOCLIENTES2_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES3 GC3
            ON SOL.GRUPOCLIENTES3_ID = GC3.GRUPOCLIENTES3_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES4 GC4
            ON SOL.GRUPOCLIENTES4_ID = GC4.GRUPOCLIENTES4_ID
            LEFT JOIN CON.DIM_CLI_GRUPOCLIENTES5 GC5
            ON SOL.GRUPOCLIENTES5_ID = GC5.GRUPOCLIENTES5_ID
            LEFT JOIN CON.DIM_CLI_GRUPOIMPUTCLIENTE GI
            ON SOL.GRUPOIMPUTCLIENTE_ID = GI.GRUPOIMPUTCLIENTE_ID 
            LEFT JOIN CON.DIM_MAT_INDICADOR_ABC ABC
            ON SOL.INDICADOR_ABC_ID = ABC.INDICADOR_ABC_ID

        ;

     ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED); 
END;
$$;
 


 
