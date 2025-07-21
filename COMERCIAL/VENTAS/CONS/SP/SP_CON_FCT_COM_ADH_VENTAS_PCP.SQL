CREATE OR REPLACE PROCEDURE CON.SP_CON_FCT_COM_ADH_VENTAS_PCP()
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-24
 Creador:            Fidel Ramírez
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_COM_VENTAS
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

        TRUNCATE TABLE CON.FCT_COM_ADH_VENTAS_PCP;

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

        INSERT INTO CON.FCT_COM_ADH_VENTAS_PCP
        (
            ANIO	,
            MES	,
            ANIOMES	,
            SOCIEDAD_ID	,
            ORGVENTAS_ID	,
            CANALDISTRIB_ID	,
            SECTOR_ID	,
            CENTRO_ID	,
            OFICINAVENTAS_ID ,
            UENADHESIVOS_ID	,
            PAIS_ID	,
            REGION_ID	,
            ASESORFACTURA_ID	,
            CLIENTE_ID	,
            SOLICITANTE_ID	,
            DESTINATARIO_ID	,
            MATERIAL_ID	,
            MATERIALVENTAS_ID	,
            MATERIALCENTRO_ID	,
            IND_VENTA_PCP_TON	,
            UNI_EST	,
            IND_VENTA_PCP_LOC	,
            MON_LOC	,
            IND_VENTA_PCP_USD	,
            MON_USD	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA
        )
        SELECT 
            ANIO	,
            MES	,
            ANIOMES	,
            SOCIEDAD_ID	,
            ORGVENTAS_ID	,
            CANALDISTRIB_ID	,
            SECTOR_ID	,
            CENTRO_ID	,
            OFICINAVENTAS_ID,
            UENADHESIVOS_ID	,
            PAIS_ID	,
            REGION_ID	,
            ASESORFACTURA_ID	,
            CLIENTE_ID	,
            SOLICITANTE_ID	,
            DESTINATARIO_ID	,
            MATERIAL_ID	,
            MATERIALVENTAS_ID	,
            MATERIALCENTRO_ID	,
            IND_VENTA_PCP_TON	,
            UNI_EST	,
            IND_VENTA_PCP_LOC	,
            MON_LOC	,
            IND_VENTA_PCP_USD	,
            MON_USD	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA
        FROM 
            PRE.PFCT_COM_ADH_VENTAS_PCP;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.FCT_COM_ADH_VENTAS_PCP;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_FCT_COM_ADH_VENTAS_PCP','FCT_COM_ADH_VENTAS_PCP', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 

 call CON.SP_CON_FCT_COM_ADH_VENTAS_PCP();