CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_COM_ADH_VENTAS_PCP()
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

        TRUNCATE TABLE PRE.PFCT_COM_ADH_VENTAS_PCP;

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

        INSERT INTO PRE.PFCT_COM_ADH_VENTAS_PCP
        (
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
        )
        SELECT 
            LEFT(ANIOMES,4)	AS	ANIO	,
            RIGHT(ANIOMES,2)	AS	MES	,
            ANIOMES	AS	ANIOMES	,
            ORGVENTAS_ID	AS	SOCIEDAD_ID	,
            ORGVENTAS_ID	AS	ORGVENTAS_ID	,
            CANALDISTRIB_ID	AS	CANALDISTRIB_ID	,
            '00'	AS	SECTOR_ID	,
            CENTRO_ID	AS	CENTRO_ID	,
	        OFICINAVENTAS_ID ,            
            UENADHESIVOS_ID	AS	UENADHESIVOS_ID	,
            PAIS_ID	AS	PAIS_ID	,
            CONCAT(PAIS_ID,  '_', REGION_ID) AS	REGION_ID	,
            ASESORFACTURA_ID	AS	ASESORFACTURA_ID	,
            CLIENTE_ID	AS	CLIENTE_ID	,
            CONCAT(ORGVENTAS_ID , '_', CANALDISTRIB_ID, '_',' 00', '_', CLIENTE_ID)	AS	SOLICITANTE_ID	,
            CONCAT(ORGVENTAS_ID,  '_', CANALDISTRIB_ID, '_', '00', '_', DESTINATARIO_ID)	AS	DESTINATARIO_ID	,
            MATERIAL_ID	AS	MATERIAL_ID	,
            CONCAT(ORGVENTAS_ID, '_', CANALDISTRIB_ID, '_', MATERIAL_ID)	AS	MATERIALVENTAS_ID	,
            CONCAT(CENTRO_ID, '_', MATERIAL_ID)	AS	MATERIALCENTRO_ID	,
            IND_VENTA_PCP_TON	AS	IND_VENTA_PCP_TON	,
            COALESCE(U.VALOR_ESTANDARD, UNI_EST) AS UNI_EST,
            IND_VENTA_PCP_LOC	AS	IND_VENTA_PCP_LOC	,
            MON_LOC	AS	MON_LOC	,
            IND_VENTA_PCP_USD	AS	IND_VENTA_PCP_USD	,
            MON_USD	AS	MON_USD	,
            'FILE' 	AS	SISORIGEN_ID	,
            '' AS	MANDANTE	, 
            CURRENT_TIMESTAMP	AS	FECHA_CARGA	,
            RIGHT(CURRENT_TIMESTAMP,5)	AS	ZONA_HORARIA	
        FROM 
            RAW.FILE_XLS_COM_ADH_VENTAS_PCP  
            LEFT JOIN RAW.CAT_UNIDAD_ESTADISTICA AS U ON UPPER(TRIM(UNI_EST)) = UPPER(TRIM(U.VALOR_ORIGINAL));

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_COM_ADH_VENTAS_PCP;

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
    VALUES ('SP_PRE_PFCT_COM_ADH_VENTAS_PCP','PFCT_COM_ADH_VENTAS_PCP', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 

--  call PRE.SP_PRE_PFCT_COM_ADH_VENTAS_PCP();

--  select distinct uni_est from PRE.PFCT_COM_ADH_VENTAS_PCP;