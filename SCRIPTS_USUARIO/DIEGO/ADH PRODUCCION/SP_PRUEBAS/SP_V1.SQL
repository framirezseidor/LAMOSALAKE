CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_OPE_PROD_ADH()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-06-09
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PFCT_OPE_ADH_PROD
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);
    /* 
     ------------ VARIABLES DE ENTORNO ------------ 
	ID varchar(250)					:= NULL;
    D_PROCESO VARCHAR(250)	        :=''SP_PFCT_OPE_PROD_ADH_DELTA'';
	D_TABLAAFECTADA VARCHAR(250)    := NULL;
	D_RESULTADO VARCHAR(10)         := NULL;
	D_OPERACION VARCHAR(10)		    := NULL;
	N_REGISTROS NUMBER(38,0)        := NULL;
	NM_TIPO_LOG VARCHAR(100)        := NULL;
	F_INICIO TIMESTAMP_NTZ(9)       := NULL;
	F_FIN TIMESTAMP_NTZ(9)          := NULL;
	T_EJECUCION NUMBER(38,0)   		:= NULL;
	D_MENSAJE VARCHAR(250)          := NULL;
*/
BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM PRE.PFCT_OPE_REV_PROD;

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

        INSERT INTO PRE.PFCT_OPE_ADH_PROD
        (
            -- Lista de columnas destino en tabla PRE
            -- Ejemplo: ID, FECHA, CLIENTE_ID, MONTO
        ANIO ,
        MES,
        ANIOMES ,
        SOCIEDAD_ID,
        CENTRO_ID, --,
        ALMACEN_ID,
        CENTRO_COSTO_ID , 
        SOCIEDADCO_ID,
        MATERIAL_ID ,
        LOTE ,
        CLASEORDFAB_ID,
        CLASEMOVIMIENTO,
        ORDENFABRICACION_ID ,
        CLAVEMODELO,
        SOLICITANTE,
        IND_INVFABRICACION_UMB ,
        IND_INVFABRICACION_UME ,
        IND_PRODTERMINADA_UME ,
        ID_CANTIDAD ,
        IND_CONSUMO_UMB ,
        IND_INVPRODUCCION_UME ,
        IND_EMBARCADO_UME ,
        IND_AJUSTEINVENTARIO_UME ,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA ,
        ZONA_HORARIA,
        )
        SELECT
            -- Transformaciones desde RAW
            -- Ejemplo: COL1, COL2, CAST(COL3 AS DATE), ...
        YEAR(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD')) AS ANIO,
        MONTH(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD')) AS MES,
        TO_CHAR(TO_DATE(E2LISBF.BUDAT, 'YYYYMMDD'), 'YYYYMM') AS ANIOMES, 
        E2LISBF.BUKRS AS SOCIEDAD_ID,
        E2LISBF.WERKS AS CENTRO_ID,
        --E2LISBF.LGORT AS ALMACEN_ID,--RUTINA INICIAL- 0STOR_LOC --revisado
        E2LISBF.KOSTL AS CENTRO_COSTO_ID,
        E2LISBF.KOKRS AS SOCIEDADCO_ID,
        E2LISBF.MATNR AS MATERIAL_ID,
        E2LISBF.CHARG AS LOTE,
        E2LISBF.AUART AS CLASEORDFAB_ID, -- NO SE ASIGNA DIRECTAMENTE DE AUART SE MODIFICA EN LA RUTINA FINAL -- REVISADO
        E2LISBF.BWART AS CLASEMOVIMIENTO,
        E2LISBF.AUFNR AS ORDENFABRICACION_ID, -- Se usa en rutina, se cambia al nombre 0PRODORDER. También se usa en 0COORD_TYPE. SE MODIFICA EN RUTINA INICIAL. -revisado está en la logica de 0STOR_LOC (ALMACEN_ID)
        E2LISBF AS CLAVEMODELO, --Trae el valor de ZPP_KTSCH de una tabla auxiliar según el pedido y centro. SE MODIFICA EN RUTINA FINAL. REVISADO
        E2LISBF AS PUESTOTRABAJO_ID,--OJO NO HAY LOGICA?
        E2LISBF.KUNNR AS SOLICITANTE_ID,
        E2LISBF.ZZLABST AS IND_INVFABRICACION_UMB,
        E2LISBF.ZZLABST AS IND_INVFABRICACION_UME,-- resultado de convertir ZPPIFUB mediante un factor de conversión  basado en la unidad del material y el centro. SE MODIFICA EN RUTINA INICIAL. -revisado
        --E2LISBF AS IND_PRODTERMINADA_UME, -- FORMULA 
        CASE 
            WHEN E2LISBF.LGORT IN ('NC01', 'NC02', 'PA01') THEN 
                CASE 
                    WHEN E2LISBF.SHKZG = 'S' THEN IND_PRODTERMINADA_UME * -1
                    ELSE IND_PRODTERMINADA_UME
            ELSE 
                CASE
                    WHEN E2LISBF.SHKZG = 'H' THEN IND_PRODTERMINADA_UME * -1
                    ELSE IND_PRODTERMINADA_UME
                END
        END AS IND_PRODTERMINADA_UME -- VERIFICAR FORMULA CON LA RUTINA INICIAL REVISADO
        --E2LISBF.MENGE AS ID_CANTIDAD, -- También se usa en formula, se cambia al nombre 0QUANT_B. FORMULA
        CASE 
            WHEN E2LISBF.LGORT  IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'S' THEN E2LISBF.MENGE * -1
            WHEN E2LISBF.LGORT  IN ('NC01', 'NC02', 'PA01') AND E2LISBF.SHKZG = 'H' THEN E2LISBF.MENGE * -1
            ELSE E2LISBF.MENGE
        END AS ID_CANTIDAD
        E2LISBF AS  IND_CONSUMO_UMB, --NUEVO ESPERAR FORMULA
        E2LISBF AS IND_INVPRODUCCION_UME,--NUEVO ESPERAR FORMULA
        E2LISBF AS IND_EMBARCADO_UME,--NUEVO ESPERAR FORMULA
        E2LISBF AS IND_AJUSTEINVENTARIO_UME,--NUEVO ESPERAR FORMULA
        E2LISBF.SISORIGEN_ID,
        E2LISBF.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA
    FROM RAW.SQ1_EXT_2LIS_03_BF AS E2LISBF
    LEFT JOIN ZPP_NOTIFICACIONES AS NOTIFICACIONES
        ON E2LISBF.MBLNR = NOTIFICACIONES.MBLNR
        AND E2LISBF.MJAHR = NOTIFICACIONES.MJAHR
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_OPE_ADH_PROD;

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
    VALUES ('SP_PRE_PFCT_OPE_PROD','PRE.PFCT_OPE_ADH_PROD', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 