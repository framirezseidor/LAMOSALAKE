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

        DELETE FROM PRE.PFCT_OPE_ADH_PROD;

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
        ZONA_HORARIA
        )
        WITH FILTRADO_INICIAL AS (
            SELECT * 
            FROM RAW.SQ1_EXT_2LIS_03_BF
            WHERE NOT (
                (LEFT(WERKS, 1) = 'R' AND LGORT BETWEEN 'FB01' AND 'FB02' AND BWART BETWEEN '261' AND '262' AND CHARG = '') OR
                (LEFT(WERKS, 1) = 'A' AND LGORT BETWEEN 'PT01' AND 'PT02' AND BWART BETWEEN '261' AND '262')
            )
        ),
        VALIDADOS AS (
            SELECT 
            FI.*,
            N.AUFNR ,
            N.MKPF_BUDAT ,
            N.AFRU_BUDAT 
            FROM FILTRADO_INICIAL FI
            LEFT JOIN RAW.ZPP_NOTIFICACIONES N
                ON FI.MBLNR = N.MBLNR AND FI.MJAHR = N.MJAHR
        ),
        FILTRADO_FINAL AS (
            SELECT *
            FROM VALIDADOS
            WHERE AUFNR IS NOT NULL
        )
        CON_UNIDAD AS ( --
            SELECT 
            F.*,
            TM.UNIT AS ZPPEST, -- unidad de medida
            TM.ZPP_TBLFC -- tabla fecha contabilización
            FROM FILTRADO_FINAL F
            LEFT JOIN RAW.TEXT_MANUAL TM
                ON F.BUKRS = TM.COMP_CODE -- on sociedad
        )
        CON_CONVERSION AS (
            SELECT
            C.*,
            P.DENOMINTR,
            P.NUMERATOR
            FROM CON_UNIDAD C
            LEFT JOIN RAW.PMAT_UNIT P -- DATO MAESTRO 0MAT_UNIT Ó 0MAT_ATTR?
                ON C.MATERIAL = P.MATERIAL --
                AND C.ZPPEST = P.MAT_UNIT --
            AND P.OBJVERS = 'A' --
        )
        TRANSFORMADO AS (
            SELECT *,
            -- LOGICA DE ID_CANTIDAD 
            CASE 
                WHEN LGORT IN ('NC01', 'NC02', 'PA01') AND SHKZG = 'S' THEN MENGE * -1
                WHEN LGORT NOT IN ('NC01', 'NC02', 'PA01') AND SHKZG = 'H' THEN MENGE * -1
                ELSE MENGE
            END AS ID_CANTIDAD,
            --  LOGICA DE IND_PRODTERMINADA_UME (ZPPPTUE)
            CASE 
                WHEN LGORT IN ('NC01', 'NC02', 'PA01') THEN
                    CASE 
                        WHEN SHKZG = 'S' THEN (MENGE * DENOMINTR) / NULLIF(NUMERATOR, 0) * -1
                        ELSE (MENGE * DENOMINTR) / NULLIF(NUMERATOR, 0)
                    END
                ELSE
                    CASE 
                        WHEN SHKZG = 'H' THEN (MENGE * DENOMINTR) / NULLIF(NUMERATOR, 0) * -1
                        ELSE (MENGE * DENOMINTR) / NULLIF(NUMERATOR, 0)
                    END
            END AS IND_PRODTERMINADA_UME,
            -- ZPPIFUE SE CALCULA NORMALMENTE
            (ZZLABST * DENOMINTR) / NULLIF(NUMERATOR, 0) AS IND_INVFABRICACION_UME
            FROM CON_CONVERSION
        ),
        CON_FINAL AS (
            SELECT 
            T.* ,
            2PARBL.AUART, --COORD_TYPE,
            2PARBL.ZZKTSCH --ZPP_KTSCH CLAVEMODELO
            FROM TRANSFORMADO T
            LEFT JOIN RAW.2LIS_04_P_ARBL 2PARBL
                ON T.WERKS = 2PARBL.WERKS --PLANT
                AND T.AUFNR = 2PARBL.AUFNR--PRODORDER
                WHERE LEFT(T.WERKS, 1) = 'R'
        )
        SELECT
        YEAR(TO_DATE(CF.BUDAT, 'YYYYMMDD')) AS ANIO,
        MONTH(TO_DATE(CF.BUDAT, 'YYYYMMDD')) AS MES,
        TO_CHAR(TO_DATE(CF.BUDAT, 'YYYYMMDD'), 'YYYYMM') AS ANIOMES, 
        CF.BUKRS AS SOCIEDAD_ID,
        CF.WERKS AS CENTRO_ID,
        CF.LGORT AS ALMACEN_ID,
        CF.KOSTL AS CENTRO_COSTO_ID,
        CF.KOKRS AS SOCIEDADCO_ID,
        CF.MATNR AS MATERIAL_ID,
        CF.CHARG AS LOTE,
        CF.AUART AS CLASEORDFAB_ID, 
        CF.BWART AS CLASEMOVIMIENTO,
        CF.AUFNR AS ORDENFABRICACION_ID, 
        CF.ZZKTSCH AS CLAVEMODELO,
        CF AS PUESTOTRABAJO_ID,--OJO NO HAY LOGICA?
        CF.KUNNR AS SOLICITANTE_ID,
        CF.ZZLABST AS IND_INVFABRICACION_UMB,
        CF.IND_INVFABRICACION_UME AS IND_INVFABRICACION_UME,
        CF.IND_PRODTERMINADA_UME AS IND_PRODTERMINADA_UME, 
        CF.ID_CANTIDAD AS ID_CANTIDAD, 
        CF AS  IND_CONSUMO_UMB, 
        CF AS IND_INVPRODUCCION_UME,
        CF AS IND_EMBARCADO_UME,
        CF AS IND_AJUSTEINVENTARIO_UME,
        CF.SISORIGEN_ID,
        CF.MANDANTE,
        CURRENT_TIMESTAMP AS FECHA_CARGA,
        RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA
    FROM CON_FINAL CF

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
    VALUES ('SP_PRE_PFCT_OPE_PROD_ADH','PRE.PFCT_OPE_ADH_PROD', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 