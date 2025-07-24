CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_FIN_CUENTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE


     ------------ VARIABLES DE ENTORNO ------------ 
	ID varchar(250)					:= NULL;
    D_PROCESO VARCHAR(250)	        :=''PDIM_FIN_CUENTA'';
	D_TABLAAFECTADA VARCHAR(250)    := NULL;
	D_RESULTADO VARCHAR(10)         := NULL;
	D_OPERACION VARCHAR(10)		    := NULL;
	N_REGISTROS NUMBER(38,0)        := NULL;
	NM_TIPO_LOG VARCHAR(100)        := NULL;
	F_INICIO TIMESTAMP_NTZ(9)       := NULL;
	F_FIN TIMESTAMP_NTZ(9)          := NULL;
	T_EJECUCION NUMBER(38,0)   		:= NULL;
	D_MENSAJE VARCHAR(250)          := NULL;

BEGIN

    //Generacion del identificador del proceso
    SELECT UUID_STRING() INTO :ID; 
    
    BEGIN

        ------------ TRUNCAR TABLAS -----------------------------
        ---------------------------------------------------------
        // Inicia proceso TRUNCATE 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Process TRUNCATE
        TRUNCATE TABLE PRE.PDIM_FIN_CUENTA;
		
		
        // Fin proceso TRUNCATE
        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
		
		//Calculo tiempo
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
        
        //Registro bitacora       
        --CALL PRE_PRD_SAP.LOGS.SP_LOG_PRD (:ID, :D_PROCESO, ''PDIM_FIN_CUENTA'', ''EXITOSO'',''TRUNCATE'',:SQLROWCOUNT, :NM_TIPO_LOG, :F_INICIO, :F_FIN,  :T_EJECUCION, :D_MENSAJE);


		------------ LLENADO DE TABLAS --------------------------
        ---------------------------------------------------------
		
        // Inicia proceso INSERT 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Process INSERT
        INSERT INTO PRE.PDIM_FIN_CUENTA
        SELECT
            CONCAT(A.KOKRS,''_'',A.KSTAR) AS CUENTA_ID,
            A.KOKRS AS SOCIEDADCO_ID,
            A.KSTAR AS CUENTA,
            A.TXTSH AS CUENTA_TEXT,
            LPAD(B.TEXTO, 3, ''0'') AS SUBDIVISION_ID,
            C.SUBDIVISION_TEXT AS SUBDIVISION_TEXT,
            A.SISORIGEN_ID,
            A.MANDANTE,
            A.FECHA_CARGA,
            A.ZONA_HORARIA
        FROM  RAW.SQ1_EXT_0COSTELMNT_TEXT AS A
        LEFT JOIN RAW.SQ1_EXT_ZSTXH AS B
        ON A.KSTAR = LEFT(TDNAME,10)
        AND A.KOKRS=''CAGL''
        LEFT JOIN RAW.FILE_CSV_SUBDIVISION AS C
        ON LPAD(B.TEXTO, 3, ''0'') = LPAD(C.SUBDIVISION_ID, 3, ''0'')
        WHERE A.LANGU=''S''
        GROUP BY ALL;

        INSERT INTO PRE.PDIM_FIN_CUENTA (CUENTA_ID, CUENTA_TEXT)
        VALUES (''DUMMY'', ''DUMMY'');

        // Fin proceso INSERT
        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        
        //Registro bitacora       
        --CALL PRE_PRD_SAP.LOGS.SP_LOG_PRD (:ID, :D_PROCESO, ''PDIM_FIN_CUENTA'', ''EXITOSO'',''INSERT'',:SQLROWCOUNT, :NM_TIPO_LOG, :F_INICIO, :F_FIN,  :T_EJECUCION, :D_MENSAJE);
		

		-------------------------------------------------
        -------------------------------------------------

        RETURN (''Complete'');
        
    ---- MANEJO DE EXCEPCIONES POR ERRORES
    EXCEPTION
    
        when statement_error then 
        
        SELECT CONCAT(''Failed: Code: '',:sqlcode,
                    ''\\\\n  Message: '',:sqlerrm,
                    ''\\\\n  State: '',:sqlstate) INTO :D_MENSAJE;
        
        //Registro bitacora       
        --CALL PRE_PRD_SAP.LOGS.SP_LOG_PRD (:ID, :D_PROCESO, ''PRE_DIM_CUENTA'', ''FALLIDO'',''INSERT'',:SQLROWCOUNT, :NM_TIPO_LOG, :F_INICIO, :F_FIN,  :T_EJECUCION, :D_MENSAJE);

        RETURN (:D_MENSAJE);

END;
END';

CALL PRE.SP_PRE_PDIM_FIN_CUENTA();