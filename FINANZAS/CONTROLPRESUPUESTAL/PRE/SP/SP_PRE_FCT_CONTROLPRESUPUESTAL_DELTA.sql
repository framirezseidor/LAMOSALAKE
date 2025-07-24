CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FCT_CONTROLPRESUPUESTAL_DELTA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP Delta que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_CONTROLPRESUPUESTAL
---------------------------------------------------------------------------------
*/

DECLARE

    ------------ VARIABLES DE ENTORNO ------------------
	    F_INICIO        TIMESTAMP_NTZ(9);
        F_FIN           TIMESTAMP_NTZ(9);
        T_EJECUCION     NUMBER(38,0);
        ROWS_INSERTED   NUMBER(38,0);
        TEXTO           VARCHAR(200);

BEGIN
    //Generacion del identificador del proceso
    SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;


        ---------------------------------------------------------
        ------------------ LLENADO DE TABLAS --------------------
        ---------------------------------------------------------
	BEGIN
        // Inicia proceso INSERT 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        

//Merge entre DELTA y Tabla Espejo

//31:

MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_31_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                            BTART,
                            FISCYEAR,
                            ACTDETL,
                            FIKRS,
                            BUCAT,
                            RFORG,
                            STUNR,
                            RFKNT,
                            RFPOS,
                            REFBN,
                            RFETE,
                            RCOND,
                            UPDMOD
                            
            ORDER BY FECHA_CARGA DESC, -- Primero por FECHA_CARGA en orden descendente
               TS_SEQUENCE_NUMBER DESC       -- Luego por TS_SEQUENCE_NUMBER en orden descendente
            ) AS RN
        FROM RAW.SQ1_EXT_0PU_IS_PS_31_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
ON 
        T.FISCVAR = S.FISCVAR
    AND T.BTART = S.BTART
    AND T.FISCYEAR = S.FISCYEAR
    AND T.ACTDETL = S.ACTDETL
    AND T.FIKRS = S.FIKRS
    AND T.BUCAT = S.BUCAT
    AND T.RFORG = S.RFORG
    AND T.STUNR = S.STUNR
    AND T.RFKNT = S.RFKNT
    AND T.RFPOS = S.RFPOS
    AND T.REFBN = S.REFBN
    AND T.RFETE = S.RFETE
    AND T.RCOND = S.RCOND
    AND T.UPDMOD = S.UPDMOD

WHEN MATCHED THEN
    UPDATE SET
        T.REFBN = S.REFBN,
        T.RFORG = S.RFORG,
        T.RFPOS = S.RFPOS,
        T.RFKNT = S.RFKNT,
        T.RFETE = S.RFETE,
        T.RCOND = S.RCOND,
        T.RFSYS = S.RFSYS,
        T.BTART = S.BTART,
        T.BUCAT = S.BUCAT,
        T.FISCYEAR = S.FISCYEAR,
        T.STUNR = S.STUNR,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.GNJHR = S.GNJHR,
        T.CEFFYEAR_BCS = S.CEFFYEAR_BCS,
        T.ZHLDT = S.ZHLDT,
        T.FIKRS = S.FIKRS,
        T.FONDS = S.FONDS,
        T.FISTL = S.FISTL,
        T.FIPEX = S.FIPEX,
        T.FAREA = S.FAREA,
        T.MEASURE = S.MEASURE,
        T.GRANT_NBR = S.GRANT_NBR,
        T.USERDIM = S.USERDIM,
        T.BUKRS = S.BUKRS,
        T.KTOPL = S.KTOPL,
        T.HKONT = S.HKONT,
        T.KOKRS = S.KOKRS,
        T.KOSTL = S.KOSTL,
        T.AUFNR = S.AUFNR,
        T.POSID = S.POSID,
        T.PRCTR = S.PRCTR,
        T.FMTYPE = S.FMTYPE,
        T.ACTDETL = S.ACTDETL,
        T.VRGNG = S.VRGNG,
        T.STATS = S.STATS,
        T.ERLKZ = S.ERLKZ,
        T.LOEKZ = S.LOEKZ,
        T.CFLEV = S.CFLEV,
        T.CFCNT = S.CFCNT,
        T.LIFNR = S.LIFNR,
        T.SGTXT = S.SGTXT,
        T.BUDAT = S.BUDAT,
        T.VREFBN = S.VREFBN,
        T.VRFPOS = S.VRFPOS,
        T.VRFKNT = S.VRFKNT,
        T.USNAM = S.USNAM,
        T.BLDOCDATE = S.BLDOCDATE,
        T.WAERS = S.WAERS,
        T.FKBTR = S.FKBTR,
        T.TWAER = S.TWAER,
        T.TRBTR = S.TRBTR,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = S.FECHA_CARGA
WHEN NOT MATCHED THEN
    INSERT (
        REFBN, RFORG, RFPOS, RFKNT, RFETE, RCOND, RFSYS, BTART, BUCAT, FISCYEAR,
        STUNR, FISCVAR, FISCPER, GNJHR, CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL,
        FIPEX, FAREA, MEASURE, GRANT_NBR, USERDIM, BUKRS, KTOPL, HKONT, KOKRS,
        KOSTL, AUFNR, POSID, PRCTR, FMTYPE, ACTDETL, VRGNG, STATS, ERLKZ, LOEKZ,
        CFLEV, CFCNT, LIFNR, SGTXT, BUDAT, VREFBN, VRFPOS, VRFKNT, USNAM, BLDOCDATE,
        WAERS, FKBTR, TWAER, TRBTR, XARCH, UPDMOD, FMAA, TIPO, FECHA_CARGA
    )
    VALUES (
        S.REFBN, S.RFORG, S.RFPOS, S.RFKNT, S.RFETE, S.RCOND,
        S.RFSYS, S.BTART, S.BUCAT, S.FISCYEAR, S.STUNR, S.FISCVAR,
        S.FISCPER, S.GNJHR, S.CEFFYEAR_BCS, S.ZHLDT, S.FIKRS,
        S.FONDS, S.FISTL, S.FIPEX, S.FAREA, S.MEASURE, S.GRANT_NBR,
        S.USERDIM, S.BUKRS, S.KTOPL, S.HKONT, S.KOKRS, S.KOSTL,
        S.AUFNR, S.POSID, S.PRCTR, S.FMTYPE, S.ACTDETL, S.VRGNG,
        S.STATS, S.ERLKZ, S.LOEKZ, S.CFLEV, S.CFCNT, S.LIFNR,
        S.SGTXT, S.BUDAT, S.VREFBN, S.VRFPOS, S.VRFKNT, S.USNAM,
        S.BLDOCDATE, S.WAERS, S.FKBTR, S.TWAER, S.TRBTR, S.XARCH,
        S.UPDMOD, S.FMAA, S.TIPO, S.FECHA_CARGA
    );

//32:
MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_32_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                    BTART,
                    FISCYEAR,
                    FIKRS,
                    BUCAT,
                    FMBUZEI,
                    FMBELNR,
                    STUNR

            ORDER BY FECHA_CARGA DESC, -- Primero por FECHA_CARGA en orden descendente
               TS_SEQUENCE_NUMBER DESC       -- Luego por TS_SEQUENCE_NUMBER en orden descendente
            ) AS RN
        FROM (  ------Limpia los UPDMOD = D ----
        SELECT *
        FROM RAW.SQ1_EXT_0PU_IS_PS_32_DELTA
        WHERE (FISCVAR, BTART, FISCYEAR, FIKRS, BUCAT, FMBUZEI, FMBELNR, STUNR) NOT IN (
            SELECT FISCVAR, BTART, FISCYEAR, FIKRS, BUCAT, FMBUZEI, FMBELNR, STUNR
            FROM RAW.SQ1_EXT_0PU_IS_PS_32_DELTA
            WHERE UPDMOD = 'D'
            GROUP BY FISCVAR, BTART, FISCYEAR, FIKRS, BUCAT, FMBUZEI, FMBELNR, STUNR
)

        )
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
ON 
            T.FISCVAR = S.FISCVAR
        AND T.BTART = S.BTART
        AND T.FISCYEAR = S.FISCYEAR
        AND T.FIKRS = S.FIKRS
        AND T.BUCAT = S.BUCAT
        AND T.FMBUZEI = S.FMBUZEI
        AND T.FMBELNR = S.FMBELNR
        AND T.STUNR = S.STUNR

WHEN MATCHED THEN
    UPDATE SET
        T.FMBELNR = S.FMBELNR,
        T.FMBUZEI = S.FMBUZEI,
        T.BTART = S.BTART,
        T.BUCAT = S.BUCAT,
        T.FISCYEAR = S.FISCYEAR,
        T.STUNR = S.STUNR,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.GNJHR = S.GNJHR,
        T.CEFFYEAR_BCS = S.CEFFYEAR_BCS,
        T.ZHLDT = S.ZHLDT,
        T.FIKRS = S.FIKRS,
        T.FONDS = S.FONDS,
        T.FISTL = S.FISTL,
        T.FIPEX = S.FIPEX,
        T.FAREA = S.FAREA,
        T.MEASURE = S.MEASURE,
        T.GRANT_NBR = S.GRANT_NBR,
        T.USERDIM = S.USERDIM,
        T.BUKRS = S.BUKRS,
        T.KTOPL = S.KTOPL,
        T.HKONT = S.HKONT,
        T.KOKRS = S.KOKRS,
        T.KOSTL = S.KOSTL,
        T.AUFNR = S.AUFNR,
        T.POSID = S.POSID,
        T.PRCTR = S.PRCTR,
        T.FMTYPE = S.FMTYPE,
        T.ACTDETL = S.ACTDETL,
        T.VRGNG = S.VRGNG,
        T.STATS = S.STATS,
        T.CFLEV = S.CFLEV,
        T.CFCNT = S.CFCNT,
        T.LIFNR = S.LIFNR,
        T.KUNNR = S.KUNNR,
        T.SGTXT = S.SGTXT,
        T.XREVS = S.XREVS,
        T.BLART = S.BLART,
        T.BUDAT = S.BUDAT,
        T.VOBUKRS = S.VOBUKRS,
        T.VOGJAHR = S.VOGJAHR,
        T.VOBELNR = S.VOBELNR,
        T.VOBUZEI = S.VOBUZEI,
        T.KNGJAHR = S.KNGJAHR,
        T.KNBELNR = S.KNBELNR,
        T.KNBUZEI = S.KNBUZEI,
        T.VREFBN = S.VREFBN,
        T.VRFPOS = S.VRFPOS,
        T.VRFORG = S.VRFORG,
        T.VRFKNT = S.VRFKNT,
        T.WAERS = S.WAERS,
        T.FKBTR = S.FKBTR,
        T.TWAER = S.TWAER,
        T.TRBTR = S.TRBTR,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.AWTYP = S.AWTYP,
        T.AWREF = S.AWREF,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = S.FECHA_CARGA
WHEN NOT MATCHED THEN
    INSERT (
        FMBELNR, FMBUZEI, BTART, BUCAT, FISCYEAR, STUNR, FISCVAR, FISCPER,
        GNJHR, CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL, FIPEX, FAREA, MEASURE,
        GRANT_NBR, USERDIM, BUKRS, KTOPL, HKONT, KOKRS, KOSTL, AUFNR, POSID,
        PRCTR, FMTYPE, ACTDETL, VRGNG, STATS, CFLEV, CFCNT, LIFNR, KUNNR, SGTXT,
        XREVS, BLART, BUDAT, VOBUKRS, VOGJAHR, VOBELNR, VOBUZEI, KNGJAHR,
        KNBELNR, KNBUZEI, VREFBN, VRFPOS, VRFORG, VRFKNT, WAERS, FKBTR, TWAER,
        TRBTR, XARCH, UPDMOD, FMAA, AWTYP, AWREF, TIPO, FECHA_CARGA
    )
    VALUES (
        S.FMBELNR, S.FMBUZEI, S.BTART, S.BUCAT, S.FISCYEAR, S.STUNR, S.FISCVAR, S.FISCPER,
        S.GNJHR, S.CEFFYEAR_BCS, S.ZHLDT, S.FIKRS, S.FONDS, S.FISTL, S.FIPEX, S.FAREA,
        S.MEASURE, S.GRANT_NBR, S.USERDIM, S.BUKRS, S.KTOPL, S.HKONT, S.KOKRS, S.KOSTL,
        S.AUFNR, S.POSID, S.PRCTR, S.FMTYPE, S.ACTDETL, S.VRGNG, S.STATS, S.CFLEV,
        S.CFCNT, S.LIFNR, S.KUNNR, S.SGTXT, S.XREVS, S.BLART, S.BUDAT, S.VOBUKRS,
        S.VOGJAHR, S.VOBELNR, S.VOBUZEI, S.KNGJAHR, S.KNBELNR, S.KNBUZEI, S.VREFBN,
        S.VRFPOS, S.VRFORG, S.VRFKNT, S.WAERS, S.FKBTR, S.TWAER, S.TRBTR, S.XARCH,
        S.UPDMOD, S.FMAA, S.AWTYP, S.AWREF, S.TIPO, S.FECHA_CARGA
    );

//33:
MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_33_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                        ACTDETL,
                        FIKRS,
                        BUCAT,
                        REFRYEAR,
                        REFDOCNR,
                        REFDOCLN

            ORDER BY TS_SEQUENCE_NUMBER DESC, -- Primero por TS_SEQUENCE_NUMBER en orden descendente
                     FECHA_CARGA DESC -- Luego por FECHA_CARGA en orden descendente -- Ordenar por una columna relevante
            ) AS RN
        FROM RAW.SQ1_EXT_0PU_IS_PS_33_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
        ON  T.FISCVAR = S.FISCVAR
        AND T.ACTDETL = S.ACTDETL
        AND T.FIKRS = S.FIKRS
        AND T.BUCAT = S.BUCAT
        AND T.REFRYEAR = S.REFRYEAR
        AND T.REFDOCNR = S.REFDOCNR
        AND T.REFDOCLN = S.REFDOCLN

WHEN MATCHED THEN
    UPDATE SET
        T.RBTART = S.RBTART,
        T.BUCAT = S.BUCAT,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.RGNJHR = S.RGNJHR,
        T.CEFFYEAR_BCS = S.CEFFYEAR_BCS,
        T.ZHLDT = S.ZHLDT,
        T.FIKRS = S.FIKRS,
        T.RFONDS = S.RFONDS,
        T.RFISTL = S.RFISTL,
        T.RFIPEX = S.RFIPEX,
        T.FAREA = S.FAREA,
        T.MEASURE = S.MEASURE,
        T.GRANT_NBR = S.GRANT_NBR,
        T.RUSERDIM = S.RUSERDIM,
        T.RBUKRS = S.RBUKRS,
        T.KTOPL = S.KTOPL,
        T.RHKONT = S.RHKONT,
        T.KOKRS = S.KOKRS,
        T.KOSTL = S.KOSTL,
        T.AUFNR = S.AUFNR,
        T.POSID = S.POSID,
        T.PRCTR = S.PRCTR,
        T.FMTYPE = S.FMTYPE,
        T.ACTDETL = S.ACTDETL,
        T.RVRGNG = S.RVRGNG,
        T.RSTATS = S.RSTATS,
        T.RCFLEV = S.RCFLEV,
        T.SGTXT = S.SGTXT,
        T.WAERS = S.WAERS,
        T.FKBTR = S.FKBTR,
        T.TWAER = S.TWAER,
        T.TRBTR = S.TRBTR,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        REFDOCNR,
        REFDOCLN,
        REFRYEAR,
        RBTART,
        BUCAT,
        FISCVAR,
        FISCPER,
        RGNJHR,
        CEFFYEAR_BCS,
        ZHLDT,
        FIKRS,
        RFONDS,
        RFISTL,
        RFIPEX,
        FAREA,
        MEASURE,
        GRANT_NBR,
        RUSERDIM,
        RBUKRS,
        KTOPL,
        RHKONT,
        KOKRS,
        KOSTL,
        AUFNR,
        POSID,
        PRCTR,
        FMTYPE,
        ACTDETL,
        RVRGNG,
        RSTATS,
        RCFLEV,
        SGTXT,
        WAERS,
        FKBTR,
        TWAER,
        TRBTR,
        XARCH,
        UPDMOD,
        FMAA,
        TIPO,
        FECHA_CARGA
    )
    VALUES (
        S.REFDOCNR,
        S.REFDOCLN,
        S.REFRYEAR,
        S.RBTART,
        S.BUCAT,
        S.FISCVAR,
        S.FISCPER,
        S.RGNJHR,
        S.CEFFYEAR_BCS,
        S.ZHLDT,
        S.FIKRS,
        S.RFONDS,
        S.RFISTL,
        S.RFIPEX,
        S.FAREA,
        S.MEASURE,
        S.GRANT_NBR,
        S.RUSERDIM,
        S.RBUKRS,
        S.KTOPL,
        S.RHKONT,
        S.KOKRS,
        S.KOSTL,
        S.AUFNR,
        S.POSID,
        S.PRCTR,
        S.FMTYPE,
        S.ACTDETL,
        S.RVRGNG,
        S.RSTATS,
        S.RCFLEV,
        S.SGTXT,
        S.WAERS,
        S.FKBTR,
        S.TWAER,
        S.TRBTR,
        S.XARCH,
        S.UPDMOD,
        S.FMAA,
        S.TIPO,
        CURRENT_TIMESTAMP
    );

//41:
MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_41_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                            RCMMTITEM,
                            FISCPER,
                            RFIKRS,
                            BUCAT,
                            FMTYPE,
                            RFUNCAREA,
                            RFUND,
                            RFUNDSCTR,
                            RGRANT_NBR,
                            BUDTYPE_9,
                            RMEASURE,
                            PROCESS_9,
                            VALTYPE_9,
                            WFSTATE_9,
                            RVERS
            ORDER BY TS_SEQUENCE_NUMBER DESC, -- Primero por TS_SEQUENCE_NUMBER en orden descendente
                     FECHA_CARGA DESC -- Luego por FECHA_CARGA en orden descendente
            ) AS RN
        FROM RAW.SQ1_EXT_0PU_IS_PS_41_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
    ON T.FISCVAR = S.FISCVAR
    AND T.RCMMTITEM = S.RCMMTITEM
    AND T.FISCPER = S.FISCPER
    AND T.RFIKRS = S.RFIKRS
    AND T.BUCAT = S.BUCAT
    AND T.FMTYPE = S.FMTYPE
    AND T.RFUNCAREA = S.RFUNCAREA
    AND T.RFUND = S.RFUND
    AND T.RFUNDSCTR = S.RFUNDSCTR
    AND T.RGRANT_NBR = S.RGRANT_NBR
    AND T.BUDTYPE_9 = S.BUDTYPE_9
    AND T.RMEASURE = S.RMEASURE
    AND T.PROCESS_9 = S.PROCESS_9
    AND T.VALTYPE_9 = S.VALTYPE_9
    AND T.WFSTATE_9 = S.WFSTATE_9
    AND T.RVERS = S.RVERS
WHEN MATCHED THEN
    UPDATE SET
        T.RFIKRS = S.RFIKRS,
        T.RFUND = S.RFUND,
        T.RFUNDSCTR = S.RFUNDSCTR,
        T.RCMMTITEM = S.RCMMTITEM,
        T.RFUNCAREA = S.RFUNCAREA,
        T.RGRANT_NBR = S.RGRANT_NBR,
        T.RMEASURE = S.RMEASURE,
        T.RUSERDIM = S.RUSERDIM,
        T.RVERS = S.RVERS,
        T.BUCAT = S.BUCAT,
        T.VALTYPE_9 = S.VALTYPE_9,
        T.WFSTATE_9 = S.WFSTATE_9,
        T.FMTYPE = S.FMTYPE,
        T.PROCESS_9 = S.PROCESS_9,
        T.BUDTYPE_9 = S.BUDTYPE_9,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.CEFFYEAR_9 = S.CEFFYEAR_9,
        T.FMCUR = S.FMCUR,
        T.AMOUNT = S.AMOUNT,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        RFIKRS,
        RFUND,
        RFUNDSCTR,
        RCMMTITEM,
        RFUNCAREA,
        RGRANT_NBR,
        RMEASURE,
        RUSERDIM,
        RVERS,
        BUCAT,
        VALTYPE_9,
        WFSTATE_9,
        FMTYPE,
        PROCESS_9,
        BUDTYPE_9,
        FISCVAR,
        FISCPER,
        CEFFYEAR_9,
        FMCUR,
        AMOUNT,
        XARCH,
        UPDMOD,
        FMAA,
        TIPO,
        FECHA_CARGA
    )
    VALUES (
        S.RFIKRS,
        S.RFUND,
        S.RFUNDSCTR,
        S.RCMMTITEM,
        S.RFUNCAREA,
        S.RGRANT_NBR,
        S.RMEASURE,
        S.RUSERDIM,
        S.RVERS,
        S.BUCAT,
        S.VALTYPE_9,
        S.WFSTATE_9,
        S.FMTYPE,
        S.PROCESS_9,
        S.BUDTYPE_9,
        S.FISCVAR,
        S.FISCPER,
        S.CEFFYEAR_9,
        S.FMCUR,
        S.AMOUNT,
        S.XARCH,
        S.UPDMOD,
        S.FMAA,
        S.TIPO,
        CURRENT_TIMESTAMP
    );

//42:
MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_42_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                            FISCPER,
                            RFIKRS,
                            DOCLN,
                            DOCNR

            ORDER BY TS_SEQUENCE_NUMBER DESC, -- Primero por TS_SEQUENCE_NUMBER en orden descendente
                     FECHA_CARGA DESC -- Luego por FECHA_CARGA en orden descendente
            ) AS RN
        FROM RAW.SQ1_EXT_0PU_IS_PS_42_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
    ON T.FISCVAR = S.FISCVAR
    AND T.FISCPER = S.FISCPER
    AND T.RFIKRS = S.RFIKRS
    AND T.DOCLN = S.DOCLN
    AND T.DOCNR = S.DOCNR

WHEN MATCHED THEN
    UPDATE SET
        T.DOCCT = S.DOCCT,
        T.DOCNR = S.DOCNR,
        T.DOCLN = S.DOCLN,
        T.REFDOCCT = S.REFDOCCT,
        T.REFDOCNR = S.REFDOCNR,
        T.REFDOCLN = S.REFDOCLN,
        T.REFACTIV = S.REFACTIV,
        T.REFRYEAR = S.REFRYEAR,
        T.CPUDT = S.CPUDT,
        T.CPUTM = S.CPUTM,
        T.USNAM = S.USNAM,
        T.RFIKRS = S.RFIKRS,
        T.RFUND = S.RFUND,
        T.RFUNDSCTR = S.RFUNDSCTR,
        T.RCMMTITEM = S.RCMMTITEM,
        T.RFUNCAREA = S.RFUNCAREA,
        T.RGRANT_NBR = S.RGRANT_NBR,
        T.RMEASURE = S.RMEASURE,
        T.RUSERDIM = S.RUSERDIM,
        T.RVERS = S.RVERS,
        T.BUCAT = S.BUCAT,
        T.VALTYPE_9 = S.VALTYPE_9,
        T.WFSTATE_9 = S.WFSTATE_9,
        T.FMTYPE = S.FMTYPE,
        T.PROCESS_9 = S.PROCESS_9,
        T.BUDTYPE_9 = S.BUDTYPE_9,
        T.SGTXT = S.SGTXT,
        T.BUDAT = S.BUDAT,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.CEFFYEAR_9 = S.CEFFYEAR_9,
        T.FMCUR = S.FMCUR,
        T.AMOUNT = S.AMOUNT,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        DOCCT,
        DOCNR,
        DOCLN,
        REFDOCCT,
        REFDOCNR,
        REFDOCLN,
        REFACTIV,
        REFRYEAR,
        CPUDT,
        CPUTM,
        USNAM,
        RFIKRS,
        RFUND,
        RFUNDSCTR,
        RCMMTITEM,
        RFUNCAREA,
        RGRANT_NBR,
        RMEASURE,
        RUSERDIM,
        RVERS,
        BUCAT,
        VALTYPE_9,
        WFSTATE_9,
        FMTYPE,
        PROCESS_9,
        BUDTYPE_9,
        SGTXT,
        BUDAT,
        FISCVAR,
        FISCPER,
        CEFFYEAR_9,
        FMCUR,
        AMOUNT,
        XARCH,
        UPDMOD,
        FMAA,
        TIPO,
        FECHA_CARGA
    )
    VALUES (
        S.DOCCT,
        S.DOCNR,
        S.DOCLN,
        S.REFDOCCT,
        S.REFDOCNR,
        S.REFDOCLN,
        S.REFACTIV,
        S.REFRYEAR,
        S.CPUDT,
        S.CPUTM,
        S.USNAM,
        S.RFIKRS,
        S.RFUND,
        S.RFUNDSCTR,
        S.RCMMTITEM,
        S.RFUNCAREA,
        S.RGRANT_NBR,
        S.RMEASURE,
        S.RUSERDIM,
        S.RVERS,
        S.BUCAT,
        S.VALTYPE_9,
        S.WFSTATE_9,
        S.FMTYPE,
        S.PROCESS_9,
        S.BUDTYPE_9,
        S.SGTXT,
        S.BUDAT,
        S.FISCVAR,
        S.FISCPER,
        S.CEFFYEAR_9,
        S.FMCUR,
        S.AMOUNT,
        S.XARCH,
        S.UPDMOD,
        S.FMAA,
        S.TIPO,
        CURRENT_TIMESTAMP
    );

//43:
MERGE INTO RAW.SQ1_EXT_0PU_IS_PS_43_SHADOW AS T
USING (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY FISCVAR,
                            DOCYEAR,
                            FISCPER,
                            FM_AREA,
                            DOCLN,
                            DOCNR
            ORDER BY TS_SEQUENCE_NUMBER DESC, -- Primero por TS_SEQUENCE_NUMBER en orden descendente
                     FECHA_CARGA DESC -- Luego por FECHA_CARGA en orden descendente
            ) AS RN
        FROM RAW.SQ1_EXT_0PU_IS_PS_43_DELTA
    )
    WHERE RN = 1 -- Considera solo la primera fila de cada grupo de duplicados
) AS S
    ON T.FISCVAR = S.FISCVAR
    AND T.DOCYEAR = S.DOCYEAR
    AND T.FISCPER = S.FISCPER
    AND T.FM_AREA = S.FM_AREA
    AND T.DOCLN = S.DOCLN
    AND T.DOCNR = S.DOCNR

WHEN MATCHED THEN
    UPDATE SET
        T.DOCNR = S.DOCNR,
        T.DOCLN = S.DOCLN,
        T.DOCYEAR = S.DOCYEAR,
        T.FM_AREA = S.FM_AREA,
        T.FUND = S.FUND,
        T.FUNDSCTR = S.FUNDSCTR,
        T.CMMTITEM = S.CMMTITEM,
        T.FUNCAREA = S.FUNCAREA,
        T.GRANT_NBR = S.GRANT_NBR,
        T.MEASURE = S.MEASURE,
        T.USERDIM = S.USERDIM,
        T.VALTYPE = S.VALTYPE,
        T.BUCAT = S.BUCAT,
        T.FMTYPE = S.FMTYPE,
        T.PROCESS = S.PROCESS,
        T.BUDTYPE = S.BUDTYPE,
        T.FISCVAR = S.FISCVAR,
        T.FISCPER = S.FISCPER,
        T.CEFFYEAR = S.CEFFYEAR,
        T.LTEXT = S.LTEXT,
        T.DOCFAM = S.DOCFAM,
        T.PROCESS_UI = S.PROCESS_UI,
        T.VERSION = S.VERSION,
        T.CRTUSER = S.CRTUSER,
        T.CRTDATE = S.CRTDATE,
        T.DOCDATE = S.DOCDATE,
        T.POSTDATE = S.POSTDATE,
        T.RESPPERS = S.RESPPERS,
        T.HTEXT = S.HTEXT,
        T.LTXT_IND = S.LTXT_IND,
        T.DOCSTATE = S.DOCSTATE,
        T.REVSTATE = S.REVSTATE,
        T.REV_REFNR = S.REV_REFNR,
        T.DOCTYPE = S.DOCTYPE,
        T.COHORT = S.COHORT,
        T.PUBLAW = S.PUBLAW,
        T.LEGIS = S.LEGIS,
        T.FMCUR = S.FMCUR,
        T.AMOUNT = S.AMOUNT,
        T.XARCH = S.XARCH,
        T.UPDMOD = S.UPDMOD,
        T.FMAA = S.FMAA,
        T.TIPO = S.TIPO,
        T.FECHA_CARGA = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        DOCNR,
        DOCLN,
        DOCYEAR,
        FM_AREA,
        FUND,
        FUNDSCTR,
        CMMTITEM,
        FUNCAREA,
        GRANT_NBR,
        MEASURE,
        USERDIM,
        VALTYPE,
        BUCAT,
        FMTYPE,
        PROCESS,
        BUDTYPE,
        FISCVAR,
        FISCPER,
        CEFFYEAR,
        LTEXT,
        DOCFAM,
        PROCESS_UI,
        VERSION,
        CRTUSER,
        CRTDATE,
        DOCDATE,
        POSTDATE,
        RESPPERS,
        HTEXT,
        LTXT_IND,
        DOCSTATE,
        REVSTATE,
        REV_REFNR,
        DOCTYPE,
        COHORT,
        PUBLAW,
        LEGIS,
        FMCUR,
        AMOUNT,
        XARCH,
        UPDMOD,
        FMAA,
        TIPO,
        FECHA_CARGA
    )
    VALUES (
        S.DOCNR,
        S.DOCLN,
        S.DOCYEAR,
        S.FM_AREA,
        S.FUND,
        S.FUNDSCTR,
        S.CMMTITEM,
        S.FUNCAREA,
        S.GRANT_NBR,
        S.MEASURE,
        S.USERDIM,
        S.VALTYPE,
        S.BUCAT,
        S.FMTYPE,
        S.PROCESS,
        S.BUDTYPE,
        S.FISCVAR,
        S.FISCPER,
        S.CEFFYEAR,
        S.LTEXT,
        S.DOCFAM,
        S.PROCESS_UI,
        S.VERSION,
        S.CRTUSER,
        S.CRTDATE,
        S.DOCDATE,
        S.POSTDATE,
        S.RESPPERS,
        S.HTEXT,
        S.LTXT_IND,
        S.DOCSTATE,
        S.REVSTATE,
        S.REV_REFNR,
        S.DOCTYPE,
        S.COHORT,
        S.PUBLAW,
        S.LEGIS,
        S.FMCUR,
        S.AMOUNT,
        S.XARCH,
        S.UPDMOD,
        S.FMAA,
        S.TIPO,
        CURRENT_TIMESTAMP
    );



    //Fin Merge

 //Inicio Proceso carga ANIOMES
    TRUNCATE TABLE RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT
            CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_31_DELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT 
            CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_32_DELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT 
            CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_33_DELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT 
            CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_41_DELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT 
            CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_42_DELTA;

    INSERT INTO RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA
    SELECT DISTINCT 
            CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) AS ANIOMES
    FROM RAW.SQ1_EXT_0PU_IS_PS_43_DELTA;
    
 //Fin Proceso carga ANIOMES 



//Inicio Borrado ANIOMES tabla PRE de los meses DELTA
    DELETE FROM PRE.PFCT_FIN_CONTROLPRESUPUESTAL WHERE ANIOMES IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA);
//Fin Borrado ANIOMES tabla PRE de los meses DELTA



        // Process INSERT
		INSERT INTO PRE.PFCT_FIN_CONTROLPRESUPUESTAL
			
        SELECT  SUBSTR(ZHLDT,0,4) AS ANIO,
                IFF(SUBSTR(ZHLDT,6,2) = '00','01',
                        IFF(SUBSTR(ZHLDT,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(ZHLDT,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                KOKRS	AS SOCIEDADCO_ID,
                BUKRS	AS SOCIEDAD_ID,
                FIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                FONDS	AS FONDO_ID,
                CONCAT(FIKRS,'_',FISTL) AS CENTROGESTOR_ID,
                ACTDETL	AS DETALLECOMPREAL_ID,
                STATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                CONCAT(FIKRS,'_',FIPEX)	AS POSICIONPRES_ID,
                CONCAT(KOKRS,'_',FISTL)	AS CENTROCOSTO_ID,
                CONCAT(KOKRS,'_',FIPEX)	AS CUENTA_ID,
                LIFNR	AS ACREEDOR_ID,
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                BUDAT AS FECHA_CONTABILIZACION,
                FAREA AS AREAFUNCIONAL_ID,
                VRGNG AS OPDETALLE_ID,
                GRANT_NBR AS SUBVENCION_ID,
                '' AS CLASEPRES_ID, --BUDTYPE_9
                BTART AS CLASEIMPORTE_ID,
                KTOPL AS PLANCUENTAS_ID,
                HKONT AS CUENTAMAYOR_ID,
                '' AS TIPOVALORPRES_ID, --VALTYPE_9
                '' AS STATUSWORKFLOW_ID, --WFSTATE_9
                '' AS VERSION_ID, --RVERS
                SGTXT AS TEXTO_TEXT,
                TRBTR AS IND_IMPORTE_TRANS,
                TWAER AS MON_TRANS,
                FKBTR AS IND_IMPORTE_ENTCP,
                WAERS AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,

                EXT31.MANDANTE,
                EXT31.SISORIGEN_ID,
                EXT31.FECHA_CARGA,
                EXT31.ZONA_HORARIA,
                EXT31.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_31 AS EXT31
        WHERE FIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)

        UNION ALL

        SELECT  SUBSTR(ZHLDT,0,4) AS ANIO,
                IFF(SUBSTR(ZHLDT,6,2) = '00','01',
                        IFF(SUBSTR(ZHLDT,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(ZHLDT,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                KOKRS	AS SOCIEDADCO_ID,
                BUKRS	AS SOCIEDAD_ID,
                FIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                FONDS	AS FONDO_ID,
                CONCAT(FIKRS,'_',FISTL) AS CENTROGESTOR_ID,
                ACTDETL	AS DETALLECOMPREAL_ID,
                STATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                CONCAT(FIKRS,'_',FIPEX)	AS POSICIONPRES_ID,
                CONCAT(KOKRS,'_',FISTL)	AS CENTROCOSTO_ID,
                CONCAT(KOKRS,'_',FIPEX)	AS CUENTA_ID,
                LIFNR AS ACREEDOR_ID,
                KUNNR AS DEUDOR_ID,
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                BUDAT AS FECHA_CONTABILIZACION,
                FAREA AS AREAFUNCIONAL_ID,
                VRGNG AS OPDETALLE_ID,
                GRANT_NBR AS SUBVENCION_ID,
                '' AS CLASEPRES_ID, --BUDTYPE_9
                BTART AS CLASEIMPORTE_ID,
                KTOPL AS PLANCUENTAS_ID,
                HKONT AS CUENTAMAYOR_ID,
                '' AS TIPOVALORPRES_ID, --VALTYPE_9
                '' AS STATUSWORKFLOW_ID, --WFSTATE_9
                '' AS VERSION_ID, --RVERS
                SGTXT AS TEXTO_TEXT,
                TRBTR AS IND_IMPORTE_TRANS,
                TWAER AS MON_TRANS,
                FKBTR AS IND_IMPORTE_ENTCP,
                WAERS AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,

                EXT32.MANDANTE,
                EXT32.SISORIGEN_ID,
                EXT32.FECHA_CARGA,
                EXT32.ZONA_HORARIA,
                EXT32.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_32 AS EXT32
        WHERE FIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)

        UNION ALL

        SELECT  SUBSTR(ZHLDT,0,4) AS ANIO,
                IFF(SUBSTR(ZHLDT,6,2) = '00','01',
                        IFF(SUBSTR(ZHLDT,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(ZHLDT,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                KOKRS	AS SOCIEDADCO_ID,
                RBUKRS	AS SOCIEDAD_ID,
                FIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                RFONDS	AS FONDO_ID,
                CONCAT(FIKRS,'_',RFISTL) AS CENTROGESTOR_ID,
                ACTDETL	AS DETALLECOMPREAL_ID,
                RSTATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                CONCAT(FIKRS,'_',RFIPEX)	AS POSICIONPRES_ID,
                CONCAT(KOKRS,'_',RFISTL)	AS CENTROCOSTO_ID,
                CONCAT(KOKRS,'_',RFIPEX)	AS CUENTA_ID,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                '00000000' AS FECHA_CONTABILIZACION, --BUDAT
                FAREA AS AREAFUNCIONAL_ID,
                RVRGNG AS OPDETALLE_ID,
                GRANT_NBR AS SUBVENCION_ID,
                '' AS CLASEPRES_ID, --BUDTYPE_9
                RBTART AS CLASEIMPORTE_ID,
                KTOPL AS PLANCUENTAS_ID,
                RHKONT AS CUENTAMAYOR_ID,
                '' AS TIPOVALORPRES_ID, --VALTYPE_9
                '' AS STATUSWORKFLOW_ID, --WFSTATE_9
                '' AS VERSION_ID, --RVERS
                SGTXT AS TEXTO_TEXT,
                TRBTR AS IND_IMPORTE_TRANS,
                TWAER AS MON_TRANS,
                FKBTR AS IND_IMPORTE_ENTCP,
                WAERS AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,

                EXT33.MANDANTE,
                EXT33.SISORIGEN_ID,
                EXT33.FECHA_CARGA,
                EXT33.ZONA_HORARIA,
                EXT33.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_33 AS EXT33
        WHERE FIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)

        UNION ALL

        SELECT  SUBSTR(FISCPER,0,4) AS ANIO,
                IFF(SUBSTR(FISCPER,6,2) = '00','01',
                        IFF(SUBSTR(FISCPER,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(FISCPER,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                'CAGL'	AS SOCIEDADCO_ID,
                ''	AS SOCIEDAD_ID, --LEFT JOIN CON DIM
                RFIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                RFUND	AS FONDO_ID,
                CONCAT(RFIKRS,'_',RFUNDSCTR) AS CENTROGESTOR_ID,
                ''	AS DETALLECOMPREAL_ID, --ACTDETL
                ''	AS INDICADOREST_ID, --STATS
                CONCAT(RFIKRS,'_',RMEASURE) AS PROGRAMAPRES_ID,
                CONCAT(RFIKRS,'_',RCMMTITEM)	AS POSICIONPRES_ID,
                CONCAT(SOCIEDADCO_ID,'_',RFUNDSCTR)	AS CENTROCOSTO_ID,
                CONCAT(SOCIEDADCO_ID,'_',RCMMTITEM)	AS CUENTA_ID,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                '00000000' AS FECHA_CONTABILIZACION, --BUDAT
                RFUNCAREA AS AREAFUNCIONAL_ID,
                '' AS OPDETALLE_ID, --VRGNG
                RGRANT_NBR AS SUBVENCION_ID,
                BUDTYPE_9 AS CLASEPRES_ID,
                '' AS CLASEIMPORTE_ID, --BTART
                '' AS PLANCUENTAS_ID, --KTOPL
                '' AS CUENTAMAYOR_ID, --HKONT
                VALTYPE_9 AS TIPOVALORPRES_ID,
                WFSTATE_9 AS STATUSWORKFLOW_ID,
                RVERS AS VERSION_ID,
                '' AS TEXTO_TEXT,
                0 AS IND_IMPORTE_TRANS,
                '' AS MON_TRANS,
                AMOUNT AS IND_IMPORTE_ENTCP,
                FMCUR AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,

                EXT41.MANDANTE,
                EXT41.SISORIGEN_ID,
                EXT41.FECHA_CARGA,
                EXT41.ZONA_HORARIA,
                EXT41.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_41 AS EXT41
        LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
            ON CONCAT('CAGL','_',EXT41.RFUNDSCTR) = CECO.CENTROCOSTO_ID
        WHERE RFIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND (CECO.FECHA_DESDE IS NULL OR CECO.FECHA_HASTA IS NULL OR CURRENT_DATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)
        
        UNION ALL

        SELECT  SUBSTR(FISCPER,0,4) AS ANIO,
                IFF(SUBSTR(FISCPER,6,2) = '00','01',
                        IFF(SUBSTR(FISCPER,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(FISCPER,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                'CAGL' AS SOCIEDADCO_ID,
                CECO.SOCIEDAD_ID AS SOCIEDAD_ID,
                RFIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                RFUND	AS FONDO_ID,
                CONCAT(RFIKRS,'_',RFUNDSCTR) AS CENTROGESTOR_ID,
                ''	AS DETALLECOMPREAL_ID, --ACTDETL
                ''	AS INDICADOREST_ID, --STATS
                CONCAT(RFIKRS,'_',RMEASURE) AS PROGRAMAPRES_ID,
                CONCAT(RFIKRS,'_',RCMMTITEM)	AS POSICIONPRES_ID,
                CONCAT(SOCIEDADCO_ID,'_',RFUNDSCTR)	AS CENTROCOSTO_ID,
                CONCAT(SOCIEDADCO_ID,'_',RCMMTITEM)	AS CUENTA_ID,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                BUDAT AS FECHA_CONTABILIZACION, --BUDAT
                RFUNCAREA AS AREAFUNCIONAL_ID,
                '' AS OPDETALLE_ID, --VRGNG
                RGRANT_NBR AS SUBVENCION_ID,
                BUDTYPE_9 AS CLASEPRES_ID,
                '' AS CLASEIMPORTE_ID, --BTART
                '' AS PLANCUENTAS_ID, --KTOPL
                '' AS CUENTAMAYOR_ID, --HKONT
                VALTYPE_9 AS TIPOVALORPRES_ID,
                WFSTATE_9 AS STATUSWORKFLOW_ID,
                RVERS AS VERSION_ID,
                SGTXT AS TEXTO_TEXT,
                0 AS IND_IMPORTE_TRANS,
                '' AS MON_TRANS,
                AMOUNT AS IND_IMPORTE_ENTCP,
                FMCUR AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,
                
                EXT42.MANDANTE,
                EXT42.SISORIGEN_ID,
                EXT42.FECHA_CARGA,
                EXT42.ZONA_HORARIA,
                EXT42.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_42 AS EXT42
        LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
            ON CONCAT('CAGL','_',EXT42.RFUNDSCTR) = CECO.CENTROCOSTO_ID
        WHERE RFIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND (	(BUDAT = '1970-01-01'
                    AND (CECO.FECHA_DESDE IS NULL 
                            OR CECO.FECHA_HASTA IS NULL
                            OR CURRENT_DATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
                )
                OR (BUDAT != '1970-01-01'
                    AND (CECO.FECHA_DESDE IS NULL
                            OR CECO.FECHA_HASTA IS NULL
                            OR BUDAT BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
                )
        )
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)

        UNION ALL

        SELECT  SUBSTR(FISCPER,0,4) AS ANIO,
                IFF(SUBSTR(FISCPER,6,2) = '00','01',
                        IFF(SUBSTR(FISCPER,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(FISCPER,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                'CAGL' AS SOCIEDADCO_ID,
                CECO.SOCIEDAD_ID AS SOCIEDAD_ID,
                FM_AREA	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                FUND	AS FONDO_ID,
                CONCAT(FM_AREA,'_',FUNDSCTR) AS CENTROGESTOR_ID,
                '' AS DETALLECOMPREAL_ID, --ACTDETL
                '' AS INDICADOREST_ID, --STATS
                CONCAT(FM_AREA,'_',MEASURE) AS PROGRAMAPRES_ID,
                CONCAT(FM_AREA,'_',CMMTITEM)	AS POSICIONPRES_ID,
                CONCAT(SOCIEDADCO_ID,'_',FUNDSCTR)	AS CENTROCOSTO_ID,
                CONCAT(SOCIEDADCO_ID,'_',CMMTITEM)	AS CUENTA_ID,
                '' AS ACREEDOR_ID, --LIFNR
                '' AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                POSTDATE AS FECHA_CONTABILIZACION, --BUDAT
                FUNCAREA AS AREAFUNCIONAL_ID,
                '' AS OPDETALLE_ID, --VRGNG
                GRANT_NBR AS SUBVENCION_ID,
                BUDTYPE AS CLASEPRES_ID,
                '' AS CLASEIMPORTE_ID, --BTART
                '' AS PLANCUENTAS_ID, --KTOPL
                '' AS CUENTAMAYOR_ID, --HKONT
                VALTYPE AS TIPOVALORPRES_ID,
                '' AS STATUSWORKFLOW_ID,
                VERSION AS VERSION_ID,
                '' AS TEXTO_TEXT,
                0 AS IND_IMPORTE_TRANS,
                '' AS MON_TRANS,
                AMOUNT AS IND_IMPORTE_ENTCP,
                FMCUR AS MON_ENTCP,
                CONCAT(ENTIDADCP_ID, CENTROGESTOR_ID) AS ENTIDAD_CENGES_ID,
                CONCAT(ENTIDADCP_ID, PROGRAMAPRES_ID) AS ENTIDAD_PROPRES_ID,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    )AS IND_PRESUPUESTO_GASTOREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X'
                        AND DETALLECOMPREAL_ID NOT IN ('110','140','160'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_GASTOCOMP,

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO) * 100, -- Calculo de vista CON
                        IND_PRESUPUESTO_ASIGNABLE - IND_PRESUPUESTO_ASIGNADO -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_DISPONIBLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'B1' AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ACTUAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70',IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_COMPREAL,
                
                IFF(MON_ENTCP IN ('CLP','COP'), 
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '70' AND DETALLECOMPREAL_ID IN ('110', '140', '160'),
                        IND_IMPORTE_ENTCP, 0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_VALORREAL,

                EXT43.MANDANTE,
                EXT43.SISORIGEN_ID,
                EXT43.FECHA_CARGA,
                EXT43.ZONA_HORARIA,
                EXT43.TIPO
        FROM RAW.SQ1_EXT_0PU_IS_PS_43 AS EXT43
        LEFT JOIN PRE.PDIM_FIN_CENTROCOSTO AS CECO
            ON CONCAT('CAGL','_',EXT43.FUNDSCTR) = CECO.CENTROCOSTO_ID
        WHERE FM_AREA BETWEEN 'FMAR' AND 'FMPE'
        AND (	(POSTDATE = '1970-01-01'
                    AND (CECO.FECHA_DESDE IS NULL 
                            OR CECO.FECHA_HASTA IS NULL
                            OR CURRENT_DATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
                )
                OR (POSTDATE != '1970-01-01'
                    AND (CECO.FECHA_DESDE IS NULL
                            OR CECO.FECHA_HASTA IS NULL
                            OR POSTDATE BETWEEN CECO.FECHA_DESDE AND CECO.FECHA_HASTA)
                )
        )
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) IN (SELECT DISTINCT ANIOMES FROM RAW.SQ1_TBL_CONTROLPRESUPUESTAL_ANIOMESDELTA)
        
        ;

        
        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_GASTOSREAL;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en INSERT: ' || :sqlerrm) INTO :TEXTO;
        RETURN :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('PRE.SP_PRE_PFCT_FIN_CONTROLPRESUPUESTALDELTA','PRE.PFCT_FIN_CONTROLPRESUPUESTAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;