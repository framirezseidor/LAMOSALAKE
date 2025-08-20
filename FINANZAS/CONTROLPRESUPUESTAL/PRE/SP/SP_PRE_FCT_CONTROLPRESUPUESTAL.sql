CREATE OR REPLACE PROCEDURE PRE.SP_PRE_FCT_CONTROLPRESUPUESTAL("FECHA_INICIO" VARCHAR(100), "FECHA_FIN" VARCHAR(100))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$

/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-21
 Creador:            Juan Santiago Pedreros
 Descripción:        SP que transforma datos desde la capa RAW a PRE para la tabla PFCT_FIN_CONTROLPRESUPUESTAL
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
    
    BEGIN


        ---------------------------------------------------------
        ------------------- DELETE TABLAS -----------------------
        ---------------------------------------------------------
        // Inicia proceso DELETE 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Process DELETE
        DELETE FROM PRE.PFCT_FIN_CONTROLPRESUPUESTAL
        WHERE ANIOMES BETWEEN :FECHA_INICIO AND :FECHA_FIN; 
		
		
        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;

    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
        RETURN :TEXTO;
    END;
		

        ---------------------------------------------------------
        ------------------ LLENADO DE TABLAS --------------------
        ---------------------------------------------------------
	BEGIN
        // Inicia proceso INSERT 
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;
        
        // Process INSERT
		INSERT INTO PRE.PFCT_FIN_CONTROLPRESUPUESTAL
			
        SELECT  SUBSTR(ZHLDT,0,4) AS ANIO,
                IFF(SUBSTR(ZHLDT,6,2) = '00','01',
                        IFF(SUBSTR(ZHLDT,6,2) IN ('13','14','15','16'),'12',
                    SUBSTR(ZHLDT,6,2))) AS MES,
                CONCAT(ANIO,MES) AS ANIOMES,
                KOKRS	AS SOCIEDADCO_ID,
                IFF(CONCAT(FIKRS,'_',FISTL) = CONCAT(FIKRS,'_','DUMMY'),'NA',EXT31.BUKRS) AS SOCIEDAD_ID,
                FIKRS	AS ENTIDADCP_ID,
                BUCAT	AS CATEGORIAPRES_ID,
                FMTYPE	AS TIPOVALOR_ID,
                FONDS	AS FONDO_ID,
                CONCAT(FIKRS,'_',FISTL) AS CENTROGESTOR_ID,
                FISTL AS CENTROGESTOR,
                ACTDETL	AS DETALLECOMPREAL_ID,
                STATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                MEASURE AS PROGRAMAPRES,
                CONCAT(FIKRS,'_',FIPEX)	AS POSICIONPRES_ID,
                FIPEX	AS POSICIONPRES,
                CONCAT(KOKRS,'_',LTRIM(FISTL, '0'))	AS CENTROCOSTO_ID,
                LTRIM(FISTL, '0')	AS CENTROCOSTO,
                CONCAT(KOKRS,'_',LTRIM(FIPEX, '0'))	AS CUENTA_ID,
                LTRIM(FIPEX, '0')	AS CUENTA,
                LIFNR	AS ACREEDOR_ID,
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(CUENTA.SUBDIVISION_ID = 'Z08', '8', IFF(CUENTA.SUBDIVISION_ID = 'Z25', '9', 'ZTPOGASTO')) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- BUDAT AS FECHA_CONTABILIZACION,
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

                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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

        LEFT JOIN PRE.PDIM_FIN_CUENTA AS CUENTA
		ON CONCAT(KOKRS,'_',LTRIM(EXT31.FIPEX, '0')) = CUENTA.CUENTA_ID

        WHERE FIKRS BETWEEN 'FMAR' AND 'FMPE'
        AND CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN --AND EXT31.TIPO='FULL'

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
                FISTL AS CENTROGESTOR,
                ACTDETL	AS DETALLECOMPREAL_ID,
                STATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                MEASURE AS PROGRAMAPRES,
                CONCAT(FIKRS,'_',FIPEX)	AS POSICIONPRES_ID,
                FIPEX	AS POSICIONPRES,
                CONCAT(KOKRS,'_',LTRIM(FISTL, '0'))	AS CENTROCOSTO_ID,
                LTRIM(FISTL, '0')	AS CENTROCOSTO,
                CONCAT(KOKRS,'_',LTRIM(FIPEX, '0'))	AS CUENTA_ID,
                LTRIM(FIPEX, '0')	AS CUENTA,
                LIFNR AS ACREEDOR_ID,
                KUNNR AS DEUDOR_ID,
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- TO_DATE(CONCAT(SUBSTR(ZHLDT, 0, 4), '-', SUBSTR(ZHLDT, 6, 2), '-01')) AS FECHA_CONTABILIZACION,
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
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN --AND EXT32.TIPO='FULL'

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
                RFISTL AS CENTROGESTOR,
                ACTDETL	AS DETALLECOMPREAL_ID,
                RSTATS	AS INDICADOREST_ID,
                CONCAT(FIKRS,'_',MEASURE) AS PROGRAMAPRES_ID,
                MEASURE AS PROGRAMAPRES,
                CONCAT(FIKRS,'_',RFIPEX) AS POSICIONPRES_ID,
                RFIPEX AS POSICIONPRES,
                CONCAT(KOKRS,'_',LTRIM(RFISTL, '0')) AS CENTROCOSTO_ID,
                LTRIM(RFISTL, '0')	AS CENTROCOSTO,
                CONCAT(KOKRS,'_',LTRIM(RFIPEX, '0')) AS CUENTA_ID,
                LTRIM(RFIPEX, '0')	AS CUENTA,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- CONCAT(SUBSTR(FISCPER,0,4),IFF(SUBSTR(FISCPER,6,2) = '00','01',
                --         IFF(SUBSTR(FISCPER,6,2) IN ('13','14','15','16'),'12',
                --     SUBSTR(FISCPER,6,2))),'01') AS FECHA_CONTABILIZACION, --BUDAT
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
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN

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
                RFUNDSCTR AS CENTROGESTOR,
                ''	AS DETALLECOMPREAL_ID, --ACTDETL
                ''	AS INDICADOREST_ID, --STATS
                CONCAT(RFIKRS,'_',RMEASURE) AS PROGRAMAPRES_ID,
                RMEASURE AS PROGRAMAPRES,
                CONCAT(RFIKRS,'_',RCMMTITEM) AS POSICIONPRES_ID,
                RCMMTITEM AS POSICIONPRES,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(RFUNDSCTR, '0')) AS CENTROCOSTO_ID,
                LTRIM(RFUNDSCTR, '0')	AS CENTROCOSTO,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(RCMMTITEM, '0')) AS CUENTA_ID,
                LTRIM(RCMMTITEM, '0')	AS CUENTA,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- CONCAT(SUBSTR(FISCPER,0,4),IFF(SUBSTR(FISCPER,6,2) = '00','01',
                --         IFF(SUBSTR(FISCPER,6,2) IN ('13','14','15','16'),'12',
                --     SUBSTR(FISCPER,6,2))),'01') AS FECHA_CONTABILIZACION, --BUDAT
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
                
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN

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
                RFUNDSCTR AS CENTROGESTOR,
                ''	AS DETALLECOMPREAL_ID, --ACTDETL
                ''	AS INDICADOREST_ID, --STATS
                CONCAT(RFIKRS,'_',RMEASURE) AS PROGRAMAPRES_ID,
                RMEASURE AS PROGRAMAPRES,
                CONCAT(RFIKRS,'_',RCMMTITEM) AS POSICIONPRES_ID,
                RCMMTITEM AS POSICIONPRES,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(RFUNDSCTR, '0')) AS CENTROCOSTO_ID,
                LTRIM(RFUNDSCTR, '0')	AS CENTROCOSTO,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(RCMMTITEM, '0')) AS CUENTA_ID,
                LTRIM(RCMMTITEM, '0')	AS CUENTA,
                ''	AS ACREEDOR_ID, --LIFNR
                ''	AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- BUDAT AS FECHA_CONTABILIZACION, --BUDAT
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
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN

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
                FUNDSCTR AS CENTROGESTOR,
                '' AS DETALLECOMPREAL_ID, --ACTDETL
                '' AS INDICADOREST_ID, --STATS
                CONCAT(FM_AREA,'_',MEASURE) AS PROGRAMAPRES_ID,
                MEASURE AS PROGRAMAPRES,
                CONCAT(FM_AREA,'_',CMMTITEM) AS POSICIONPRES_ID,
                CMMTITEM AS POSICIONPRES,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(FUNDSCTR, '0')) AS CENTROCOSTO_ID,
                LTRIM(FUNDSCTR, '0')	AS CENTROCOSTO,
                CONCAT(SOCIEDADCO_ID,'_',LTRIM(CMMTITEM, '0')) AS CUENTA_ID,
                LTRIM(CMMTITEM, '0')	AS CUENTA,
                '' AS ACREEDOR_ID, --LIFNR
                '' AS DEUDOR_ID, --KUNNR
                --IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '8', '08', IFF(LTRIM(COSTE.SUBDIVISION_ID,'0') = '25', '09', CECO.TIPOGASTO_ID)) AS GASTOER_ID,
                '' AS GASTOER_ID,
                TO_DATE(CONCAT(ANIO, '-', MES, '-01')) AS FECHA_CONTABILIZACION,
                -- POSTDATE AS FECHA_CONTABILIZACION, --BUDAT
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
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) * 100 , -- Calculo de vista CON
                        (IFF(TIPOVALOR_ID = '80' AND TIPOVALORPRES_ID = 'R1'
                        AND STATUSWORKFLOW_ID IN ('P','R') AND VERSION_ID = '000',
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNABLE,
                
                IFF(MON_ENTCP IN ('CLP','COP'),
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                        (IFF(TIPOVALORPRES_ID IN ('B1'),
                        IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                    ) AS IND_PRESUPUESTO_ASIGNADO,

                -- IFF(MON_ENTCP IN ('CLP','COP'),
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) * 100, -- Calculo de vista CON
                --         (IFF(TIPOVALOR_ID = '70' AND INDICADOREST_ID <> 'X',
                --         IND_IMPORTE_ENTCP,0)) -- Calculo de tabla CON
                --     ) AS IND_PRESUPUESTO_ASIGNADO,

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
        AND CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN --AND EXT43.TIPO='FULL'
        ;




    //Borrado e insersion tablas shadow:

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 31 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_31_SHADOW
WHERE CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 31 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_31_SHADOW
(       REFBN, RFORG, RFPOS, RFKNT, RFETE, RCOND, RFSYS, BTART, BUCAT, FISCYEAR, STUNR,
        FISCVAR, FISCPER, GNJHR, CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL, FIPEX, FAREA,
        MEASURE, GRANT_NBR, USERDIM, BUKRS, KTOPL, HKONT, KOKRS, KOSTL, AUFNR, POSID,
        PRCTR, FMTYPE, ACTDETL, VRGNG, STATS, ERLKZ, LOEKZ, CFLEV, CFCNT, LIFNR, SGTXT,
        BUDAT, VREFBN, VRFPOS, VRFKNT, USNAM, BLDOCDATE, WAERS, FKBTR, TWAER, TRBTR,
        XARCH, UPDMOD, FMAA, MANDANTE, SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  REFBN, RFORG, RFPOS, RFKNT, RFETE, RCOND, RFSYS, BTART, BUCAT, FISCYEAR, STUNR,
        FISCVAR, FISCPER, GNJHR, CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL, FIPEX, FAREA,
        MEASURE, GRANT_NBR, USERDIM, BUKRS, KTOPL, HKONT, KOKRS, KOSTL, AUFNR, POSID,
        PRCTR, FMTYPE, ACTDETL, VRGNG, STATS, ERLKZ, LOEKZ, CFLEV, CFCNT, LIFNR, SGTXT,
        BUDAT, VREFBN, VRFPOS, VRFKNT, USNAM, BLDOCDATE, WAERS, FKBTR, TWAER, TRBTR,
        XARCH, UPDMOD, FMAA, MANDANTE, SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_31
WHERE CONCAT(SUBSTR(ZHLDT,0,4),SUBSTR(ZHLDT,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

------------------------------

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 32 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_32_SHADOW
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 32 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_32_SHADOW
(       FMBELNR, FMBUZEI, BTART, BUCAT, FISCYEAR, STUNR, FISCVAR, FISCPER, GNJHR,
        CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL, FIPEX, FAREA, MEASURE, GRANT_NBR,
        USERDIM, BUKRS, KTOPL, HKONT, KOKRS, KOSTL, AUFNR, POSID, PRCTR, FMTYPE, ACTDETL,
        VRGNG, STATS, CFLEV, CFCNT, LIFNR, KUNNR, SGTXT, XREVS, BLART, BUDAT, VOBUKRS,
        VOGJAHR, VOBELNR, VOBUZEI, KNGJAHR, KNBELNR, KNBUZEI, VREFBN, VRFPOS, VRFORG,
        VRFKNT, WAERS, FKBTR, TWAER, TRBTR, XARCH, UPDMOD, FMAA, AWTYP, AWREF, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  FMBELNR, FMBUZEI, BTART, BUCAT, FISCYEAR, STUNR, FISCVAR, FISCPER, GNJHR,
        CEFFYEAR_BCS, ZHLDT, FIKRS, FONDS, FISTL, FIPEX, FAREA, MEASURE, GRANT_NBR,
        USERDIM, BUKRS, KTOPL, HKONT, KOKRS, KOSTL, AUFNR, POSID, PRCTR, FMTYPE, ACTDETL,
        VRGNG, STATS, CFLEV, CFCNT, LIFNR, KUNNR, SGTXT, XREVS, BLART, BUDAT, VOBUKRS,
        VOGJAHR, VOBELNR, VOBUZEI, KNGJAHR, KNBELNR, KNBUZEI, VREFBN, VRFPOS, VRFORG,
        VRFKNT, WAERS, FKBTR, TWAER, TRBTR, XARCH, UPDMOD, FMAA, AWTYP, AWREF, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_32
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

------------------------------

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 33 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_33_SHADOW WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 33 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_33_SHADOW
(       REFDOCNR, REFDOCLN, REFRYEAR, RBTART, BUCAT,FISCVAR, FISCPER, RGNJHR, CEFFYEAR_BCS,
        ZHLDT, FIKRS, RFONDS, RFISTL, RFIPEX, FAREA, MEASURE, GRANT_NBR, RUSERDIM,
        RBUKRS, KTOPL, RHKONT, KOKRS, KOSTL, AUFNR, POSID, PRCTR, FMTYPE, ACTDETL, RVRGNG,
        RSTATS, RCFLEV, SGTXT, WAERS, FKBTR, TWAER, TRBTR, XARCH, UPDMOD, FMAA, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  REFDOCNR, REFDOCLN, REFRYEAR, RBTART, BUCAT,FISCVAR, FISCPER, RGNJHR, CEFFYEAR_BCS,
        ZHLDT, FIKRS, RFONDS, RFISTL, RFIPEX, FAREA, MEASURE, GRANT_NBR, RUSERDIM,
        RBUKRS, KTOPL, RHKONT, KOKRS, KOSTL, AUFNR, POSID, PRCTR, FMTYPE, ACTDETL, RVRGNG,
        RSTATS, RCFLEV, SGTXT, WAERS, FKBTR, TWAER, TRBTR, XARCH, UPDMOD, FMAA, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_33
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

------------------------------

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 41 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_41_SHADOW WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 41 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_41_SHADOW
(       RFIKRS, RFUND, RFUNDSCTR, RCMMTITEM, RFUNCAREA, RGRANT_NBR, RMEASURE, RUSERDIM,
        RVERS, BUCAT, VALTYPE_9, WFSTATE_9, FMTYPE, PROCESS_9, BUDTYPE_9, FISCVAR,
        FISCPER, CEFFYEAR_9, FMCUR, AMOUNT, XARCH, UPDMOD, FMAA, MANDANTE, SISORIGEN_ID,
        FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  RFIKRS, RFUND, RFUNDSCTR, RCMMTITEM, RFUNCAREA, RGRANT_NBR, RMEASURE, RUSERDIM,
        RVERS, BUCAT, VALTYPE_9, WFSTATE_9, FMTYPE, PROCESS_9, BUDTYPE_9, FISCVAR,
        FISCPER, CEFFYEAR_9, FMCUR, AMOUNT, XARCH, UPDMOD, FMAA, MANDANTE, SISORIGEN_ID,
        FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_41
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

------------------------------

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 42 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_42_SHADOW WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 42 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_42_SHADOW
(       DOCCT, DOCNR, DOCLN, REFDOCCT, REFDOCNR, REFDOCLN, REFACTIV, REFRYEAR, CPUDT,
        CPUTM, USNAM, RFIKRS, RFUND, RFUNDSCTR, RCMMTITEM, RFUNCAREA, RGRANT_NBR, RMEASURE,
        RUSERDIM, RVERS, BUCAT, VALTYPE_9, WFSTATE_9, FMTYPE, PROCESS_9, BUDTYPE_9,
        SGTXT, BUDAT, FISCVAR, FISCPER, CEFFYEAR_9, FMCUR, AMOUNT, XARCH, UPDMOD,
        FMAA, MANDANTE, SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  DOCCT, DOCNR, DOCLN, REFDOCCT, REFDOCNR, REFDOCLN, REFACTIV, REFRYEAR, CPUDT,
        CPUTM, USNAM, RFIKRS, RFUND, RFUNDSCTR, RCMMTITEM, RFUNCAREA, RGRANT_NBR, RMEASURE,
        RUSERDIM, RVERS, BUCAT, VALTYPE_9, WFSTATE_9, FMTYPE, PROCESS_9, BUDTYPE_9,
        SGTXT, BUDAT, FISCVAR, FISCPER, CEFFYEAR_9, FMCUR, AMOUNT, XARCH, UPDMOD,
        FMAA, MANDANTE, SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_42
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

------------------------------

-- Paso 1: ELIMINAR REGISTROS DE SHADOW 43 EN EL RANGO ESPECIFICADO
DELETE FROM RAW.SQ1_EXT_0PU_IS_PS_43_SHADOW
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;

-- Paso 2: INSERTAR REGISTROS DE SHADOW 43 EN EL RANGO ESPECIFICADO
INSERT INTO RAW.SQ1_EXT_0PU_IS_PS_43_SHADOW
(       DOCNR, DOCLN, DOCYEAR, FM_AREA, FUND, FUNDSCTR, CMMTITEM, FUNCAREA, GRANT_NBR,
        MEASURE, USERDIM, VALTYPE, BUCAT, FMTYPE, PROCESS, BUDTYPE, FISCVAR, FISCPER,
        CEFFYEAR, LTEXT, DOCFAM, PROCESS_UI, VERSION, CRTUSER, CRTDATE, DOCDATE,
        POSTDATE, RESPPERS, HTEXT, LTXT_IND, DOCSTATE, REVSTATE, REV_REFNR, DOCTYPE,
        COHORT, PUBLAW, LEGIS, FMCUR, AMOUNT, XARCH, UPDMOD, FMAA, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
)
SELECT  DOCNR, DOCLN, DOCYEAR, FM_AREA, FUND, FUNDSCTR, CMMTITEM, FUNCAREA, GRANT_NBR,
        MEASURE, USERDIM, VALTYPE, BUCAT, FMTYPE, PROCESS, BUDTYPE, FISCVAR, FISCPER,
        CEFFYEAR, LTEXT, DOCFAM, PROCESS_UI, VERSION, CRTUSER, CRTDATE, DOCDATE,
        POSTDATE, RESPPERS, HTEXT, LTXT_IND, DOCSTATE, REVSTATE, REV_REFNR, DOCTYPE,
        COHORT, PUBLAW, LEGIS, FMCUR, AMOUNT, XARCH, UPDMOD, FMAA, MANDANTE,
        SISORIGEN_ID, FECHA_CARGA, ZONA_HORARIA, TIPO
FROM RAW.SQ1_EXT_0PU_IS_PS_43
WHERE CONCAT(SUBSTR(FISCPER,0,4),SUBSTR(FISCPER,6,2)) BETWEEN :FECHA_INICIO AND :FECHA_FIN;


        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_CONTROLPRESUPUESTAL;

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
    VALUES ('PRE.SP_PRE_FCT_CONTROLPRESUPUESTAL','PRE.PFCT_FIN_CONTROLPRESUPUESTAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;