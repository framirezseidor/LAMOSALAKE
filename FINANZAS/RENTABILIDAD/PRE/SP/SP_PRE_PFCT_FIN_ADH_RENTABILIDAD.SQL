CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_FIN_ADH_RENTABILIDAD()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-06-09
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa RAW a PRE para SP_PRE_PFCT_FIN_ADH_RENTABILIDAD
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

        DELETE FROM PRE.PFCT_FIN_ADH_RENTABILIDAD;

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

        INSERT INTO PRE.PFCT_FIN_ADH_RENTABILIDAD
        (
        FECHA_CONTABILIZACION,
        ANIO_MES,
        EJERCICIO,
        PERIODO,
        FECHA_DOCUMENTO,
        LEDGER,
        DOCUMENTO,
        DOCUMENTO_POS,
        ANULADA,
        SOCIEDADCO_ID,
        SOCIEDAD_ID,
        ORGVENTAS_ID,
        CANALDISTRIB_ID,
        SECTOR_ID,
        CENTRO_ID,
        OFICINAVENTAS_ID,
        CENTROBENEFICIO_ID,
        CENTROCOSTO_ID,
        CUENTA_ID,
        CLIENTE_ID,
        SOLICITANTE_ID,
        DESTINATARIO_ID,
        GRUPOSOLICITANTE_ID,
        CONSTRUCTORA_ID,
        TIPO_OBRA_ID,
        ID_PLAN_DE_OBRA,
        PAIS_ID,
        PAISCLIENTE_ID,
        REGION_ID,
        ZONATRANSPORTE_ID,
        MATERIAL_ID,
        CLAVE_MONEDA,
        UNIDAD_MEDIDA,
        CENTROCOSTO_GRUPO_ID,
        CUENTA_GRUPO_ID,
        CONVENIO_OBRA,
        IND_HSL,
        IND_QUANT2,
        IND_FAB_COSTOVENTAFIJO,
        IND_VTA_IMP_PRECIOLISTA_ML,
        IND_VTA_IMP_DESCUENTOS_ML,
        IND_VTA_IMP_PROMOCIONES_ML,
        IND_VTA_IMP_RAPPELES_ML,
        IND_VTA_IMP_CORRECTIVAS_ML,
        IND_VTA_IMP_VENTAREAL_ML,
        IND_VTA_IMP_VENTAREAL_TON,
        IND_VTA_PRECIO_CONSTRUCTORA_ML,
        IND_VTA_IMP_CONSTRUCTORA_ML,
        IND_FAB_MOD,
        IND_FAB_MOI,
        IND_FAB_ELECTRICIDAD,
        IND_FAB_DEPRECIACION,
        IND_FAB_MANTENIMIENTO,
        IND_FAB_GASTOSINDIRECTOS,
        IND_FAB_EMPAQUE,
        IND_FAB_CEMENTOS,
        IND_FAB_QUIMICOSMP,
        IND_FAB_CARGASQUIMICAS,
        IND_FAB_PTCOMPRADO,
        IND_FAB_CARBONATOCOMPRADO,
        IND_FAB_CARBONATOPIEDRA,
        IND_FAB_SUBCONTRATACIONMAQUILA,
        IND_FAB_OTROCOSTOSVENTA,
        IND_FAB_VARIOSADH,
        IND_FAB_MOD_FIL,
        IND_FAB_MOI_FIL,
        IND_FAB_ELECTRICIDAD_FIL,
        IND_FAB_DEPRECIACION_FIL,
        IND_FAB_MANTENIMIENTO_FIL,
        IND_FAB_GASTOSINDIRECTOS_FIL,
        IND_FAB_EMPAQUE_FIL,
        IND_FAB_CEMENTOS_FIL,
        IND_FAB_QUIMICOSMP_FIL,
        IND_FAB_CARGASQUIMICAS_FIL,
        IND_FAB_CARBONATOCOMPRADO_FIL,
        IND_FAB_CARBONATOENPIEDRA_FIL,
        IND_FAB_PRECIOTRANSFERENCIA_FIL,
        IND_FAB_VARIOSADH_FIL,
        IND_FAB_OTROSCOSTOSVENTA_FIL,
        IND_VAR_COMISIONES,
        IND_VAR_MANIOBRAS,
        IND_VAR_FLETES,
        IND_OPE_ADMINISTRACION,
        IND_OPE_VENTAS,
        IND_OPE_LOGISTICA,
        IND_OPE_MARKETING,
        IND_OPE_PROYECTOS,
        IND_OPE_DIRECCIONNEGOCIO,
        IND_OPE_CORPORATIVOSTAFF,
        IND_FAB_MOD_STD,
        IND_FAB_MOI_STD,
        IND_FAB_ELECTRICIDAD_STD,
        IND_FAB_DEPRECIACION_STD,
        IND_FAB_MANTENIMIENTO_STD,
        IND_FAB_GASTOSINDIRECTOS_STD,
        IND_FAB_EMPAQUE_STD,
        IND_FAB_CEMENTOS_STD,
        IND_FAB_QUIMICOSMP_STD,
        IND_FAB_CARGASQUIMICAS_STD,
        IND_FAB_PTCOMPRADO_STD,
        IND_FAB_CARBONATOCOMPRADO_STD,
        IND_FAB_CARBONATOENPIEDRA_STD,
        IND_FAB_SUBCONTRATACIONMAQUILA_STD,
        IND_FAB_VARIOSADH_STD,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA        
        )
        SELECT 

        IFNULL(AC.BUDAT,'1900-01-01'), --FECHA_CONTABILIZACION
        IFNULL(AC.FISCYEARPER,''), --ANIO_MES
        IFNULL(AC.GJAHR,''), --EJERCICIO 
        IFNULL(AC.POPER,''), --PERIODO
        IFNULL(AC.BLDAT,'1900-01-01'),--FECHA_DOCUMENTO
        IFNULL(AC.RLDNR,''), --LEDGER
        IFNULL(LTRIM(AC.BELNR,'0'),''), --DOCUMENTO
        IFNULL(LTRIM(AC.DOCLN,'0'),''), --DOCUMENTO_POS
        IFNULL(LTRIM(AC.XREVERSED,'0'),''),--ANULADA
        IFNULL(LTRIM(AC.KOKRS,'0'),''), --SOCIEDADCO_ID
        IFNULL(LTRIM(AC.RBUKRS,'0'),''), --SOCIEDAD_ID
        IFNULL(LTRIM(AC.VKORG,'0'),''), --ORGVENTAS_ID
        IFNULL(LTRIM(AC.VTWEG,'0'),''), --CANALDISTRIB_ID
        IFNULL(LTRIM(AC.SPART,'0'),''), --SECTOR_ID
        IFNULL(LTRIM(AC.WERKS,'0'),''), --CENTRO_ID
        IFNULL(LTRIM(AC.ZZ1_VKBUR,'0'),''), --OFICINAVENTAS_ID
        IFNULL(CONCAT(LTRIM(AC.KOKRS,'0'),'_',LTRIM(AC.PRCTR,'0')),''), --CENTROBENEFICIO_ID
        IFNULL(CONCAT(LTRIM(AC.KOKRS,'0'),'_',CASE WHEN AC.SCNTR=''THEN LTRIM(AC.RCNTR,'0') ELSE LTRIM(AC.SCNTR,'0') END),''), --CENTROCOSTO_ID       
        IFNULL(CONCAT(LTRIM(AC.KOKRS,'0'),'_',LTRIM(AC.RACCT,'0')),''), --CUENTA_ID
        IFNULL(LTRIM(AC.KUNNR,'0'),''), --CLIENTE_ID
        IFNULL(CONCAT(LTRIM(AC.VKORG,'0'),'_',LTRIM(AC.VTWEG,'0'),'_','00','_',LTRIM(AC.KUNNR,'0')),''), --SOLICITANTE_ID
        IFNULL(CONCAT(LTRIM(AC.VKORG,'0'),'_',LTRIM(AC.VTWEG,'0'),'_','00','_',LTRIM(AC.KUNWE,'0')),''), --DESTINATARIO_ID
        IFNULL(LTRIM(AC.KONZS,'0'),''), --GRUPOSOLICITANTE_ID
        IFNULL(LTRIM(ZSD.CNSTR_ID,'0'),''), --CONSTRUCTORA_ID
        IFNULL(LTRIM(AC.WW004_PA,'0'),''), --TIPO_OBRA_ID
        IFNULL(LTRIM(AC.SUBMI_PA,'0'),''), --ID_PLAN_DE_OBRA
        IFNULL(LTRIM(AC.LAND1,'0'),''), --PAIS_ID
        IFNULL(LTRIM(AC.KMLAND_PA,'0'),''), --PAISCLIENTE_ID
        IFNULL(CONCAT(LTRIM(AC.LAND1,'0'),'_',LTRIM(AC.WW016_PA,'0')),''), --REGION_ID
        IFNULL(CONCAT(LTRIM(AC.LAND1,'0'),'_',LTRIM(AC.WW002_PA,'0')),''), --ZONATRANSPORTE_ID
        IFNULL(LTRIM(AC.MATNR,'0'),''), --MATERIAL_ID
        IFNULL(LTRIM(AC.RHCUR,'0'),''), --CLAVE_MONEDA
        IFNULL(LTRIM(AC.QUNIT2,'0'),''), --UNIDAD_MEDIDA
        IFNULL(CO.ZZCOD_ENC_GASTO,''), --CENTROCOSTO_GRUPO_ID
        IFNULL(SE.SETNAME,''), --CUENTA_GRUPO_ID
        IFNULL(LTRIM(ZSD.ID_CONVENIO,'0'),''), --CONVENIO_OBRA
        IFNULL(AC.HSL,0), --IND_HSL
        IFNULL(AC.QUANT2,0), --IND_QUANT2
        CASE WHEN SETNAME = 'A_MOI' THEN HSL ELSE 0 END + CASE WHEN SETNAME = 'A_MTTO' THEN HSL ELSE 0 END
        +CASE WHEN SETNAME = 'A_DEPN' THEN HSL ELSE 0 END + CASE WHEN SETNAME = 'A_OTR_GIF' THEN HSL ELSE 0 END,--IND_FAB_COSTOVENTAFIJO
        -1*(CASE WHEN SETNAME = 'A_INGRESOS' THEN HSL ELSE 0 END), --IND_VTA_IMP_PRECIOLISTA_ML
        CASE WHEN SETNAME = 'A_DSCTOS' THEN HSL ELSE 0 END, --IND_VTA_IMP_DESCUENTOS_ML
        CASE WHEN SETNAME = 'A_PROMO' THEN HSL ELSE 0 END, --IND_VTA_IMP_PROMOCIONES_ML
        CASE WHEN SETNAME = 'A_RAPPELES' THEN HSL ELSE 0 END, --IND_VTA_IMP_RAPPELES_ML
        CASE WHEN SETNAME = 'A_CORRECT' THEN HSL ELSE 0 END, --IND_VTA_IMP_CORRECTIVAS_ML
        -1*(CASE WHEN SETNAME = 'A_INGRESOS' THEN HSL ELSE 0 END)
        -CASE WHEN SETNAME = 'A_DSCTOS' THEN HSL ELSE 0 END-CASE WHEN SETNAME = 'A_PROMO' THEN HSL ELSE 0 END
        -CASE WHEN SETNAME = 'A_RAPPELES' THEN HSL ELSE 0 END-CASE WHEN SETNAME = 'A_CORRECT' THEN HSL ELSE 0 END, --IND_VTA_IMP_VENTAREAL_ML
        CASE WHEN SETNAME = 'A_INGRESOS' THEN QUANT2 ELSE 0 END, --IND_VTA_IMP_VENTAREAL_TON
        IFNULL(LTRIM(PREC.PRECIO,'0'),0), --IND_VTA_PRECIO_CONSTRUCTORA_ML
        IFNULL(PREC.PRECIO * (CASE WHEN SETNAME = 'A_INGRESOS' THEN QUANT2 ELSE 0 END),0), --IND_VTA_IMP_CONSTRUCTORA_ML
        CASE WHEN SETNAME = 'A_MOD' THEN HSL ELSE 0 END, --IND_FAB_MOD
        CASE WHEN SETNAME = 'A_MOI' THEN HSL ELSE 0 END, --IND_FAB_MOI
        CASE WHEN SETNAME = 'A_ELEC' THEN HSL ELSE 0 END, --IND_FAB_ELECTRICIDAD
        CASE WHEN SETNAME = 'A_DEPN' THEN HSL ELSE 0 END, --IND_FAB_DEPRECIACION
        CASE WHEN SETNAME = 'A_MTTO' THEN HSL ELSE 0 END, --IND_FAB_MANTENIMIENTO
        CASE WHEN SETNAME = 'A_OTR_GIF' THEN HSL ELSE 0 END, --IND_FAB_GASTOSINDIRECTOS
        CASE WHEN SETNAME = 'A_EMPAQUE' THEN HSL ELSE 0 END, --IND_FAB_EMPAQUE
        CASE WHEN SETNAME = 'A_CEMENTO' THEN HSL ELSE 0 END, --IND_FAB_CEMENTOS
        CASE WHEN SETNAME = 'A_QUIMICOS' THEN HSL ELSE 0 END, --IND_FAB_QUIMICOSMP
        CASE WHEN SETNAME = 'A_CARGQUIM' THEN HSL ELSE 0 END, --IND_FAB_CARGASQUIMICAS
        CASE WHEN SETNAME = 'A_PT_COMP' THEN HSL ELSE 0 END, --IND_FAB_PTCOMPRADO
        CASE WHEN SETNAME = 'A_CARB_COM' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOCOMPRADO
        CASE WHEN SETNAME = 'A_CARB_PIE' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOPIEDRA
        CASE WHEN SETNAME = 'A_SUBC_MAQ' THEN HSL ELSE 0 END, --IND_FAB_SUBCONTRATACIONMAQUILA
        CASE WHEN SETNAME = 'A_OTR_CV' THEN HSL ELSE 0 END, --IND_FAB_OTROCOSTOSVENTA
        CASE WHEN SETNAME = 'A_VARIOS' THEN HSL ELSE 0 END, --IND_FAB_VARIOSADH
        CASE WHEN SETNAME = 'A_MOD_FIL' THEN HSL ELSE 0 END, --IND_FAB_MOD_FIL
        CASE WHEN SETNAME = 'A_MOI_FIL' THEN HSL ELSE 0 END, --IND_FAB_MOI_FIL
        CASE WHEN SETNAME = 'A_ELEC_FIL' THEN HSL ELSE 0 END, --IND_FAB_ELECTRICIDAD_FIL
        CASE WHEN SETNAME = 'A_DEPN_FIL' THEN HSL ELSE 0 END, --IND_FAB_DEPRECIACION_FIL
        CASE WHEN SETNAME = 'A_MTTO_FIL' THEN HSL ELSE 0 END, --IND_FAB_MANTENIMIENTO_FIL
        CASE WHEN SETNAME = 'A_OGIF_FIL' THEN HSL ELSE 0 END, --IND_FAB_GASTOSINDIRECTOS_FIL
        CASE WHEN SETNAME = 'A_EMP_FIL' THEN HSL ELSE 0 END, --IND_FAB_EMPAQUE_FIL
        CASE WHEN SETNAME = 'A_CEM_FIL' THEN HSL ELSE 0 END, --IND_FAB_CEMENTOS_FIL
        CASE WHEN SETNAME = 'A_QUIM_FIL' THEN HSL ELSE 0 END, --IND_FAB_QUIMICOSMP_FIL
        CASE WHEN SETNAME = 'A_CQ_FIL' THEN HSL ELSE 0 END, --IND_FAB_CARGASQUIMICAS_FIL
        CASE WHEN SETNAME = 'A_CAC_FIL' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOCOMPRADO_FIL
        CASE WHEN SETNAME = 'A_CAPI_FIL' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOENPIEDRA_FIL
        CASE WHEN SETNAME = 'A_PRTR_FIL' THEN HSL ELSE 0 END, --IND_FAB_PRECIOTRANSFERENCIA_FIL
        CASE WHEN SETNAME = 'A_VAR_FIL' THEN HSL ELSE 0 END, --IND_FAB_VARIOSADH_FIL
        CASE WHEN SETNAME = 'A_OTCV_FIL' THEN HSL ELSE 0 END, --IND_FAB_OTROSCOSTOSVENTA_FIL
        CASE WHEN SETNAME = 'A_COMSNS' THEN HSL ELSE 0 END, --IND_VAR_COMISIONES
        CASE WHEN SETNAME = 'A_MANIOB' THEN HSL ELSE 0 END, --IND_VAR_MANIOBRAS
        CASE WHEN SETNAME = 'A_FLETES' THEN HSL ELSE 0 END, --IND_VAR_FLETES
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '03'  THEN HSL ELSE 0 END, --IND_OPE_ADMINISTRACION
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '04'  THEN HSL ELSE 0 END, --IND_OPE_VENTAS
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '05'  THEN HSL ELSE 0 END, --IND_OPE_LOGISTICA
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '06'  THEN HSL ELSE 0 END, --IND_OPE_MARKETING
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '07'  THEN HSL ELSE 0 END, --IND_OPE_PROYECTOS
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '08'  THEN HSL ELSE 0 END, --IND_OPE_DIRECCIONNEGOCIO
        CASE WHEN SETNAME = 'A_GTO_OP' AND ZZCOD_ENC_GASTO = '09'  THEN HSL ELSE 0 END, --IND_OPE_CORPORATIVOSTAFF
        CASE WHEN SETNAME = 'A_MOD_STD' THEN HSL ELSE 0 END, --IND_FAB_MOD_STD
        CASE WHEN SETNAME = 'A_MOI_STD' THEN HSL ELSE 0 END, --IND_FAB_MOI_STD
        CASE WHEN SETNAME = 'A_ELEC_STD' THEN HSL ELSE 0 END, --IND_FAB_ELECTRICIDAD_STD
        CASE WHEN SETNAME = 'A_DEPN_STD' THEN HSL ELSE 0 END, --IND_FAB_DEPRECIACION_STD
        CASE WHEN SETNAME = 'A_MTTO_STD' THEN HSL ELSE 0 END, --IND_FAB_MANTENIMIENTO_STD
        CASE WHEN SETNAME = 'A_OGIF_STD' THEN HSL ELSE 0 END, --IND_FAB_GASTOSINDIRECTOS_STD
        CASE WHEN SETNAME = 'A_EMP_STD' THEN HSL ELSE 0 END, --IND_FAB_EMPAQUE_STD
        CASE WHEN SETNAME = 'A_CEMTO_ST' THEN HSL ELSE 0 END, --IND_FAB_CEMENTOS_STD
        CASE WHEN SETNAME = 'A_QUIM_STD' THEN HSL ELSE 0 END, --IND_FAB_QUIMICOSMP_STD
        CASE WHEN SETNAME = 'A_CQ_STD' THEN HSL ELSE 0 END, --IND_FAB_CARGASQUIMICAS_STD
        CASE WHEN SETNAME = 'A_PTC_STD' THEN HSL ELSE 0 END, --IND_FAB_PTCOMPRADO_STD
        CASE WHEN SETNAME = 'A_CAC_STD' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOCOMPRADO_STD
        CASE WHEN SETNAME = 'A_CAPI_STD' THEN HSL ELSE 0 END, --IND_FAB_CARBONATOENPIEDRA_STD
        CASE WHEN SETNAME = 'A_SUBM_STD' THEN HSL ELSE 0 END, --IND_FAB_SUBCONTRATACIONMAQUILA_STD
        CASE WHEN SETNAME = 'A_VAR_STD' THEN HSL ELSE 0 END, --IND_FAB_VARIOSADH_STD 
        AC.SISORIGEN_ID, --SISORIGEN_ID
        AC.MANDANTE, --MANDANTE
        AC.FECHA_CARGA, --FECHA_CARGA
        AC.ZONA_HORARIA --ZONA_HORARIA
        FROM 
        --ELIMINACIÓN DE DUPLICADOS ACDOCA, TOMANDO EL REGISTRO CON FECHA DE ACTUALIZACIÓN MÁS RECIENTE
        (SELECT * FROM
        (SELECT 
        ROW_NUMBER () OVER (PARTITION BY RLDNR,RBUKRS,GJAHR,BELNR,DOCLN ORDER BY LAST_CHANGE_DATETIME DESC) AS ROW_NUMBER
        ,* 
        FROM RAW.SQ1_TBL_ACDOCA AC ) WHERE ROW_NUMBER=1) AC
        --LEFT JOIN (SELECT * FROM RAW.SQ1_TBL_SETLEAF WHERE SUBCLASS='PCGL' AND STARTSWITH(SETNAME,'A_')) SE ON AC.RACCT BETWEEN SE.VALFROM AND SE.VALTO
        LEFT JOIN RAW.SQ1_TBL_SETLEAF SE ON AC.RACCT BETWEEN SE.VALFROM AND SE.VALTO
        LEFT JOIN RAW.SQ1_EXT_0COSTCENTER_ATTR CO ON CASE WHEN AC.SCNTR=''THEN AC.RCNTR ELSE AC.SCNTR END=CO.KOSTL
        LEFT JOIN RAW.SQ1_TBL_ZSD_CONVENIO_HDR ZSD ON LEFT(SUBMI_PA,8)=ZSD.ID_CONVENIO
        LEFT JOIN
        (SELECT * FROM
        (SELECT 
        ROW_NUMBER () OVER (PARTITION BY ID_CONVENIO,MATNR  ORDER BY ZONA_PREC ASC) AS ROW_NUMBER
        ,* 
        FROM RAW.SQ1_TBL_ZSD_CONV_PRECMAT P ) WHERE ROW_NUMBER=1) PREC ON LEFT(AC.SUBMI_PA,8)=PREC.ID_CONVENIO AND AC.MATNR=PREC.MATNR
        --FILTRADO DE PLANES DE CUENTA PCGL QUE EMPIECEN CON A_ Y QUE NO SEAN CANAL DE DISTRIBUCIÓN VACIOS
        WHERE AC.VTWEG<>'' AND SUBCLASS='PCGL' AND STARTSWITH(SETNAME,'A_');

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_ADH_RENTABILIDAD;

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
        VALUES('SP_PRE_PFCT_FIN_ADH_RENTABILIDAD','PFCT_FIN_ADH_RENTABILIDAD', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;