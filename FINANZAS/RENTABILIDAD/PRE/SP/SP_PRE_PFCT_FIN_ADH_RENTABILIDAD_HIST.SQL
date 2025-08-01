CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_FIN_ADH_RENTABILIDAD_HIST()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-07-02
 Creador:            Carlos Panta
 Descripción:        SP que transforma datos desde la capa RAW a PRE para SP_PRE_PFCT_FIN_ADH_RENTABILIDAD_HIST
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

        DELETE FROM PRE.PFCT_FIN_ADH_RENTABILIDAD_HIST;

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

        INSERT INTO PRE.PFCT_FIN_ADH_RENTABILIDAD_HIST
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
        IND_FAB_PTMAQUILA,
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
        IND_FAB_PTMAQUILA_STD,
        SISORIGEN_ID,
        MANDANTE,
        FECHA_CARGA,
        ZONA_HORARIA        
        )
        SELECT

        IFNULL(last_day(to_date(concat('01',replace("Clave [0CALMONTH].[20CALMONTH]",'.','')),'DDMMYYYY')),'1900-01-01'), --FECHA_CONTABILIZACION
        IFNULL(CONCAT(SUBSTR("Clave [0CALMONTH].[20CALMONTH]",4,4),'0',SUBSTR("Clave [0CALMONTH].[20CALMONTH]",1,2)),''), --ANIO_MES
        IFNULL("Clave [0CALYEAR].[20CALYEAR]",''), --EJERCICIO 
        IFNULL(CASE WHEN LEN("Clave [0CALMONTH2].[20CALMONTH2]")=1 THEN CONCAT('00',"Clave [0CALMONTH2].[20CALMONTH2]") ELSE CONCAT('0',"Clave [0CALMONTH2].[20CALMONTH2]") END,''), --PERIODO
        IFNULL(last_day(to_date(concat('01',replace("Clave [0CALMONTH].[20CALMONTH]",'.','')),'DDMMYYYY')),'1900-01-01'),--FECHA_DOCUMENTO
        '', --LEDGER
        '', --DOCUMENTO
        '', --DOCUMENTO_POS
        '',--ANULADA
        IFNULL("Clave [0CO_AREA].[20CO_AREA]",''), --SOCIEDADCO_ID
        IFNULL("Clave [0COMP_CODE].[20COMP_CODE]",''), --SOCIEDAD_ID
        IFNULL("Clave [0SALESORG].[20SALESORG]",''), --ORGVENTAS_ID
        IFNULL("Clave [0DISTR_CHAN].[20DISTR_CHAN]",''), --CANALDISTRIB_ID
        IFNULL("Clave [0DIVISION].[20DIVISION]",''), --SECTOR_ID
        IFNULL("Clave [0PLANT].[20PLANT]",''), --CENTRO_ID
        IFNULL("Clave [0SALES_OFF].[20SALES_OFF]",''), --OFICINAVENTAS_ID
        '', --CENTROBENEFICIO_ID
        '', --CENTROCOSTO_ID   
        '', --CUENTA_ID
        IFNULL(split("Clave [ZIODESTM].[2ZIODESTM]",'/')[3],''), --CLIENTE_ID
        IFNULL(CONCAT(split("Clave [ZIOCUSSLS].[2ZIOCUSSLS]",'/')[2],'_',split("Clave [ZIOCUSSLS].[2ZIOCUSSLS]",'/')[1],'_',split("Clave [ZIOCUSSLS].[2ZIOCUSSLS]",'/')[0],'_',split("Clave [ZIOCUSSLS].[2ZIOCUSSLS]",'/')[3]),''), --SOLICITANTE_ID
        IFNULL(CONCAT(split("Clave [ZIODESTM].[2ZIODESTM]",'/')[2],'_',split("Clave [ZIODESTM].[2ZIODESTM]",'/')[1],'_',split("Clave [ZIODESTM].[2ZIODESTM]",'/')[0],'_',split("Clave [ZIODESTM].[2ZIODESTM]",'/')[3]),''), --DESTINATARIO_ID
        '', --GRUPOSOLICITANTE_ID
        IFNULL("Clave [ZIOCONTID].[2ZIOCONTID]",''), --CONSTRUCTORA_ID
        IFNULL("Clave [ZIOTOBRA].[2ZIOTOBRA]",''), --TIPO_OBRA_ID
        '', --ID_PLAN_DE_OBRA
        IFNULL("Clave [0COUNTRY].[20COUNTRY]",''), --PAIS_ID
        IFNULL("Clave [0COUNTRY].[20COUNTRY]",''), --PAISCLIENTE_ID
        IFNULL(REPLACE("Clave [0REGION].[20REGION]",'/','_'),''), --REGION_ID
        IFNULL(REPLACE("Clave [ZIOZNTRA].[2ZIOZNTRA]",'/','_'),''), --ZONATRANSPORTE_ID
        IFNULL("Clave [ZIOMATER].[2ZIOMATER]",''), --MATERIAL_ID
        IFNULL("Clave [0CURRENCY].[20CURRENCY]",''), --CLAVE_MONEDA
        IFNULL("Unit of Venta Real TON [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZSP9R].[UNIT]",''), --UNIDAD_MEDIDA
        '', --CENTROCOSTO_GRUPO_ID
        '', --CUENTA_GRUPO_ID
        '', --CONVENIO_OBRA
        0, --IND_HSL
        0, --IND_QUANT2
        0, --IND_FAB_COSTOVENTAFIJO
        IFNULL("Venta Precio Lista ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZA38V]",0), --IND_VTA_IMP_PRECIOLISTA_ML
        IFNULL("Importe Descuentos ML0 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZA9KF]",0), --IND_VTA_IMP_DESCUENTOS_ML
        IFNULL("Importe Promociones ML0 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZAFVZ]",0), --IND_VTA_IMP_PROMOCIONES_ML
        IFNULL("Importe Rappeles ML0 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZASJ3]",0), --IND_VTA_IMP_RAPPELES_ML
        IFNULL("Importe Correctivas ML0 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZAYUN]",0), --IND_VTA_IMP_CORRECTIVAS_ML
        IFNULL("Venta Real ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZB567]",0), --IND_VTA_IMP_VENTAREAL_ML
        IFNULL("Venta Real TON [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZSP9R]",0), --IND_VTA_IMP_VENTAREAL_TON
        0, --IND_VTA_PRECIO_CONSTRUCTORA_ML
        IFNULL("Venta Constructora ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZBBHR]",0), --IND_VTA_IMP_CONSTRUCTORA_ML
        IFNULL("TA.1.a  MOD
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZBUGF]",0), --IND_FAB_MOD
        IFNULL("TA.1.b  MOI
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZC0RZ]",0), --IND_FAB_MOI
        IFNULL("TA.1.c  Electricidad [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZG8HB]",0), --IND_FAB_ELECTRICIDAD
        IFNULL("TA.1.d  Depreciación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGESV]",0), --IND_FAB_DEPRECIACION
        IFNULL("TA.1.e  Mantenimiento (Conserv.)
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGL4F]",0), --IND_FAB_MANTENIMIENTO
        IFNULL("TA.1.g  Otros
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGXRJ]",0), --IND_FAB_GASTOSINDIRECTOS
        IFNULL("TA.1.h  Empaque
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZH433]",0), --IND_FAB_EMPAQUE
        IFNULL("TA.1.i  Cementos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHAEN]",0), --IND_FAB_CEMENTOS
        IFNULL("TA.1.j  Químicos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHGQ7]",0), --IND_FAB_QUIMICOSMP
        0, --IND_FAB_CARGASQUIMICAS
        IFNULL("TA.1.k  PT Comprado [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHN1R]",0), --IND_FAB_PTCOMPRADO
        IFNULL("TA.1.n Carbonatos [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZI60F]",0), --IND_FAB_CARBONATOCOMPRADO
        0, --IND_FAB_CARBONATOPIEDRA
        IFNULL("TA.1.m Subcontratación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHZOV]",0), --IND_FAB_SUBCONTRATACIONMAQUILA
        0, --IND_FAB_OTROCOSTOSVENTA
        0, --IND_FAB_VARIOSADH
        IFNULL("TA.1.l  PT Maquila
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHTDB]",0), --IND_FAB_PTMAQUILA
        0, --IND_FAB_MOD_FIL
        0, --IND_FAB_MOI_FIL
        0, --IND_FAB_ELECTRICIDAD_FIL
        0, --IND_FAB_DEPRECIACION_FIL
        0, --IND_FAB_MANTENIMIENTO_FIL
        0, --IND_FAB_GASTOSINDIRECTOS_FIL
        0, --IND_FAB_EMPAQUE_FIL
        0, --IND_FAB_CEMENTOS_FIL
        0, --IND_FAB_QUIMICOSMP_FIL
        0, --IND_FAB_CARGASQUIMICAS_FIL
        0, --IND_FAB_CARBONATOCOMPRADO_FIL
        0, --IND_FAB_CARBONATOENPIEDRA_FIL
        0, --IND_FAB_PRECIOTRANSFERENCIA_FIL
        0, --IND_FAB_VARIOSADH_FIL
        0, --IND_FAB_OTROSCOSTOSVENTA_FIL
        IFNULL("TA.2.a  Comisiones [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZICBZ]",0), --IND_VAR_COMISIONES
        IFNULL("TA.2.b  Maniobras
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIINJ]",0), --IND_VAR_MANIOBRAS
        IFNULL("TA.2.c  Fletes [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIOZ3]",0), --IND_VAR_FLETES
        IFNULL("TC.a Administración [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIVAN]",0), --IND_OPE_ADMINISTRACION
        IFNULL("TC.d Ventas [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJE9B]",0), --IND_OPE_VENTAS
        IFNULL("TC.e Logística [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJKKV]",0), --IND_OPE_LOGISTICA
        IFNULL("TC.g Marketing
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJX7Z]",0), --IND_OPE_MARKETING
        IFNULL("TC.f Proyectos [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJQWF]",0), --IND_OPE_PROYECTOS
        IFNULL("TC.b Dirección Negocios [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJ1M7]",0), --IND_OPE_DIRECCIONNEGOCIO
        IFNULL("TC.c Corporativo [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJ7XR]",0), --IND_OPE_CORPORATIVOSTAFF
        IFNULL("ST.a MOD
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZU3U7]",0), --IND_FAB_MOD_STD
        IFNULL("ST.b MOI [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUA5R]",0), --IND_FAB_MOI_STD
        IFNULL("ST.c Electricidad [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUGHB]",0), --IND_FAB_ELECTRICIDAD_STD
        IFNULL("ST.d Depreciación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUMSV]",0), --IND_FAB_DEPRECIACION_STD
        IFNULL("ST.e Mantenimiento (Conserv.) [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUT4F]",0), --IND_FAB_MANTENIMIENTO_STD
        IFNULL("ST.f Otros [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUZFZ]",0), --IND_FAB_GASTOSINDIRECTOS_STD
        IFNULL("ST.g Empaque
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZV5RJ]",0), --IND_FAB_EMPAQUE_STD
        IFNULL("ST.h Cementos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVC33]",0), --IND_FAB_CEMENTOS_STD
        IFNULL("ST.i Químicos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVIEN]",0), --IND_FAB_QUIMICOSMP_STD
        0, --IND_FAB_CARGASQUIMICAS_STD
        0, --IND_FAB_PTCOMPRADO_STD
        IFNULL("ST.l Carbonatos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZW1DB]",0), --IND_FAB_CARBONATOCOMPRADO_STD
        0, --IND_FAB_CARBONATOENPIEDRA_STD
        IFNULL("ST.k Subcontratación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVV1R]",0), --IND_FAB_SUBCONTRATACIONMAQUILA_STD
        0, --IND_FAB_VARIOSADH_STD 
        IFNULL("ST.j PT Maquila
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVOQ7]",0), --IND_FAB_PTMAQUILA_STD
        SISORIGEN_ID, --SISORIGEN_ID
        MANDANTE, --MANDANTE
        FECHA_CARGA, --FECHA_CARGA
        ZONA_HORARIA --ZONA_HORARIA
        FROM RAW.BW4_RENTABILIDAD_HIST;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_FIN_ADH_RENTABILIDAD_HIST;

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
        VALUES('SP_PRE_PFCT_FIN_ADH_RENTABILIDAD_HIST','PFCT_FIN_ADH_RENTABILIDAD_HIST', :F_INICIO, :F_FIN,:T_EJECUCION ,:ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;