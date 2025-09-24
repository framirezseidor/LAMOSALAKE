    CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PFCT_COM_VENTAS_HIST()
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
    Descripción:        SP que transforma datos desde la capa RAW A PRE para VENTAS HISTORICAS
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

            TRUNCATE TABLE PRE.PFCT_COM_ADH_VENTAS_HIST;

            SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
            SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
        EXCEPTION
            WHEN statement_error THEN
                SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
        END;

        ---------------------------------------------------------------------------------
        -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA PRE
        ---------------------------------------------------------------------------------
        BEGIN
            SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

            INSERT INTO PRE.PFCT_COM_ADH_VENTAS_HIST
            (
                POSICION_DE_FACTURA, 
                POSICION_DE_FACTURA_CLAVE, 
                FACTURA, 
                FACTURA_CLAVE, 
                CLASE_FACTURA_ORIGINAL, 
                CLASE_FACTURA_ORIGINAL_CLAVE, 
                SOCIEDAD, 
                SOCIEDAD_CLAVE, 
                PAIS, 
                PAIS_CLAVE, 
                CANAL_DISTRIBUCION, 
                CANAL_DISTRIBUCION_CLAVE, 
                INCOTERMS, 
                INCOTERMS_CLAVE, 
                TIPO_POSICION, 
                TIPO_POSICION_CLAVE, 
                GRUPO_MATERIALES_SUBLINEA, 
                GRUPO_MATERIALES_SUBLINEA_CLAVE, 
                MOTIVO_PEDIDO, 
                MOTIVO_PEDIDO_CLAVE, 
                CENTRO, 
                CENTRO_CLAVE, 
                REGION, 
                REGION_CLAVE, 
                REGION_CLAVE_NO_RELACIONADA, 
                ORGANIZACION_VENTAS, 
                ORGANIZACION_VENTAS_CLAVE, 
                ZONA_VENTAS, 
                ZONA_VENTAS_CLAVE, 
                GRUPO_VENDEDORES, 
                GRUPO_VENDEDORES_CLAVE, 
                OFICINA_VENTAS, 
                OFICINA_VENTAS_CLAVE, 
                INDICADOR_DE_ANULACION, 
                INDICADOR_DE_ANULACION_CLAVE, 
                POSICION_PEDIDO, 
                POSICION_PEDIDO_CLAVE, 
                GRUPO_ARTICULOS_LINEA, 
                GRUPO_ARTICULOS_LINEA_CLAVE, 
                TIPO_MATERIAL, 
                TIPO_MATERIAL_CLAVE, 
                STATUS_CONTABILIDAD, 
                STATUS_CONTABILIDAD_CLAVE, 
                CONVENIO, 
                CONVENIO_CLAVE, 
                PEDIDO, 
                PEDIDO_CLAVE, 
                CONSTRUCTORA, 
                CONSTRUCTORA_CLAVE, 
                ORDEN_DE_COMPRA, 
                ORDEN_DE_COMPRA_CLAVE, 
                PROMOTOR, 
                PROMOTOR_CLAVE, 
                TIENDA_RECIBO, 
                TIENDA_RECIBO_CLAVE, 
                SEGMENTO, 
                SEGMENTO_CLAVE, 
                UEN, 
                UEN_CLAVE, 
                ASESOR_PEDIDO, 
                ASESOR_PEDIDO_CLAVE, 
                ASESOR_COMERCIAL_FACTURA, 
                ASESOR_COMERCIAL_FACTURA_CLAVE, 
                CONDICION_EXPEDICION, 
                CONDICION_EXPEDICION_CLAVE, 
                CLASE_FACTURA, 
                CLASE_FACTURA_CLAVE, 
                SOLICITANTE_BONO, 
                SOLICITANTE_BONO_CLAVE, 
                SOLICITANTE_BONO_CLAVE_NO_RELACIONADA, 
                SOLICITANTE, 
                SOLICITANTE_CLAVE, 
                SOLICITANTE_CLAVE_NO_RELACIONADA, 
                DESTINATARIO_MERCANCIA, 
                DESTINATARIO_MERCANCIA_CLAVE, 
                DESTINATARIO_MERCANCIA_CLAVE_NO_RELACIONADA, 
                MATERIAL, 
                MATERIAL_CLAVE, 
                REGION_NEG_ADH, 
                REGION_NEG_ADH_CLAVE, 
                TIPO_OBRA, 
                TIPO_OBRA_CLAVE, 
                ZONA_TRANSPORTE, 
                ZONA_TRANSPORTE_CLAVE, 
                DIA_NATURAL, 
                DIA_NATURAL_CLAVE, 
                ANIOMES_NATURAL, 
                ANIOMES_NATURAL_CLAVE, 
                MONEDA_LOCAL, 
                MONEDA_LOCAL_CLAVE, 
                MEDIDA_DE_VENTA, 
                MEDIDA_DE_VENTA_CLAVE, 
                UNIDAD_PESO, 
                UNIDAD_PESO_CLAVE, 
                MONEDA_MXN, 
                MONEDA_MXN_CLAVE, 
                UNIDAD_ESTADISTICA, 
                UNIDAD_ESTADISTICA_CLAVE, 
                MONEDA_USD, 
                MONEDA_USD_CLAVE, 
                VENTA_REAL_TON, 
                VENTA_REAL_ML, 
                VENTA_REAL_USD, 
                PCP_VENTA_TON, 
                PCP_VENTA_ML, 
                PCP_VENTA_USD, 
                VENTA_REAL_IVA_ML, 
                VENTA_REAL_UM_VENTA, 
                VENTA_PRECIO_LISTA_MXN, 
                VENTA_PRECIO_TEORICO_MXN, 
                VENTA_PRECIO_FACTURA_MXN, 
                VENTA_PRECIO_FACTURA_USD, 
                VENTA_PRECIO_NETO_MXN, 
                VENTA_REAL_MXN, 
                REEMBOLSO_FLETES_MXN, 
                REEMBOLSO_FLETES_USD, 
                VENTA_REAL_IVA_MXN, 
                PESO_NETO_KG, 
                PESO_BRUTO_KG, 
                OBJ_VENTA_TON, 
                OBJ_VENTA_ML, 
                OBJ_VENTA_USD, 
                VENTA_REAL_IVA_USD, 
                VENTA_PRECIO_LISTA_ML, 
                VENTA_PRECIO_TEORICO_ML, 
                VENTA_PRECIO_FACTURA_ML, 
                VENTA_PRECIO_NETO_ML, 
                VENTA_REAL_CFLETES_MXN, 
                VENTA_REAL_CFLETES_IVA_MXN, 
                DESCUENTOS_MXN, 
                PROMOCIONES_MXN, 
                REBATES_MXN, 
                CORRECTIVAS_MXN,
                REEMBOLSO_FLETES_ML,
                VENTA_PRECIO_CONSTRUCTORA_MXN,
                VENTA_PRECIO_CONSTRUCTORA_ML,
                VENTA_PRECIO_CONSTRUCTORA_USD,
                VENTA_REAL_C_FLETES_ML,
                VENTA_REAL_C_FLETES_USD,
                VENTA_REAL_C_FLETES_IVA_ML,
                VENTA_REAL_C_FLETES_IVA_USD,
                DESCUENTOS_ML,
                PROMOCIONES_ML,
                REBATES_ML,
                CORRECTIVAS_ML,
                VENTA_PRECIO_LISTA_USD,
                VENTA_PRECIO_TEORICO_USD,
                VENTA_PRECIO_NETO_USD,
                DESCUENTOS_USD,
                PROMOCIONES_USD,
                REBATES_USD,
                CORRECTIVAS_USD,
                IMPORTE_IVA_MXN,
                IMPORTE_IVA_ML,
                IMPORTE_IVA_USD,
                --NUEVOS 
                MANDANTE, 
                -- SISTEMA_ORIGEN, 
                SISORIGEN_ID, 
                FECHA_CARGA, 
                ZONA_HORARIA
            )
            SELECT
                "[0BILL_ITEM] D.Posición de factura", 
                "[0BILL_ITEM].[20BILL_ITEM] Clave", 
                "[0BILL_NUM] D.Factura", 
                "[0BILL_NUM].[20BILL_NUM] Clave", 
                "[0BILL_TYPE] D.Clase factura (Original)", 
                "[0BILL_TYPE].[20BILL_TYPE] Clave", 
                "[0COMP_CODE] O.Sociedad", 
                "[0COMP_CODE].[20COMP_CODE] Clave", 
                "[0COUNTRY] G.País", 
                "[0COUNTRY].[20COUNTRY] Clave", 
                "[0DISTR_CHAN] O.Canal Distribución", 
                "[0DISTR_CHAN].[20DISTR_CHAN] Clave", 
                "[0INCOTERMS] D.Incoterms", 
                "[0INCOTERMS].[20INCOTERMS] Clave", 
                "[0ITEM_CATEG] D.Tipo posición", 
                "[0ITEM_CATEG].[20ITEM_CATEG] Clave", 
                "[0MAT_KONDM] M.Grupo Materiales (Sublínea)", 
                "[0MAT_KONDM].[20MAT_KONDM] Clave", 
                "[0ORD_REASON] D.Motivo Pedido", 
                "[0ORD_REASON].[20ORD_REASON] Clave", 
                "[0PLANT] O.Centro", 
                "[0PLANT].[20PLANT] Clave", 
                "[0REGION] G.Región", 
                "[0REGION].[20REGION] Clave", 
                "[0REGION].[80REGION] Clave (no relacionada)", 
                "[0SALESORG] O.Organización Ventas", 
                "[0SALESORG].[20SALESORG] Clave", 
                "[0SALES_DIST] O.Zona Ventas", 
                "[0SALES_DIST].[20SALES_DIST] Clave", 
                "[0SALES_GRP] O.Grupo Vendedores", 
                "[0SALES_GRP].[20SALES_GRP] Clave", 
                "[0SALES_OFF] O.Oficina Ventas", 
                "[0SALES_OFF].[20SALES_OFF] Clave", 
                "[0STORNO] D.Indicador de anulación", 
                "[0STORNO].[20STORNO] Clave", 
                "[0S_ORD_ITEM] D.Posición Pedido", 
                "[0S_ORD_ITEM].[20S_ORD_ITEM] Clave", 
                "[4ZSD_CP100-0MATL_GROU_0] M.Grupo Artículos (Línea)", 
                "[4ZSD_CP100-0MATL_GROU_0].[24ZSD_CP100-0MATL_GROU_0] Clave", 
                "[4ZSD_CP100-0MATL_TYPE_0] M.Tipo Material", 
                "[4ZSD_CP100-0MATL_TYPE_0].[24ZSD_CP100-0MATL_TYPE_0] Clave", 
                "[ZCABRFBSK] D.Status Contabilidad", 
                "[ZCABRFBSK].[2ZCABRFBSK] Clave", 
                "[ZCCONVENI] D.Convenio", 
                "[ZCCONVENI].[2ZCCONVENI] Clave", 
                "[ZCDOCVLET] D.Pedido", 
                "[ZCDOCVLET].[2ZCDOCVLET] Clave", 
                "[ZCIDCONST] OB.Constructora", 
                "[ZCIDCONST].[2ZCIDCONST] Clave", 
                "[ZCORDCOMP] D.Orden de Compra", 
                "[ZCORDCOMP].[2ZCORDCOMP] Clave", 
                "[ZCPARZP] Ob.Promotor", 
                "[ZCPARZP].[2ZCPARZP] Clave", 
                "[ZCPARZS] C.Tienda Recibo", 
                "[ZCPARZS].[2ZCPARZS] Clave", 
                "[ZCSEGMENT] OB.Segmento", 
                LEFT("[ZCSEGMENT].[2ZCSEGMENT] Clave", 2) AS SEGMENTO_CLAVE, 
                "[ZCUEN] O.UEN", 
                "[ZCUEN].[2ZCUEN] Clave", 
                "[ZCVBPZ2] D.Asesor Pedido", 
                COALESCE(T4.CODIGO_BP, V."[ZCVBPZ2].[2ZCVBPZ2] Clave") AS ASESOR_PEDIDO_ID, --PONER TRADUCTOR
                "[ZCVBPZV] D.Asesor Comercial Factura", 
                COALESCE(T1.CODIGO_BP, V."[ZCVBPZV].[2ZCVBPZV] Clave") AS ASESOR_COMERCIAL_FACTURA_ID,
                "[ZCVSBED] D.Condición Expedición", 
                "[ZCVSBED].[2ZCVSBED] Clave", 
                "[ZIOBILTYP] D.Clase factura", 
                "[ZIOBILTYP].[2ZIOBILTYP] Clave", 
                "[ZIOCUSSL2] C.Solicitante Bono", 
                "[ZIOCUSSL2].[2ZIOCUSSL2] Clave", 
                COALESCE(T2.CODIGO_BP, V."[ZIOCUSSL2].[8ZIOCUSSL2] Clave (no relacionada)") AS SOLICITANTE_BONO_ID,
                "[ZIOCUSSLS] C.Solicitante", 
                "[ZIOCUSSLS].[2ZIOCUSSLS] Clave", 
                COALESCE(T3.CODIGO_BP, V."[ZIOCUSSLS].[8ZIOCUSSLS] Clave (no relacionada)") AS SOLICITANTE_ID, 
                "[ZIODESTM] C.Destinatario Mercancía", 
                "[ZIODESTM].[2ZIODESTM] Clave", 
                "[ZIODESTM].[8ZIODESTM] Clave (no relacionada)", 
                "[ZIOMATER] M.Material", 
                "[ZIOMATER].[2ZIOMATER] Clave", 
                "[ZIOREGADH] G.Región Neg Adh", 
                "[ZIOREGADH].[2ZIOREGADH] Clave", 
                "[ZIOTOBRA] OB.Tipo Obra", 
                "[ZIOTOBRA].[2ZIOTOBRA] Clave", 
                "[ZIOZNTRA] G.Zona Transporte", 
                "[ZIOZNTRA].[2ZIOZNTRA] Clave", 
                "[0CALDAY] T.Día natural", 
                "[0CALDAY].[20CALDAY] Clave", 
                "[0CALMONTH] T.Año/Mes natural", 
                "[0CALMONTH].[20CALMONTH] Clave", 
                "[0LOC_CURRCY] UNID.Moneda local", 
                "[0LOC_CURRCY].[20LOC_CURRCY] Clave", 
                "[0SALES_UNIT] UNID.Medida de venta", 
                "[0SALES_UNIT].[20SALES_UNIT] Clave", 
                "[0UNIT_OF_WT] UNID.Unidad peso", 
                "[0UNIT_OF_WT].[20UNIT_OF_WT] Clave", 
                "[ZIOMXPCUR] UNID.Moneda MXN", 
                "[ZIOMXPCUR].[2ZIOMXPCUR] Clave", 
                "[ZIOSDEST] UNID.Unidad Estadística", 
                "[ZIOSDEST].[2ZIOSDEST] Clave", 
                "[ZIOUSDCUR] UNID.Moneda USD", 
                "[ZIOUSDCUR].[2ZIOUSDCUR] Clave", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3DKWC] Venta Real TON", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3DR7W] Venta Real $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3DXJG] Venta Real $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3E3V0] PCP Venta TON", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3EA6K] PCP Venta $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3EGI4] PCP Venta $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3EMTO] Venta Real IVA $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3ET58] Venta Real UM Venta", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3EZGS] Venta Precio Lista $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3F5SC] Venta Precio Teórico $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3FC3W] Venta Precio Factura $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3FIFG] Venta Precio Factura $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3FOR0] Venta Precio Neto $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3FV2K] Venta Real $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3G1E4] Reembolso Fletes $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3G7PO] Reembolso Fletes $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3GE18] Venta Real IVA $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3GKCS] Peso Neto (KG)", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3GQOC] Peso Bruto (KG)", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3GWZW] OBJ Venta TON", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3H3BG] OBJ Venta $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3H9N0] OBJ Venta $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3HFYK] Venta Real IVA $USD", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3HMA4] Venta Precio Lista $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3HSLO] Venta Precio Teórico $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3HYX8] Venta Precio Factura $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3I58S] Venta Precio Neto $ML", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3IUJ0] Venta Real c/Fletes $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3J764] Venta Real c/Fletes + IVA $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3K93G] Descuentos $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3KFF0] Promociones $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3KLQK] Rebates $MXN", 
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ5S7J08L3KS24] Correctivas $MXN",
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LSZT68D7WS6] Reembolso Fletes $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTAVMD7O5YM] Venta Precio Constructora $MXN" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTAVMD7OCA6] Venta Precio Constructora $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTAVMD7OILQ] Venta Precio Constructora $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTIEMBVU1S9] Venta Real c/Fletes $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTIEMBVU83T] Venta Real c/Fletes $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTIEMBVUEFD] Venta Real c/Fletes + IVA $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTIEMBVUKQX] Venta Real c/Fletes + IVA $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTM3TGD39GP] Descuentos $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTM3TGD3FS9] Promociones $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTM3TGD3M3T] Rebates $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTM3TGD3SFD] Correctivas $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIAEX1] Venta Precio Lista $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIAL8L] Venta Precio Teórico $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIARK5] Venta Precio Neto $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIAXVP] Descuentos $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIB479] Promociones $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIBAIT] Rebates $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTQFULIBGUD] Correctivas $USD" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTUW3SWGXZZ] Importe IVA $MXN" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTUW3SWH4BJ] Importe IVA $ML" ,
                "[CDZB3U1AVKXXZ5S7J08L3IO7G].[CDZB3U1AVKXXZ8LTUW3SWHAN3] Importe IVA $USD" , 
                MANDANTE, 
                -- SISTEMA_ORIGEN, 
                SISORIGEN_ID, 
                FECHA_CARGA, 
                ZONA_HORARIA
            FROM RAW.BW4_VENTAS_HIST_150725 V
            LEFT JOIN RAW.TRADUCTOR_BP_ECC_TO_S4H AS T1 ON T1.CODIGO = "[ZCVBPZV].[2ZCVBPZV] Clave"
            LEFT JOIN RAW.TRADUCTOR_BP_ECC_TO_S4H AS T2 ON T2.CODIGO = "[ZIOCUSSL2].[8ZIOCUSSL2] Clave (no relacionada)"
            LEFT JOIN RAW.TRADUCTOR_BP_ECC_TO_S4H AS T3 ON T3.CODIGO = "[ZIOCUSSLS].[8ZIOCUSSLS] Clave (no relacionada)"    
            LEFT JOIN RAW.TRADUCTOR_BP_ECC_TO_S4H AS T4 ON T4.CODIGO = "[ZCVBPZ2].[2ZCVBPZ2] Clave";

            -- Conteo de filas insertadas
            SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PFCT_COM_ADH_VENTAS_HIST;

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
        VALUES ('SP_PRE_PFCT_COM_VENTAS_HIST','PRE.PFCT_COM_ADH_VENTAS_HIST', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

        ---------------------------------------------------------------------------------
        -- STEP 5: FINALIZACIÓN
        ---------------------------------------------------------------------------------
        RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

    END;
    $$;

