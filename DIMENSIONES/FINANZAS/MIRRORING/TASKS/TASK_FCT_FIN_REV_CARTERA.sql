CREATE OR REPLACE TASK MIRRORING.TASK_FCT_FIN_REV_CARTERA
    warehouse=LAMOSALAKE_DEV_WH
	WHEN SYSTEM$STREAM_HAS_DATA('STREAM_VW_FCT_FIN_REV_CARTERA')
	AS BEGIN

    -- 1. TRUNCAR tabla temporal
    TRUNCATE TABLE MIRRORING.TR_FCT_FIN_REV_CARTERA;

    -- 2. INSERTAR datos desde el stream a la temporal
    INSERT INTO MIRRORING.TR_FCT_FIN_REV_CARTERA
    SELECT * FROM MIRRORING.STREAM_VW_FCT_FIN_REV_CARTERA;

    -- 3. BORRAR registros eliminados
    DELETE MIRRORING.FCT_FIN_REV_CARTERA AS FACTCARTERA
    USING (
            SELECT * FROM MIRRORING.TR_FCT_FIN_REV_CARTERA
            WHERE METADATA$ACTION = 'DELETE'
    ) AS TR
    WHERE FACTCARTERA.SOCIEDAD_ID = TR.SOCIEDAD_ID
    AND FACTCARTERA.ANIO_MES = TR.ANIO_MES;

    -- 4. INSERTAR o ACTUALIZAR registros nuevos o modificados
    MERGE INTO MIRRORING.FCT_FIN_REV_CARTERA AS FACTCARTERA
    USING (
            SELECT * FROM MIRRORING.TR_FCT_FIN_REV_CARTERA
            WHERE METADATA$ACTION = 'INSERT'
    ) AS TR
    
    ON FACTCARTERA.SOCIEDAD_ID = TR.SOCIEDAD_ID
    AND FACTCARTERA.ANIO_MES = TR.ANIO_MES


    WHEN NOT MATCHED THEN INSERT (
        ANIO,
        MES,
        ANIO_MES,
        SOCIEDAD_ID,
        ORGVENTAS_ID,
        CANALDISTRIB_ID,
        SECTOR_ID,
        OFICINAVENTAS_ID,
        DEUDOR_ID,
        PAIS_ID,
        SOLICITANTE,
        SOLICITANTE_ID,
        DESTINATARIO,
        DESTINATARIO_ID,
        CLASEDOCFI_ID,
        DOCUMENTOFI,
        CENTROBENEFICIO_ID,
        CUENTALIBROMAYOR_ID,
        DOCREFERENCIA,
        FACTURA,
        CLASEFACTURA_ID,
        ASESORFACTURA_ID,
        ASESORFACTURA_TEXT,
        AREACTRLCDTO_ID,
        CLASERIESGO_ID,
        EQUIPORESPCREDITO_ID,
        EQUIPORESPCREDITO_TEXT,
        GRUPOCREDCLIENTE_ID,
        GRUPCREDCLIENTE_TEXT,
        FECHACONTAB,
        FECHAVENCIMIENTO,
        FECHACLAVE,
        IND_IMP_CARTERA_TOTAL_MDOC,
        IND_IMP_VENCIDO_030_MDOC,
        IND_IMP_VENCIDO_3160_MDOC,
        IND_IMP_VENCIDO_6190_MDOC,
        IND_IMP_VENCIDO_MAYOR90_MDOC,
        IND_IMP_VENCIDO_TOTAL_MDOC,
        IND_IMP_PORVENCER_030_MDOC,
        IND_IMP_PORVENCER_3160_MDOC,
        IND_IMP_PORVENCER_6190_MDOC,
        IND_IMP_PORVENCER_MAYOR90_MDOC,
        IND_IMP_PORVENCER_TOTAL_MDOC,
        IND_IMP_COBRANZA_MDOC,
        IND_IMP_FACTURAS_MDOC,
        MON_DOC,
        IND_IMP_CARTERA_TOTAL_MSOC,
        IND_IMP_VENCIDO_030_MSOC,
        IND_IMP_VENCIDO_3160_MSOC,
        IND_IMP_VENCIDO_6190_MSOC,
        IND_IMP_VENCIDO_MAYOR90_MSOC,
        IND_IMP_VENCIDO_TOTAL_MSOC,
        IND_IMP_PORVENCER_030_MSOC,
        IND_IMP_PORVENCER_3160_MSOC,
        IND_IMP_PORVENCER_6190_MSOC,
        IND_IMP_PORVENCER_MAYOR90_MSOC,
        IND_IMP_PORVENCER_TOTAL_MSOC,
        IND_IMP_COBRANZA_MSOC,
        IND_IMP_FACTURAS_MSOC,
        MON_SOC,
        IND_IMP_CARTERA_TOTAL_MUSD,
        IND_IMP_VENCIDO_030_MUSD,
        IND_IMP_VENCIDO_3160_MUSD,
        IND_IMP_VENCIDO_6190_MUSD,
        IND_IMP_VENCIDO_MAYOR90_MUSD,
        IND_IMP_VENCIDO_TOTAL_MUSD,
        IND_IMP_PORVENCER_030_MUSD,
        IND_IMP_PORVENCER_3160_MUSD,
        IND_IMP_PORVENCER_6190_MUSD,
        IND_IMP_PORVENCER_MAYOR90_MUSD,
        IND_IMP_PORVENCER_TOTAL_MUSD,
        IND_IMP_COBRANZA_MUSD,
        IND_IMP_FACTURAS_MUSD,
        MON_USD,
        TIPO
    ) VALUES (
        ANIO,
        MES,
        ANIO_MES,
        SOCIEDAD_ID,
        ORGVENTAS_ID,
        CANALDISTRIB_ID,
        SECTOR_ID,
        OFICINAVENTAS_ID,
        DEUDOR_ID,
        PAIS_ID,
        SOLICITANTE,
        SOLICITANTE_ID,
        DESTINATARIO,
        DESTINATARIO_ID,
        CLASEDOCFI_ID,
        DOCUMENTOFI,
        CENTROBENEFICIO_ID,
        CUENTALIBROMAYOR_ID,
        DOCREFERENCIA,
        FACTURA,
        CLASEFACTURA_ID,
        ASESORFACTURA_ID,
        ASESORFACTURA_TEXT,
        AREACTRLCDTO_ID,
        CLASERIESGO_ID,
        EQUIPORESPCREDITO_ID,
        EQUIPORESPCREDITO_TEXT,
        GRUPOCREDCLIENTE_ID,
        GRUPCREDCLIENTE_TEXT,
        FECHACONTAB,
        FECHAVENCIMIENTO,
        FECHACLAVE,
        IND_IMP_CARTERA_TOTAL_MDOC,
        IND_IMP_VENCIDO_030_MDOC,
        IND_IMP_VENCIDO_3160_MDOC,
        IND_IMP_VENCIDO_6190_MDOC,
        IND_IMP_VENCIDO_MAYOR90_MDOC,
        IND_IMP_VENCIDO_TOTAL_MDOC,
        IND_IMP_PORVENCER_030_MDOC,
        IND_IMP_PORVENCER_3160_MDOC,
        IND_IMP_PORVENCER_6190_MDOC,
        IND_IMP_PORVENCER_MAYOR90_MDOC,
        IND_IMP_PORVENCER_TOTAL_MDOC,
        IND_IMP_COBRANZA_MDOC,
        IND_IMP_FACTURAS_MDOC,
        MON_DOC,
        IND_IMP_CARTERA_TOTAL_MSOC,
        IND_IMP_VENCIDO_030_MSOC,
        IND_IMP_VENCIDO_3160_MSOC,
        IND_IMP_VENCIDO_6190_MSOC,
        IND_IMP_VENCIDO_MAYOR90_MSOC,
        IND_IMP_VENCIDO_TOTAL_MSOC,
        IND_IMP_PORVENCER_030_MSOC,
        IND_IMP_PORVENCER_3160_MSOC,
        IND_IMP_PORVENCER_6190_MSOC,
        IND_IMP_PORVENCER_MAYOR90_MSOC,
        IND_IMP_PORVENCER_TOTAL_MSOC,
        IND_IMP_COBRANZA_MSOC,
        IND_IMP_FACTURAS_MSOC,
        MON_SOC,
        IND_IMP_CARTERA_TOTAL_MUSD,
        IND_IMP_VENCIDO_030_MUSD,
        IND_IMP_VENCIDO_3160_MUSD,
        IND_IMP_VENCIDO_6190_MUSD,
        IND_IMP_VENCIDO_MAYOR90_MUSD,
        IND_IMP_VENCIDO_TOTAL_MUSD,
        IND_IMP_PORVENCER_030_MUSD,
        IND_IMP_PORVENCER_3160_MUSD,
        IND_IMP_PORVENCER_6190_MUSD,
        IND_IMP_PORVENCER_MAYOR90_MUSD,
        IND_IMP_PORVENCER_TOTAL_MUSD,
        IND_IMP_COBRANZA_MUSD,
        IND_IMP_FACTURAS_MUSD,
        MON_USD,
        TIPO
    );
END;