create or replace TABLE MIRRORING.TR_FCT_COM_ADH_VENTAS_HIST (
	ANIO VARCHAR(4),
	ANIOMES VARCHAR(6),
	FECHA_COMERCIAL DATE,
	MES VARCHAR(2),
	ALMACENCENTRO_ID VARCHAR(10),
	ALMACEN_ID VARCHAR(4),
	CANALDISTRIB_ID VARCHAR(2),
	CENTRO_ID VARCHAR(4),
	GRUPOVENDEDORES_ID VARCHAR(3),
	OFICINAVENTAS_ID VARCHAR(4),
	ORGVENTAS_ID VARCHAR(4),
	SECTOR_ID VARCHAR(2),
	SOCIEDAD_ID VARCHAR(4),
	UENADHESIVOS_ID VARCHAR(4),
	ZONAVENTAS_ID VARCHAR(6),
	PAIS_ID VARCHAR(3),
	REGION_ID VARCHAR(18),
	ZONATRANSPORTE_ID VARCHAR(20),
	EJECUTIVOCIS_ID VARCHAR(1),
	ASESORFACTURA_ID VARCHAR(10),
	TIENDARECIBO_ID VARCHAR(10),
	INCOTERMS_ID VARCHAR(3),
	ASESORPEDIDO_ID VARCHAR(10),
	MOTIVOPEDIDO_ID VARCHAR(3),
	CONDICIONEXP_ID VARCHAR(2),
	TIPOPOSICION_ID VARCHAR(4),
	BOR_BLOQUEOENTREGA_ID VARCHAR(1),
	BOR_BLOQUEOENTREGA_POS_ID VARCHAR(1),
	BOR_CLASEPEDIDO_ID VARCHAR(1),
	BOR_FECHA_BACKORDER VARCHAR(16777216),
	BOR_FECHA_CREACION_PEDIDO VARCHAR(16777216),
	BOR_FECHA_DOCUMENTO VARCHAR(16777216),
	BOR_FECHA_PEDIDO_CLIENTE VARCHAR(16777216),
	BOR_FECHA_PREFERENTE_ENTREGA VARCHAR(16777216),
	BOR_MOTIVORECHAZO_ID VARCHAR(1),
	BOR_PEDIDO VARCHAR(1),
	BOR_PEDIDO_POS NUMBER(1,0),
	BOR_RANGOANTPED_ID VARCHAR(1),
	BOR_STATUSCREDITO_ID VARCHAR(1),
	BOR_STATUS_BLOQUEADO_TOTAL VARCHAR(1),
	PED_CLASEDOC_CONSOLIDADO VARCHAR(1),
	PED_DOCUMENTO VARCHAR(1),
	PED_DOCUMENTO_POS NUMBER(1,0),
	PED_FECHA_DOCUMENTO VARCHAR(16777216),
	PED_TIPODOCUMENTO VARCHAR(1),
	VTA_CLASEFACTURA_ASOCIADO_ID VARCHAR(4),
	VTA_CLASEFACTURA_ID VARCHAR(4),
	VTA_CLASEFACTURA_ORIGEN_ID VARCHAR(4),
	VTA_FACTURA VARCHAR(10),
	VTA_FACTURA_POS NUMBER(6,0),
	VTA_FECHA_FACTURA DATE,
	VTA_INDICADOR_ANULACION VARCHAR(1),
	VTA_ORDENCOMPRA VARCHAR(40),
	VTA_PEDIDO VARCHAR(10),
	VTA_PEDIDO_POS NUMBER(6,0),
	VTA_STATUSCONTABILIDAD VARCHAR(1),
	VTA_TIPO_TRANSACCION VARCHAR(20),
	CONSTRUCTORA_ID VARCHAR(10),
	CONVENIO_OBRA VARCHAR(10),
	NIO_OBRA VARCHAR(20),
	PLAN_OBRA VARCHAR(10),
	PROMOTOR_ID VARCHAR(10),
	SEGMENTO_OBRA_ID VARCHAR(20),
	TIPO_OBRA_ID VARCHAR(2),
	CHOFER VARCHAR(39),
	CLASEEXPEDICION_ID VARCHAR(2),
	CLASETRANSPORTE_ID VARCHAR(4),
	FECHA_DESPACHOEXPREAL DATE,
	NUM_TRANSPORTE VARCHAR(10),
	PUESTOPLANTRANSP_ID VARCHAR(4),
	RUTA_ID VARCHAR(6),
	TIPOTRANSPORTE_ID VARCHAR(10),
	TIPOVEHICULO_ID VARCHAR(10),
	TRANSPORTISTA_ID VARCHAR(10),
	CLIENTE_ID VARCHAR(15),
	DESTINATARIO_ID VARCHAR(30),
	DEUDOR_ID VARCHAR(10),
	SOLICITANTE_ID VARCHAR(30),
	GRUPOARTICULOS_ID VARCHAR(9),
	MATERIALCENTRO_ID VARCHAR(50),
	MATERIALVENTAS_ID VARCHAR(50),
	MATERIAL_ID VARCHAR(40),
	TIPOMATERIAL_ID VARCHAR(4),
	BOR_IND_BO_CONFIRMADO_TON NUMBER(1,0),
	BOR_IND_BO_ENTREGA_TON NUMBER(1,0),
	BOR_IND_BO_IMPORTE_DOC NUMBER(1,0),
	BOR_IND_BO_NO_CONFIRMADO_TON NUMBER(1,0),
	BOR_IND_BO_TOTAL_TON NUMBER(1,0),
	BOR_MON_DOC VARCHAR(1),
	BOR_UNI_EST VARCHAR(1),
	PED_IND_CANT_CONFIRMADA_TON NUMBER(1,0),
	PED_IND_CANT_CONFIRMADA_UMV NUMBER(1,0),
	PED_IND_CANT_ENTREGADA_TON NUMBER(1,0),
	PED_IND_CANT_ENTREGADA_UMV NUMBER(1,0),
	PED_IND_CANT_FACTURADA_TON NUMBER(1,0),
	PED_IND_CANT_FACTURADA_UMV NUMBER(1,0),
	PED_IND_CANT_PEDIDO_TON NUMBER(1,0),
	PED_IND_CANT_PEDIDO_UMV NUMBER(1,0),
	PED_UNI_EST VARCHAR(1),
	PED_UNI_UMV VARCHAR(1),
	VTA_IND_COSTOSINTERNOS_LOC NUMBER(18,5),
	VTA_IND_COSTOSINTERNOS_MXN NUMBER(18,5),
	VTA_IND_COSTOSINTERNOS_USD NUMBER(18,5),
	VTA_IND_PESO_BRUTO NUMBER(18,5),
	VTA_IND_PESO_NETO NUMBER(18,5),
	VTA_IND_VENTA_CORRECTIVAS_LOC NUMBER(18,5),
	VTA_IND_VENTA_CORRECTIVAS_MXN NUMBER(18,5),
	VTA_IND_VENTA_CORRECTIVAS_USD NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOFIN_PRONTOPAGO_LOC NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOFIN_PRONTOPAGO_MXN NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOFIN_PRONTOPAGO_USD NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOS_LOC NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOS_MXN NUMBER(18,5),
	VTA_IND_VENTA_DESCUENTOS_USD NUMBER(18,5),
	VTA_IND_VENTA_FLETES_LOC NUMBER(18,5),
	VTA_IND_VENTA_FLETES_MXN NUMBER(18,5),
	VTA_IND_VENTA_FLETES_USD NUMBER(18,5),
	VTA_IND_VENTA_IMPUESTOS_LOC NUMBER(18,5),
	VTA_IND_VENTA_IMPUESTOS_MXN NUMBER(18,5),
	VTA_IND_VENTA_IMPUESTOS_USD NUMBER(18,5),
	VTA_IND_VENTA_PCP_LOC NUMBER(1,0),
	VTA_IND_VENTA_PCP_TON NUMBER(1,0),
	VTA_IND_VENTA_PCP_USD NUMBER(1,0),
	VTA_IND_VENTA_PRECIOCONSTRUCTORA_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIOCONSTRUCTORA_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIOCONSTRUCTORA_USD NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_IMPUESTO_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_IMPUESTO_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_IMPUESTO_USD NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIOFACTURA_USD NUMBER(18,5),
	VTA_IND_VENTA_PRECIOLISTA_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIOLISTA_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIOLISTA_USD NUMBER(18,5),
	VTA_IND_VENTA_PRECIONETO_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIONETO_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIONETO_USD NUMBER(18,5),
	VTA_IND_VENTA_PRECIOTEORICO_LOC NUMBER(18,5),
	VTA_IND_VENTA_PRECIOTEORICO_MXN NUMBER(18,5),
	VTA_IND_VENTA_PRECIOTEORICO_USD NUMBER(18,5),
	VTA_IND_VENTA_PROMOCIONES_LOC NUMBER(18,5),
	VTA_IND_VENTA_PROMOCIONES_MXN NUMBER(18,5),
	VTA_IND_VENTA_PROMOCIONES_USD NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_IMPUESTOS_LOC NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_IMPUESTOS_MXN NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_IMPUESTOS_USD NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_LOC NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_MXN NUMBER(18,5),
	VTA_IND_VENTA_REAL_FLETES_USD NUMBER(18,5),
	VTA_IND_VENTA_REAL_IMPUESTO_LOC NUMBER(18,5),
	VTA_IND_VENTA_REAL_IMPUESTO_MXN NUMBER(18,5),
	VTA_IND_VENTA_REAL_IMPUESTO_USD NUMBER(18,5),
	VTA_IND_VENTA_REAL_LOC NUMBER(18,5),
	VTA_IND_VENTA_REAL_MXN NUMBER(18,5),
	VTA_IND_VENTA_REAL_TON NUMBER(18,5),
	VTA_IND_VENTA_REAL_UMV NUMBER(18,5),
	VTA_IND_VENTA_REAL_USD NUMBER(18,5),
	VTA_IND_VENTA_REBATES_LOC NUMBER(18,5),
	VTA_IND_VENTA_REBATES_MXN NUMBER(18,5),
	VTA_IND_VENTA_REBATES_USD NUMBER(18,5),
	VTA_MON_LOC VARCHAR(5),
	VTA_MON_MXN VARCHAR(5),
	VTA_MON_USD VARCHAR(5),
	VTA_UNI_EST VARCHAR(3),
	VTA_UNI_PESO VARCHAR(5),
	VTA_UNI_UMV VARCHAR(3),
	FECHA_CARGA TIMESTAMP_TZ(9),
	MANDANTE VARCHAR(4),
	MODELO VARCHAR(6),
	SISORIGEN_ID VARCHAR(4),
	ZONA_HORARIA VARCHAR(4),
	METADATA$ROW_ID VARCHAR(16777216),
	METADATA$ACTION VARCHAR(16777216),
	METADATA$ISUPDATE BOOLEAN
);