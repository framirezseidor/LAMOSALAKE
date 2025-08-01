create or replace TABLE PRE.PFCT_COM_PEDIDOS_ACT (
ANIO	VARCHAR(4),
MES	VARCHAR(2),
ANIOMES	VARCHAR(10),
SOCIEDAD_ID	VARCHAR(4),
ORGVENTAS_ID	VARCHAR(4),
CANALDISTRIB_ID	VARCHAR(2),
DIVISION	VARCHAR(2),
CENTRO_ID	VARCHAR(4),
OFICINAVENTAS_ID	VARCHAR(4),
ZONAVENTAS_ID	VARCHAR(10),
GRUPOVENDEDORES_ID	VARCHAR(3),
UENADHESIVOS_ID	VARCHAR(4),
PAIS_ID	VARCHAR(3),
REGION_ID	VARCHAR(7),
ZONATRANSPORTE_ID	VARCHAR(20),
TIPODOCUMENTO	VARCHAR(1),
FECHA_DOCUMENTO	DATE,
CLASEDOC_CONSOLIDADO	VARCHAR(4),
DOCUMENTO	VARCHAR(10),
DOCUMENTO_POS	VARCHAR(6),
TIPOPOSICION_ID	VARCHAR(4),
MOTIVOPEDIDO_ID	VARCHAR(3),
INCOTERMS_ID	VARCHAR(3),
ASESORPEDIDO_ID	VARCHAR(10),
CONSTRUCTORA_ID	VARCHAR(10),
TIPO_OBRA_ID	VARCHAR(2),
CONVENIO_OBRA	VARCHAR(10),
PLAN_OBRA	VARCHAR(10),
SEGMENTO_OBRA_ID	VARCHAR(40),
PROMOTOR_ID	VARCHAR(10),
NIO_OBRA	VARCHAR(5),
TIPOTRANSPORTE_ID	VARCHAR(9),
CLIENTE_ID	VARCHAR(10),
SOLICITANTE_ID	VARCHAR(21),
DESTINATARIO_ID	VARCHAR(21),
MATERIAL_ID	VARCHAR(40),
MATERIALVENTAS_ID	VARCHAR(50),
MATERIALCENTRO_ID	VARCHAR(50),
LOTE	VARCHAR(10),
LOTECENTRO_ID	VARCHAR(10),
IND_CANT_PEDIDO_EST	NUMBER(19,3),
IND_CANT_CONFIRMADA_EST	NUMBER(19,3),
IND_CANT_ENTREGADA_EST	NUMBER(19,3),
IND_CANT_FACTURADA_EST	NUMBER(19,3),
UNI_EST	VARCHAR(3),
IND_CANT_PEDIDO_UMV	NUMBER(19,3),
IND_CANT_CONFIRMADA_UMV	NUMBER(19,3),
IND_CANT_ENTREGADA_UMV	NUMBER(19,3),
IND_CANT_FACTURADA_UMV	NUMBER(19,3),
UNI_UMV	VARCHAR(3),
SISORIGEN_ID	VARCHAR(3),
MANDANTE	VARCHAR(4),
FECHA_CARGA	VARCHAR(20),
ZONA_HORARIA	VARCHAR(10)
);