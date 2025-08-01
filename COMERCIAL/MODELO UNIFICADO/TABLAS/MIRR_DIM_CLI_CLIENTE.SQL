CREATE OR REPLACE TABLE MIRRORING.DIM_CLI_CLIENTE (
    CLIENTE_ID VARCHAR(10),
    RAMO_ID VARCHAR(10),
    GRUPOSOLICITANTE_ID VARCHAR(10),
    PAIS_ID VARCHAR(3),
    REGION_ID VARCHAR(10),
    LOCALIDAD VARCHAR(35),
    RFC_ID VARCHAR(16),
    CLASIFICACIONCLIENTE_ID VARCHAR(2),
    CODIGOPOSTAL VARCHAR(10),
    CALLE VARCHAR(35),
    GRUPOCUENTAS_ID VARCHAR(35),
    CLIENTE_TEXT VARCHAR(35),
    CLIENTE_ID_TEXT VARCHAR(100),
    RAMO_TEXT VARCHAR(100),
    GRUPOSOLICITANTE_TEXT VARCHAR(100),
    GRUPOSOLICITANTE_ID_TEXT VARCHAR(100),
    PAIS_TEXT VARCHAR(100),
    CLASIFICACIONCLIENTE_TEXT VARCHAR(100),
    GRUPOCUENTAS_TEXT VARCHAR(100),
    SISORIGEN_ID VARCHAR(4),
    MANDANTE VARCHAR(4),
    FECHA_CARGA TIMESTAMP_TZ,
    ZONA_HORARIA VARCHAR(10)
);

CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_CLI_CLIENTE ON TABLE MIRRORING.DIM_CLI_CLIENTE;