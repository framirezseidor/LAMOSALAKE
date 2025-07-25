create or replace TABLE MIRRORING.DIM_ORG_UENADHESIVOS (
	UENADHESIVOS_ID VARCHAR(4) NOT NULL,
	UENADHESIVOS_TEXT VARCHAR(30) NOT NULL,
    UENADHESIVOS_ID_TEXT VARCHAR(34) NOT NULL,
	SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,
	constraint PK_DIM_ORG_UENADHESIVOS primary key (UENADHESIVOS_ID)
);


--CREACION DE STREAM EN LA TABLA DE CONSUMO
CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_ORG_UENADHESIVOS ON TABLE MIRRORING.DIM_ORG_UENADHESIVOS;