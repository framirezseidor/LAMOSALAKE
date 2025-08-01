create or replace TABLE MIRRORING.DIM_ORG_CANALDISTRIBUCION (
    CANALDISTRIB_ID VARCHAR(2) NOT NULL,
    CANALDISTRIB_TEXT VARCHAR(20) NOT NULL,
    CANALDISTRIB_ID_TEXT VARCHAR(30) NOT NULL,    
    SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_ORG_CANALDISTRIBUCION PRIMARY KEY (CANALDISTRIB_ID)
);


CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_ORG_CANALDISTRIBUCION ON TABLE MIRRORING.DIM_ORG_CANALDISTRIBUCION;