create or replace TABLE MIRRORING.DIM_ORG_ORGANIZACIONVENTAS (
    ORGVENTAS_ID VARCHAR(4) NOT NULL,
    SOCIEDAD_ID VARCHAR(4) NOT NULL,
    PAIS_ID VARCHAR(3) NOT NULL,
    NEGOCIOGRUPO_ID VARCHAR(4) NOT NULL,
    ORGVENTAS_TEXT VARCHAR(20) NOT NULL,
    
    ORGVENTAS_ID_TEXT VARCHAR(40) NOT NULL,
    SOCIEDAD_TEXT VARCHAR(25) NOT NULL,
    PAIS_TEXT VARCHAR(15) NOT NULL,
    NEGOCIOGRUPO_TEXT VARCHAR(20) NOT NULL,

    SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_ORG_ORGANIZACIONVENTAS PRIMARY KEY (ORGVENTAS_ID)
);


CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_ORG_ORGANIZACIONVENTAS ON TABLE MIRRORING.DIM_ORG_ORGANIZACIONVENTAS;