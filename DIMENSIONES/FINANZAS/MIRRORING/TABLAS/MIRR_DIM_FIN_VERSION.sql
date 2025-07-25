CREATE OR REPLACE TABLE MIRRORING.DIM_FIN_VERSION(
    VERSION_ID VARCHAR(3),
	VERSION_TEXT VARCHAR(30),
    VERSION_ID_TEXT VARCHAR(40),
	SISORIGEN_ID VARCHAR(3),
	MANDANTE VARCHAR(50),
	FECHA_CARGA VARCHAR(20),
	ZONA_HORARIA VARCHAR(10)
);

CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_FIN_VERSION
ON TABLE MIRRORING.DIM_FIN_VERSION;