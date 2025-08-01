CREATE OR REPLACE TABLE MIRRORING.DIM_FIN_ENTIDADCP (
	ENTIDADCP_ID VARCHAR(4),
	ENTIDADCP_TEXT VARCHAR(40),
    ENTIDADCP_ID_TEXT VARCHAR(50),
	SISORIGEN_ID VARCHAR(3),
	MANDANTE VARCHAR(50),
	FECHA_CARGA VARCHAR(20),
	ZONA_HORARIA VARCHAR(10)
);

CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_FIN_ENTIDADCP
ON TABLE MIRRORING.DIM_FIN_ENTIDADCP;