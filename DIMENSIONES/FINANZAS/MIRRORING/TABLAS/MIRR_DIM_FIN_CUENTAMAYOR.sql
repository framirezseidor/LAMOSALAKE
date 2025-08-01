CREATE OR REPLACE TABLE MIRRORING.DIM_FIN_CUENTAMAYOR (
	CUENTAMAYOR_ID VARCHAR(15),
	CUENTAMAYOR VARCHAR(10),
	CUENTAMAYOR_TEXT VARCHAR(50),
    CUENTAMAYOR_ID_TEXT VARCHAR(70),
	PLANCUENTAS_ID VARCHAR(4),
	SISORIGEN_ID VARCHAR(3),
	MANDANTE VARCHAR(50),
	FECHA_CARGA VARCHAR(20),
	ZONA_HORARIA VARCHAR(10)
);

CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_FIN_CUENTAMAYOR
ON TABLE MIRRORING.DIM_FIN_CUENTAMAYOR;