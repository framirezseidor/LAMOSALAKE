create or alter TABLE PRE.PDIM_GEO_PAIS (
	PAIS_ID VARCHAR(3) NOT NULL,
    PAIS_TEXT VARCHAR(15) NOT NULL,
    SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_GEO_PAIS PRIMARY KEY (PAIS_ID)

);