create or alter TABLE CON.DIM_OBR_SEGMENTO_OBRA (
	SEGMENTO_OBRA_ID VARCHAR(3) NOT NULL,
    SEGMENTO_OBRA_TEXT VARCHAR(20) NOT NULL,
    SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_OBR_SEGMENTO_OBRA PRIMARY KEY (SEGMENTO_OBRA_ID)

);