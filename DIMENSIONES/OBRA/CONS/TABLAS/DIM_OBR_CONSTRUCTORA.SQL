create or alter TABLE CON.DIM_OBR_CONSTRUCTORA (
    CONSTRUCTORA_ID VARCHAR(10) NOT NULL,
    CONSTRUCTORA_TEXT VARCHAR(60) NOT NULL,
    SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,

    CONSTRAINT PK_DIM_OBR_CONSTRUCTORA PRIMARY KEY (CONSTRUCTORA_ID)
)