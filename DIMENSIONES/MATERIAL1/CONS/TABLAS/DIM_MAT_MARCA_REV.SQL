CREATE OR ALTER TABLE CON.DIM_MAT_MARCA_REV(		
MARCA_REV_ID	VARCHAR(10)	PRIMARY KEY,
MARCA_REV_TEXT	VARCHAR(70)	,
SISORIGEN_ID	VARCHAR(4)	,
MANDANTE	VARCHAR(4)	,
FECHA_CARGA	TIMESTAMP_TZ	,
ZONA_HORARIA	VARCHAR(4)	
)		
;