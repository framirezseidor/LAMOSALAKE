CREATE OR ALTER TABLE CON.DIM_MAT_INDICADOR_ABC(		
INDICADOR_ABC_ID	VARCHAR(10)	PRIMARY KEY,
INDICADOR_ABC_TEXT	VARCHAR(70)	,
SISORIGEN_ID	VARCHAR(4)	,
MANDANTE	VARCHAR(4)	,
FECHA_CARGA	TIMESTAMP_TZ	,
ZONA_HORARIA	VARCHAR(4)	
)		
;