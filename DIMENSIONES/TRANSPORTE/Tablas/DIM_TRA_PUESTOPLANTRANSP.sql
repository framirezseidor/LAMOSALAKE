create or replace TABLE PRE.PDIM_TRA_PUESTOPLANTRANSP (
PUESTOPLANTRANSP_ID	VARCHAR(4) PRIMARY KEY,
PUESTOPLANTRANSP_TEXT	VARCHAR(20),
SISORIGEN_ID	VARCHAR(3),
MANDANTE	VARCHAR(4),
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR(12)
);

create or replace TABLE CON.DIM_TRA_PUESTOPLANTRANSP (
PUESTOPLANTRANSP_ID	VARCHAR(4) PRIMARY KEY,
PUESTOPLANTRANSP_TEXT	VARCHAR(20),
SISORIGEN_ID	VARCHAR(3),
MANDANTE	VARCHAR(4),
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR(12)
);
