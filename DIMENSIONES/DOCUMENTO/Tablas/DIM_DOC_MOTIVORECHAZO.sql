create or replace TABLE PRE.PDIM_DOC_MOTIVORECHAZO (
MOTIVORECHAZO_ID	VARCHAR PRIMARY KEY,
MOTIVORECHAZO_TEXT	VARCHAR,
SISORIGEN_ID	VARCHAR,
MANDANTE	VARCHAR,
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR
);

create or replace TABLE CON.DIM_DOC_MOTIVORECHAZO (
MOTIVORECHAZO_ID	VARCHAR PRIMARY KEY,
MOTIVORECHAZO_TEXT	VARCHAR,
SISORIGEN_ID	VARCHAR,
MANDANTE	VARCHAR,
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR
);