create or replace TABLE PRE.PDIM_DOC_INCOTERMS (
INCOTERMS_ID	VARCHAR PRIMARY KEY,
INCOTERMS_TEXT	VARCHAR,
SISORIGEN_ID	VARCHAR,
MANDANTE	VARCHAR,
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR
);

create or replace TABLE CON.DIM_DOC_INCOTERMS (
INCOTERMS_ID	VARCHAR PRIMARY KEY,
INCOTERMS_TEXT	VARCHAR,
SISORIGEN_ID	VARCHAR,
MANDANTE	VARCHAR,
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR
);