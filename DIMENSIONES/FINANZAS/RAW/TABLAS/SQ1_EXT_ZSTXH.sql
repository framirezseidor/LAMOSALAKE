CREATE OR REPLACE TABLE RAW.SQ1_EXT_ZSTXH (
	DLTDATE DATE,
	TDOBJECT VARCHAR(10),
	TDNAME VARCHAR(70),
	TDID VARCHAR(4),
	TDSPRAS VARCHAR(1),
	FORMATO VARCHAR(2),
	TEXTO VARCHAR(132),
    SISORIGEN_ID VARCHAR(4),
    MANDANTE VARCHAR(4),
    FECHA_CARGA TIMESTAMP_TZ,
    ZONA_HORARIA VARCHAR(10)
);

SELECT * FROM RAW.SQ1_EXT_ZSTXH;