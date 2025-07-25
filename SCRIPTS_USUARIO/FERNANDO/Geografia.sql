create or replace TABLE CON.DIM_GEO_PAIS (
	PAIS_ID VARCHAR(3) NOT NULL,
	PAIS_TEXT VARCHAR(15) NOT NULL,
	SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,
	constraint PK_PDIM_GEO_PAIS primary key (PAIS_ID)
);

create or replace TABLE CON.DIM_GEO_REGION (
	REGION_ID VARCHAR(6) NOT NULL,
	PAIS_ID VARCHAR(3) NOT NULL,
	REGION VARCHAR(3) NOT NULL,
	REGION_TEXT VARCHAR(20) NOT NULL,
	REGION_NEGADH_ID VARCHAR(6) NOT NULL,
	SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,
	constraint PK_DIM_GEO_REGION primary key (REGION_ID)
);

create or replace TABLE CON.DIM_GEO_ZONATRANSPORTE (
	ZONATRANSPORTE_ID VARCHAR(20) NOT NULL,
	PAIS_ID VARCHAR(3) NOT NULL,
	ZONATRANSPORTE VARCHAR(20) NOT NULL,
	ZONATRANSPORTE_TEXT VARCHAR(20) NOT NULL,
	SISORIGEN_ID VARCHAR(4) NOT NULL,
	MANDANTE VARCHAR(4) NOT NULL,
	FECHA_CARGA TIMESTAMP_TZ(9) NOT NULL,
	ZONA_HORARIA VARCHAR(10) NOT NULL,
	constraint PK_DIM_GEO_ZONATRANSPORTE primary key (ZONATRANSPORTE_ID)
);


CREATE OR REPLACE MIRRORING.DIM_GEO_GEOGRAFIA AS

-- Ruta 1: País > Región
SELECT
    r.REGION_ID AS REGION_ID,
    r.REGION_TEXT AS REGION_TEXT,
    'REGIÓN' AS TIPO_NIVEL_GEOGRAFICO,
    p.PAIS_ID AS PAIS_ID,
    p.PAIS_TEXT AS PAIS_NOMBRE,
    r.SISORIGEN_ID AS SISTEMA_ORIGEN,
    r.MANDANTE AS MANDANTE,
    r.FECHA_CARGA AS FECHA_CARGA,
    r.ZONA_HORARIA AS ZONA_HORARIA
FROM CON.DIM_GEO_REGION r
JOIN CON.DIM_GEO_PAIS p ON r.PAIS_ID = p.PAIS_ID

UNION ALL

-- Ruta 2: País > Zona de Transporte
SELECT
    z.ZONATRANSPORTE_ID AS ZONATRANSPORTE_ID,
    z.ZONATRANSPORTE_TEXT AS ZONATRANSPORTE_TEXT,
    'ZONA DE TRANSPORTE' AS TIPO_NIVEL_GEOGRAFICO,
    p.PAIS_ID AS PAIS_ID,
    p.PAIS_TEXT AS PAIS_NOMBRE,
    z.SISORIGEN_ID AS SISTEMA_ORIGEN,
    z.MANDANTE AS MANDANTE,
    z.FECHA_CARGA AS FECHA_CARGA,
    z.ZONA_HORARIA AS ZONA_HORARIA
FROM CON.DIM_GEO_ZONATRANSPORTE z
JOIN CON.DIM_GEO_PAIS p ON z.PAIS_ID = p.PAIS_ID;


select distinct sublineamaterial_adh_text from mirroring.DIM_MAT_MATERIAL_ADH;