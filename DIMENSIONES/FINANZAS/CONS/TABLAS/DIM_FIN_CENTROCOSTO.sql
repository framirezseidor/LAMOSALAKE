CREATE OR REPLACE TABLE CON.DIM_FIN_CENTROCOSTO(
    CENTROCOSTO_ID VARCHAR(15),
	CENTROCOSTO VARCHAR(10),
	CENTROCOSTO_TEXT VARCHAR(40),
	SOCIEDADCO_ID VARCHAR(4),
	SOCIEDAD_ID VARCHAR(4),
	CENTROBENEF_ID VARCHAR(30),
	DIRECCION_ID VARCHAR(2),
	DIRECCION_TEXT VARCHAR(40),
	GERENCIA_ID VARCHAR(2),
	GERENCIA_TEXT VARCHAR(40),
	SUCURSAL_ID VARCHAR(2),
	SUCURSAL_TEXT VARCHAR(40),
	CCAGRUPADOR_ID VARCHAR(6),
	CCAGRUPADOR_TEXT VARCHAR(40),
	TIPOGASTO_ID VARCHAR(2),
	TIPOGASTO_TEXT VARCHAR(40),
	ENCARGADO_CC_ID VARCHAR(12),
	ENCARGADO_CC_TEXT VARCHAR(80),
	ENCARGADO_GERENCIA_ID VARCHAR(12),
	ENCARGADO_GERENCIA_TEXT VARCHAR(80),
	ENCARGADO_DIRECCION_ID VARCHAR(12),
	ENCARGADO_DIRECCION_TEXT VARCHAR(80),
	SOCIEDADCECOASOC_ID VARCHAR(4),
	CENTROCOSASOC_ID VARCHAR(10),
	CENTROCOSASOC_TEXT VARCHAR(40),
	ENCARGADO_DIRECAREA_ID VARCHAR(12),
	ENCARGADO_DIRECAREA_TEXT VARCHAR(80),
	ZTPGTOBPC VARCHAR(2),
	FECHA_DESDE DATE,
	FECHA_HASTA DATE,
	SISORIGEN_ID VARCHAR(3),
	MANDANTE VARCHAR(50),
	FECHA_CARGA VARCHAR(20),
	ZONA_HORARIA VARCHAR(10),
	TIPO VARCHAR(10)
)