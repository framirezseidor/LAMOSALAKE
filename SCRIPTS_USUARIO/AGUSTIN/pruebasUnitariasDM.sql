---------- DIM_DOC_BLOQUEOENTREGA_POS ----------

-- Verificar la carga de datos en la capa RAW mediante Alteryx.
-- SELECT 'RAW', count(*) FROM RAW.SQ1_EXT_0ROUTE_TEXT
-- UNION ALL
SELECT  'PRE', count(*) FROM PRE.PDIM_TRA_TIPOVEHICULO
UNION ALL
SELECT 'CON', count(*) FROM CON.DIM_TRA_TIPOVEHICULO;



SELECT * FROM RAW.SQ1_EXT_0ROUTE_TEXT ORDER BY ROUTE ASC;

SELECT * FROM PRE.PDIM_TRA_TIPOVEHICULO ORDER BY 1 ASC;

SELECT * FROM CON.VW_DIM_TRA_TIPOVEHICULO ORDER BY 1 ASC;


-- Confirmar la ejecución de las Stored Procedures (SPs) para transformar los datos de RAW a PRE y de PRE a CON.
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES
WHERE TABLA_DESTINO LIKE '%TIPOVEHICULO%'
AND TO_DATE(FECHA_HORA_FINAL) = CURRENT_DATE
ORDER BY FECHA_HORA_FINAL DESC;

-- Validar los datos en Snowflake comparándolos con los datos originales de SAP para asegurar la integridad.
SELECT * FROM CON.VW_DIM_DOC_BLOQUEOENTREGA_POS;

-- Verificar que la vista creada en Snowflake (si aplica) refleja correctamente las transformaciones y cumple con las reglas de negocio establecidas.
SELECT * FROM CON.VW_DIM_DOC_BLOQUEOENTREGA_POS ORDER BY CLASEMOVIMIENTO_ID ASC;

-- Validar las claves primarias y foráneas en la dimensión. (Para vistas que tengan JOINS)

DESC TABLE CON.DIM_DOC_BLOQUEOENTREGA_POS;
-- Verificar que no existan valores nulos o duplicados en campos clave.

SELECT CLASEMOVIMIENTO_ID, count(*) as COUNTER
FROM CON.DIM_DOC_CLASEORDFAB
GROUP BY CLASEMOVIMIENTO_ID
ORDER BY COUNTER DESC; // Duplicados
SELECT * FROM CON.DIM_DOC_CLASEORDFAB
WHERE CLASEMOVIMIENTO_ID IS NULL; // Nulls

-- Validar que los tipos de datos y formatos de las columnas son consistentes con el modelo de datos maestro.

SHOW COLUMNS ON RAW.SQ1_EXT_0COORD_TYPE_TEXT;

-- Confirmar que las transformaciones de campos entre capas cumplen con las reglas de negocio establecidas.
-- Verificar que las nomenclaturas de tablas y columnas siguen los estándares establecidos.
SELECT * FROM PRE.PDIM_DOC_BLOQUEOENTREGA_POS ORDER BY 1 ASC;

-- Validar la estructura de la macro en Alteryx para asegurar que sigue las mejores prácticas y es eficiente.





CO_PRODPRF
WERKS
PRODPRF_TX
SISORIGEN_ID
MANDANTE
FECHA_CARGA
ZONA_HORARIA
MANDT
SPRAS