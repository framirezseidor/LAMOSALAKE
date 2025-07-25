SELECT * FROM RAW.SQ1_MOF_ZBWSD_CUADERNO_FINANCIERO;
SELECT * FROM RAW.PARAMETROS_EXTRACCION WHERE EXTRACTOR = 'ZBWSD_CUADERNO_FINANCIERO';

CALL RAW.SP_RAW_BORRADO_PRERAW('ZBWSD_CUADERNO_FINANCIERO','PRUEBACICLO1');
CALL CON.SP_CON_FCT_COM_REV_VENTAS_ACT( 'ZBWSD_CUADERNO_FINANCIERO', 'PRUEBACICLO1');

SELECT ESTATUS FROM LOGS.HISTORIAL_EJECUCIONES ORDER BY FECHA_HORA_INICIO DESC;

SELECT * FROM CON.FCT_COM_ADH_VENTAS_ACT;

SELECT * FROM MIRRORING.FCT_COM_ADH_COMERCIAL WHERE MODELO= 'VENTAS';

SELECT * FROM MIRRORING.VW_FCT_COM_ADH_VENTAS_ACT;
SELECT * FROM MIRRORING.STREAM_FCT_COM_ADH_PEDIDOS_ACT;


CALL CON.SP_CON_FCT_COM_ADH_VENTAS_PCP();

SELECT * FROM CON.FCT_COM_ADH_VENTAS_PCP;

SELECT * FROM MIRRORING.VW_FCT_COM_ADH_VENTAS_PCP;
SELECT COUNT(0) FROM MIRRORING.FCT_COM_ADH_COMERCIAL;

show tasks;

