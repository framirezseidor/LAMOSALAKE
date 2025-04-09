-- Script para la carga de datos de backorder a la tabla de staging y la tabla de producción
CALL LAMOSALAKE_DEV.PRE.SP_PRE_PFCT_COM_BACKORDER();

CALL LAMOSALAKE_DEV.CON.SP_CON_FCT_REV_BACKORDER();

CALL LAMOSALAKE_DEV.CON.SP_CON_FCT_ADH_BACKORDER();

-----------------------------------------------------------------------------
select COUNT(*)from lamosalake_dev.raw.sq1_mof_zbwsd_pedidos_backorder;

SELECT COUNT(*) FROM LAMOSALAKE_DEV.PRE.PFCT_COM_BACKORDER --1714
WHERE ORGVENTAS_ID NOT LIKE 'R%'
AND ORGVENTAS_ID NOT LIKE 'A%'; -- 429

SELECT * FROM LAMOSALAKE_DEV.CON.FCT_COM_REV_BACKORDER_ACT; --1285

SELECT COUNT(*) FROM LAMOSALAKE_DEV.CON.FCT_COM_ADH_BACKORDER_ACT; --429


select * from lamosalake_dev.raw.parametros_extraccion;
