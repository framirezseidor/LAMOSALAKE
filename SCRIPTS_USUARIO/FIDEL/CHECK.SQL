CALL CON.SP_CON_DIM_ORG_ALMACENCENTRO();
CALL CON.SP_CON_DIM_ORG_CANALDISTRIBUCION();
CALL CON.SP_CON_DIM_ORG_CENTRO();
CALL CON.SP_CON_DIM_ORG_GRUPOVENDEDORES();
CALL CON.SP_CON_DIM_ORG_NEGOCIOGRUPO();
CALL CON.SP_CON_DIM_ORG_OFICINAVENTAS();
CALL CON.SP_CON_DIM_ORG_ORGANIZACIONVENTAS();
CALL CON.SP_CON_DIM_ORG_SECTOR();
CALL CON.SP_CON_DIM_ORG_SOCIEDAD();
CALL CON.SP_CON_DIM_ORG_UENADHESIVOS();
CALL CON.SP_CON_DIM_ORG_ZONAVENTAS();

/*
INSERT INTO MIRRORING.FCT_COM_ADH_COMERCIAL
SELECT * FROM MIRRORING.VW_FCT_COM_ADH_PEDIDOS;
*/
