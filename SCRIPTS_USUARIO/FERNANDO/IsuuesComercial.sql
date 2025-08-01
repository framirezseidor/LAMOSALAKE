//si verificamos el extractor de material en el atributo de linea no es igual a el de linea de material en su extractor

select distinct LINEAMATERIAL_ADH_ID	from mirroring.dim_mat_material_adh;

select * from con.dim_mat_sublineamaterial_adh SL;
select * from con.dim_mat_lineamaterial_adh;

SELECT STATUSMAT_CANDIS_ID FROM CON.DIM_MAT_MATERIALVENTAS;


SELECT * FROM CON.DIM_MAT_FAMILIA_ADH;
SELECT * FROM CON.DIM_MAT_SUBLINEAMATERIAL_ADH;
SELECT * FROM CON.DIM_MAT_LINEAMATERIAL_ADH;


SELECT * FROM CON.DIM_MAT_TIPOMATERIAL TM;
SELECT TIPOMATERIAL_ID,LINEAMATERIAL_ADH_ID FROM CON.DIM_MAT_MATERIAL M;

SELECT
        LTRIM(M.MATERIAL_ID,'0') MATERIAL_ID	,
        M.TIPOMATERIAL_ID	,
        M.LINEAMATERIAL_ADH_ID	,
    FROM
        CON.DIM_MAT_MATERIAL M
        LEFT JOIN CON.DIM_MAT_TIPOMATERIAL TM
        ON M.TIPOMATERIAL_ID = TM.TIPOMATERIAL_ID
        LEFT JOIN CON.DIM_MAT_GRUPOARTICULOS GA
        ON M.GRUPOARTICULOS_ID = GA.GRUPOARTICULOS_ID
        LEFT JOIN CON.DIM_MAT_STATUSMATERIAL SM
        ON M.STATUSMAT_TODOSCENTROS_ID = SM.STATUSMATERIAL_ID
        LEFT JOIN CON.DIM_MAT_COLOR_ADH CL
        ON M.COLOR_ADH_ID = CL.COLOR_ADH_ID
        LEFT JOIN CON.DIM_MAT_COLORGRUPO_ADH CG
        ON M.COLORGRUPO_ADH_ID = CG.COLORGRUPO_ADH_ID
        LEFT JOIN CON.DIM_MAT_LINEAMATERIAL_ADH LM
        ON M.LINEAMATERIAL_ADH_ID = LM.LINEAMATERIAL_ID


;
-----------CLIENTE DESTINATARIO
select * from mirroring.dim_cli_destinatario; -- coordinador y asesor comercial poblados
select * from mirroring.dim_cli_ejecutivocis; --ejecutivos corregidos a lifnr con textos correctos
select * from mirroring.dim_cli_coordinadorcomercial; --coordinador corregido a lifnr con textos correctos
select * from mirroring.dim_cli_asesorcomercial; --asesores corregidos a lifnr con textos correctos
select * from mirroring.dim_cli_tiendarecibo; --tiendas de recibo corregidas a lifnr con textos correctos
select * from mirroring.DIM_ORG_UENADHESIVOS; --espacios en los concatenados corregidos  
select * from mirroring.DIM_TRA_TRANSPORTISTA; --llena de datos de transportistas
select * from mirroring.DIM_MAT_MATERIALCENTRO_ADH; --no tiene mas ceros 




----- DIMENSIONES MIRROR DE CLIENTE -----
SELECT * FROM MIRRORING.DIM_CLI_COORDINADORCOMERCIAL;
SELECT * FROM MIRRORING.DIM_CLI_ASESORCOMERCIAL;
SELECT * FROM MIRRORING.DIM_CLI_EJECUTIVOCIS;
SELECT * FROM MIRRORING.DIM_CLI_TIENDARECIBO;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOSOLICITANTE; --desde aqui
SELECT * FROM MIRRORING.DIM_CLI_CLASIFICACIONCLIENTE;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOIMPUTCLIENTE;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES1;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES2;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES3;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES4;
SELECT * FROM MIRRORING.DIM_CLI_GRUPOCLIENTES5;
SELECT * FROM MIRRORING.DIM_CLI_RAMO; --desde aqui
SELECT * FROM MIRRORING.DIM_CLI_CLIENTE;
SELECT * FROM MIRRORING.DIM_CLI_DESTINATARIO;
SELECT * FROM MIRRORING.DIM_CLI_SOLICITANTE;

----DIMENSIONES MIRROR DE COMERCIAL
SELECT * FROM MIRRORING.DIM_ORG_UENADHESIVOS;
SELECT * FROM MIRRORING.DIM_ORG_ORGANIZACIONVENTAS;
SELECT * FROM MIRRORING.DIM_ORG_SOCIEDAD;
SELECT * FROM MIRRORING.DIM_ORG_CANALDISTRIBUCION;
SELECT * FROM MIRRORING.DIM_ORG_CENTRO;
SELECT * FROM MIRRORING.DIM_ORG_OFICINAVENTAS;
SELECT * FROM MIRRORING.DIM_ORG_ZONAVENTAS;
SELECT * FROM MIRRORING.DIM_ORG_GRUPOVENDEDORES;
SELECT * FROM MIRRORING.DIM_ORG_SECTOR;
SELECT * FROM MIRRORING.DIM_ORG_ALMACENCENTRO;
SELECT * FROM MIRRORING.DIM_GEO_PAIS;
SELECT * FROM MIRRORING.DIM_GEO_REGION;
SELECT * FROM MIRRORING.DIM_GEO_ZONATRANSPORTE;
SELECT * FROM MIRRORING.DIM_OBR_CONSTRUCTORA;
SELECT * FROM MIRRORING.DIM_OBR_PROMOTOR;
SELECT * FROM MIRRORING.DIM_OBR_SEGMENTO_OBRA;
SELECT * FROM MIRRORING.DIM_OBR_TIPO_OBRA;
SELECT * FROM MIRRORING.DIM_TRA_TRANSPORTISTA;
SELECT * FROM MIRRORING.DIM_TRA_RUTA;
SELECT * FROM MIRRORING.DIM_TRA_PUESTOPLANTRANSP;
SELECT * FROM MIRRORING.DIM_TRA_CLASETRANSPORTE;
SELECT * FROM MIRRORING.DIM_TRA_CLASEEXPEDICION;
SELECT * FROM MIRRORING.DIM_TRA_TIPOTRANSPORTE;
SELECT * FROM MIRRORING.DIM_TRA_TIPOVEHICULO;
SELECT * FROM MIRRORING.DIM_MAT_MATERIAL_ADH;
SELECT * FROM MIRRORING.DIM_MAT_MATERIALCENTRO_ADH;
SELECT * FROM MIRRORING.DIM_MAT_MATERIALVENTAS_ADH;
SELECT * FROM MIRRORING.DIM_CLI_CLIENTE;
SELECT * FROM MIRRORING.DIM_CLI_DESTINATARIO;
SELECT * FROM MIRRORING.DIM_CLI_EJECUTIVOCIS;
SELECT * FROM MIRRORING.DIM_CLI_ASESORFACTURA;
SELECT * FROM MIRRORING.DIM_CLI_ASESORPEDIDO;
SELECT * FROM MIRRORING.DIM_CLI_SOLICITANTE;
SELECT * FROM MIRRORING.DIM_CLI_TIENDARECIBO;
SELECT * FROM MIRRORING.DIM_DOC_TIPOPOSICION;
SELECT * FROM MIRRORING.DIM_DOC_STATUSCREDITO;
SELECT * FROM MIRRORING.DIM_DOC_RANGOANTPED;
SELECT * FROM MIRRORING.DIM_DOC_MOTIVORECHAZO;
SELECT * FROM MIRRORING.DIM_DOC_MOTIVOPEDIDO;
SELECT * FROM MIRRORING.DIM_DOC_INCOTERMS;
SELECT * FROM MIRRORING.DIM_DOC_CONDICIONEXP;
SELECT * FROM MIRRORING.DIM_DOC_CLASEPEDIDO;
SELECT * FROM MIRRORING.DIM_DOC_CLASEFACTURA;
SELECT * FROM MIRRORING.DIM_DOC_BLOQUEOENTREGA;
SELECT * FROM MIRRORING.DIM_DOC_BLOQUEOENTREGA_POS;


SELECT * FROM RAW.BW4_VENTAS_HIST_NEW;


SELECT 
FROM PRE.PFCT_COM_ADH_VENTAS_HIST;
