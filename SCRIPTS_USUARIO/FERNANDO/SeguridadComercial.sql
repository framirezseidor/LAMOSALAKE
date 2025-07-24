select * from mirroring.seg_usuario_dimension;

select * from mirroring.seg_role_dimension
where role_nm = 'ADH_COMERCIAL11';

select * from mirroring.seg_usuario_role;

INSERT INTO mirroring.seg_usuario_role (USER_MAIL, ROLE_NM)
VALUES ('test.pbi@grupolamosa.com', 'ADH_COMERCIAL11');

delete from mirroring.seg_role_dimension
where role_nm = 'ADH_COMERCIAL' and dimension_nm = 'ORGVENTAS_ID' and valor = 'A102';

DELETE FROM mirroring.seg_usuario_role
WHERE LOWER(USER_MAIL) = 'test.pbi@grupolamosa.com';


TRUNCATE TABLE MIRRORING.SEG_USUARIO_DIMENSION;

INSERT INTO mirroring.seg_role_dimension (ROLE_NM, DIMENSION_NM, VALOR)
VALUES
    ('ADH_COMERCIAL', 'COORDINADORDEST_ID', '1300063');


-------asesor pedido
select distinct * from mirroring.dim_cli_asesorpedido limit 10;
-- 1302165	ZV	JUAN PEDRO HERNÁNDEZ VÁZQUEZ	1302165 - JUAN PEDRO HERNÁNDEZ VÁZQUEZ
-- 1300563	ZV	MAXIMO JAVIER CASTILLO DE LA ROSA	1300563 - MAXIMO JAVIER CASTILLO DE LA ROSA
-- 1303252	ZV	JONATHAN ALEJANDRO BORBOA OLMEDO	1303252 - JONATHAN ALEJANDRO BORBOA OLMEDO

-------canal distribucion
select distinct * from mirroring.dim_org_canaldistribucion limit 10;
-- EX	Exportación	EX - Exportación
-- GE	Griferia Exportacion	GE - Griferia Exportacion
-- GN	Griferia Nacional	GN - Griferia Nacional
-- IN	Interno	IN - Interno
-- NA	Nacional	NA - Nacional

select distinct * from mirroring.dim_org_centro order by centro_id limit 100 ;
-- A111	A101	MX	MX_NL
-- A112	A101	MX	MX_HGO
-- A113	A101	MX	MX_JAL
-- A114	A101	MX	MX_MCH
-- A115	A101	MX	MX_CHI

select distinct * from mirroring.dim_cli_destinatario
WHERE COORDINADORDEST_ID = '1300063';
-- coordiandor / asesor
-- R401_EX_00_10210018	10210018	R401	EX	00	1300063	1302180	VIRGINIA TILE - KANSAS
-- R401_EX_00_10210277	10210277	R401	EX	00	1300063	1302180	VIRGINIA TILE-SAMPLE WARE
-- R401_EX_00_10210300	10210300	R401	EX	00	1300063	1302180	VIRGINIA TILE- INDIANAPOL
-- R401_EX_00_10210326	10210326	R401	EX	00	1300063	1302180	FXI- CHILE
-- R312_EX_00_10100156	10100156	R312	EX	00	1300476	1300476	SILVA INTERNACIONAL S.A.
-- R313_EX_00_10100156	10100156	R313	EX	00	1300476	1300476	SILVA INTERNACIONAL S.A.
-- R311_EX_00_10100157	10100157	R311	EX	00	1300476	1300476	DECOMAR S.A.
-- R311_EX_00_10100159	10100159	R311	EX	00	1300476	1300476	RAMSTACK INT CORPORATION

select distinct * from mirroring.dim_org_grupovendedores limit 10;
-- 001	Distribuidores	001 - Distribuidores
-- 002	Home Center	002 - Home Center
-- 003	Corp./Inst.	003 - Corp./Inst.
-- 005	Ventas Directas	005 - Ventas Directas
-- 006	Cadenas	006 - Cadenas
-- 007	Big Box	007 - Big Box
-- 008	Cadenas ferreteras	008 - Cadenas ferreteras
-- 009	Mayoristas	009 - Mayoristas
select distinct * from mirroring.dim_org_oficinaventas limit 10;
-- 170	Oficina ventas 170	170 - Oficina ventas 170
-- 550	Oficina ventas 550	550 - Oficina ventas 550
-- ACA1	CanCun Crest	ACA1 - CanCun Crest
-- ACH1	Chihuahua Crest	ACH1 - Chihuahua Crest
-- ACH2	Chihuahua Niasa	ACH2 - Chihuahua Niasa
-- ACH3	Cd Juárez Niasa	ACH3 - Cd Juárez Niasa
-- ACH4	Tijuana Niasa	ACH4 - Tijuana Niasa
-- AEX1	Exportacion Crest	AEX1 - Exportacion Crest
-- AGD2	GDL Crest	AGD2 - GDL Crest
-- AGD3	GDL Niasa	AGD3 - GDL Niasa
select distinct * from mirroring.dim_org_organizacionventas limit 10;
-- R201	R201	AR	R000	CSL Ind y Com (ARG)	R201 - CSL Ind y Com (ARG)
-- A202	A202	CL	A000	Solutek Chile SpA	A202 - Solutek Chile SpA
-- R403	R403	CL	R000	Ceramicas Cordillera	R403 - Ceramicas Cordillera
-- R202	R202	CO	R000	CSL Industrial (CO)	R202 - CSL Industrial (CO)
-- R204	R204	CO	R000	Euroceramica S.A.S	R204 - Euroceramica S.A.S
-- R404	R404	CO	R000	CSL Colombia S.A.S.	R404 - CSL Colombia S.A.S.
-- A201	A201	GT	A000	Tecnocreto	A201 - Tecnocreto
-- 5510	5510	MX		Org.ventas nac.MX	5510 - Org.ventas nac.MX
-- A101	A101	MX	A000	Crest Norteamérica	A101 - Crest Norteamérica
-- A102	A102	MX	A000	Adhesivos Perdura	A102 - Adhesivos Perdura

select distinct * from mirroring.dim_cli_solicitante 
where grupoclientes2_id = 'ZLA'
and solicitante = '10100017';
-- GRUPOCLIENTES2_ID = ZPO
-- GRUPOCLIENTES2_ID = ZLA
-- GRUPOCLIENTES2_ID = ZFI
-- GRUPOCLIENTES2_ID = ZPO
-- GRUPOCLIENTES2_ID = ZLA
-- GRUPOCLIENTES2_ID = ZLA
-- GRUPOCLIENTES2_ID = ZPO
-- GRUPOCLIENTES2_ID = ZLA
-- GRUPOCLIENTES2_ID = ZFI
-- GRUPOCLIENTES2_ID = ZAD

select distinct * from mirroring.dim_org_uenadhesivos limit 10;
-- ALE1	Perdura UEN León	ALE1 - Perdura UEN León	File
-- AMR1	Perdura UEN Mérida	AMR1 - Perdura UEN Mérida	File
-- AMX1	Perdura UEN México	AMX1 - Perdura UEN México	File
-- AGD1	Perdura UEN Guadalajara	AGD1 - Perdura UEN Guadalajara	File
-- ACA1	Crest UEC Sureste	ACA1 - Crest UEC Sureste	File
-- ACH1	Crest UEN Chihuahua	ACH1 - Crest UEN Chihuahua	File
-- ACH4	Niasa UEN Tijuana	ACH4 - Niasa UEN Tijuana	File
-- AGD2	Crest UEN Guadalajara	AGD2 - Crest UEN Guadalajara	File
-- AGD3	Niasa UEN Guadalajara	AGD3 - Niasa UEN Guadalajara	File
-- ACH2	Niasa UEN Chihuahua	ACH2 - Niasa UEN Chihuahua	File

select DISTINCT ASESORFACTURA_ID, COUNT(*) FROM CON.FCT_COM_ADH_VENTAS_HIST
GROUP BY ASESORFACTURA_ID
ORDER BY COUNT(*) DESC;

SELECT * FROM MIRRORING.DIM_CLI_ASESORFACTURA
WHERE ASESORFACTURA_ID = '10400885';

SELECT * FROM MIRRORING.DIM_GEO_REGION
WHERE REGION_ID LIKE 'MX%';


select min(fecha_comercial), max(fecha_comercial) from mirroring.fct_com_adh_comercial
where modelo = 'PEDIDOS';


select min(fecha_comercial), max(fecha_comercial) from mirroring.fct_com_adh_comercial
where modelo = 'BACKORDER';

select * from mirroring.fct_com_adh_comercial
where modelo = 'BACKORDER';