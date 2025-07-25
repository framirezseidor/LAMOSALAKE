create or alter TABLE RAW.BW4_RENTABILIDAD_HIST (
	MANDANTE VARCHAR(50),
	SISORIGEN_ID VARCHAR(10),
	FECHA_CARGA VARCHAR(20),
	ZONA_HORARIA VARCHAR(10),
	"ORGN.Sociedad [0COMP_CODE]" VARCHAR(255),
	"Clave [0COMP_CODE].[20COMP_CODE]" VARCHAR(4),
	"GEOG.País [0COUNTRY]" VARCHAR(255),
	"Clave [0COUNTRY].[20COUNTRY]" VARCHAR(3),
	"ORGN.Sociedad CO [0CO_AREA]" VARCHAR(255),
	"Clave [0CO_AREA].[20CO_AREA]" VARCHAR(4),
	"ORGN.Canal Distribución [0DISTR_CHAN]" VARCHAR(255),
	"Clave [0DISTR_CHAN].[20DISTR_CHAN]" VARCHAR(2),
	"ORGN.Sector [0DIVISION]" VARCHAR(255),
	"Clave [0DIVISION].[20DIVISION]" VARCHAR(2),
	"ORGN.Centro (Planta) [0PLANT]" VARCHAR(255),
	"Clave [0PLANT].[20PLANT]" VARCHAR(4),
	"GEOG.Región [0REGION]" VARCHAR(255),
	"Clave [0REGION].[20REGION]" VARCHAR(7),
	"ORGN.Organización Ventas [0SALESORG]" VARCHAR(255),
	"Clave [0SALESORG].[20SALESORG]" VARCHAR(4),
	"ORGN.Oficina Ventas [0SALES_OFF]" VARCHAR(255),
	"Clave [0SALES_OFF].[20SALES_OFF]" VARCHAR(4),
	"CLTE.Constructora [ZIOCONTID]" VARCHAR(255),
	"Clave [ZIOCONTID].[2ZIOCONTID]" VARCHAR(10),
	"UNID.Moneda MXN [ZIOCURMXN]" VARCHAR(255),
	"Clave [ZIOCURMXN].[2ZIOCURMXN]" VARCHAR(5),
	"CLTE.Solicitante [ZIOCUSSLS]" VARCHAR(255),
	"Clave [ZIOCUSSLS].[2ZIOCUSSLS]" VARCHAR(21),
	"CLTE.Destinatario Mercancía [ZIODESTM]" VARCHAR(255),
	"Clave [ZIODESTM].[2ZIODESTM]" VARCHAR(21),
	"MATL. Material [ZIOMATER]" VARCHAR(255),
	"Clave [ZIOMATER].[2ZIOMATER]" VARCHAR(18),
	"CLTE.Tipo Obra [ZIOTOBRA]" VARCHAR(255),
	"Clave [ZIOTOBRA].[2ZIOTOBRA]" VARCHAR(2),
	"UNID.Unidad Medida [ZIOUNME]" VARCHAR(255),
	"Clave [ZIOUNME].[2ZIOUNME]" VARCHAR(3),
	"GEOG. Zona Transporte [ZIOZNTRA]" VARCHAR(255),
	"Clave [ZIOZNTRA].[2ZIOZNTRA]" VARCHAR(14),
	"TMPO.Año/Mes natural [0CALMONTH]" VARCHAR(255),
	"Clave [0CALMONTH].[20CALMONTH]" VARCHAR(7),
	"TMPO.Mes natural [0CALMONTH2]" VARCHAR(255),
	"Clave [0CALMONTH2].[20CALMONTH2]" VARCHAR(2),
	"TMPO.Año natural [0CALYEAR]" VARCHAR(255),
	"Clave [0CALYEAR].[20CALYEAR]" VARCHAR(4),
	"Venta Precio Lista ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZA38V]" NUMBER(18,2),
	"Venta Real ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZB567]" NUMBER(18,2),
	"Venta Constructora ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZBBHR]" NUMBER(18,2),
	"TA.1.a  MOD
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZBUGF]" NUMBER(18,2),
	"TA.1.b  MOI
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZC0RZ]" NUMBER(18,2),
	"TA.1.c  Electricidad [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZG8HB]" NUMBER(18,2),
	"TA.1.d  Depreciación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGESV]" NUMBER(18,2),
	"TA.1.e  Mantenimiento (Conserv.)
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGL4F]" NUMBER(18,2),
	"TA.1.g  Otros
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZGXRJ]" NUMBER(18,2),
	"TA.1.h  Empaque
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZH433]" NUMBER(18,2),
	"TA.1.i  Cementos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHAEN]" NUMBER(18,2),
	"TA.1.j  Químicos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHGQ7]" NUMBER(18,2),
	"TA.1.k  PT Comprado [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHN1R]" NUMBER(18,2),
	"TA.1.l  PT Maquila
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHTDB]" NUMBER(18,2),
	"TA.1.m Subcontratación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZHZOV]" NUMBER(18,2),
	"TA.1.n Carbonatos [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZI60F]" NUMBER(18,2),
	"TA.2.a  Comisiones [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZICBZ]" NUMBER(18,2),
	"TA.2.b  Maniobras
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIINJ]" NUMBER(18,2),
	"TA.2.c  Fletes [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIOZ3]" NUMBER(18,2),
	"TC.a Administración [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZIVAN]" NUMBER(18,2),
	"TC.b Dirección Negocios [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJ1M7]" NUMBER(18,2),
	"TC.c Corporativo [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJ7XR]" NUMBER(18,2),
	"TC.d Ventas [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJE9B]" NUMBER(18,2),
	"TC.e Logística [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJKKV]" NUMBER(18,2),
	"TC.f Proyectos [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJQWF]" NUMBER(18,2),
	"TC.g Marketing
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZJX7Z]" NUMBER(18,2),
	"Venta Real TON [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZSP9R]" NUMBER(19,3),
	"ST.a MOD
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZU3U7]" NUMBER(18,2),
	"ST.b MOI [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUA5R]" NUMBER(18,2),
	"ST.c Electricidad [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUGHB]" NUMBER(18,2),
	"ST.d Depreciación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUMSV]" NUMBER(18,2),
	"ST.e Mantenimiento (Conserv.) [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUT4F]" NUMBER(18,2),
	"ST.f Otros [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZUZFZ]" NUMBER(18,2),
	"ST.g Empaque
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZV5RJ]" NUMBER(18,2),
	"ST.h Cementos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVC33]" NUMBER(18,2),
	"ST.i Químicos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVIEN]" NUMBER(18,2),
	"ST.j PT Maquila
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVOQ7]" NUMBER(18,2),
	"ST.k Subcontratación [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZVV1R]" NUMBER(18,2),
	"ST.l Carbonatos
 [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZW1DB]" NUMBER(18,2),
	"Importe Descuentos ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZYUI7]" FLOAT,
	"Importe Promociones ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZZ0TR]" FLOAT,
	"Importe Rappeles ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZZ75B]" FLOAT,
	"Importe Correctivas ML [CDZB3U1AVKXW8WNBXAFFZ9DYN].[CDZB3U1AVKXW8WNBXAFFZZDGV]" FLOAT
);