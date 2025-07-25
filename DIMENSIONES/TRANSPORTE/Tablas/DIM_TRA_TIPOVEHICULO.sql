create or replace TABLE PRE.PDIM_TRA_TIPOVEHICULO (
TIPOVEHICULO_ID	VARCHAR(18) PRIMARY KEY,
TIPOVEHICULO_TEXT	VARCHAR(60),
SISORIGEN_ID	VARCHAR(3),
MANDANTE	VARCHAR(4),
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR(12)
);

create or replace TABLE CON.DIM_TRA_TIPOVEHICULO (
TIPOVEHICULO_ID	VARCHAR(18) PRIMARY KEY,
TIPOVEHICULO_TEXT	VARCHAR(60),
SISORIGEN_ID	VARCHAR(3),
MANDANTE	VARCHAR(4),
FECHA_CARGA	TIMESTAMP_TZ,
ZONA_HORARIA	VARCHAR(12)
);
SHOW COLUMNS ON CON.DIM_TRA_TIPOVEHICULO;

INSERT INTO CON.DIM_TRA_TIPOVEHICULO
VALUES 
('1','CAJA 48','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('2','CAJA 53','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('3','CAMIONETA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('4','CONTENEDOR 20','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('5','FULL JAULAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('6','FULL PLANAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('7','JAULA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('8','PLATAFORMA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('9','RABON','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('10','TORTON','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('11','CONTENEDOR 53','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('12','PLATAFORMA HD','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('13','FULL HD','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('14','PLATAFORMA HD 12','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('15','PLATAFORMA HD 25','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('16','FULL PLANAS CONTRATO','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR01','AUTO','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR02','CAMIONETA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR03','FURGON','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR04','CHASIS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR05','CHASIS Y ACOPLADO','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR06','SEMI','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR07','SEMI HOMOLOGADO','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR08','RETIRA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR11','SEMI 28','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('AR99','FACTURADO SIN DESPACHAR','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL01','C-3,5','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL02','C-5','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL03','C-10 2 EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL04','C-15 3 EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL05','C-15 PLUMA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL06','C-18 4 EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL07','C-20 4 EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL08','C-30 SEMI 2+1','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL09','C-30 SEMI 2','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CL10','C-30 SEMI 3','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CLCR','CLIENTE RETIRA','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO00','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO01','TURBO','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO02','SENCILLO 8T','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO03','SENCILLO 10T','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO04','DOBLETROQUE','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO05','CUATROMANOS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO06','TRACTOMULA DOS EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO07','TRACTOMULA TRES EJES','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('CO08','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE02','VEHICULO 1.8 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE05','VEHICULO 5 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE08','VEHICULO 8 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE10','VEHICULO 10 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE15','VEHICULO 15 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE17','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE20','VEHICULO 20 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE25','VEHICULO 25 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE27','VEHICULO 27 TONELADAS (EXP MARITIMA)','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE30','VEHICULO 30 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE32','VEHICULO 32 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE33','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE34','VEHICULO 34 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('PE36','VEHICULO 36 TONELADAS','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZASIALINE','ASIALINE PERU S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZC&M','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZCAP LOGIS','CAP LOGISTIC ADUANAS S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZCMACGM','CMA CGM PERU S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZCRAFT MUL','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZDHL','DHL GLOBAL FORWARDING PERU S.A.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZDPWL','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZFIR LOGIS','FIR LOGISTICS S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZGAVA','GAVA PERU S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZGEODIS','GEODIS PERU S.A.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZHAPAG','Hapag LLoyd','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZHMBRGS','HAMBURG SUD PERU','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZIFS LOGIS','IFS LOGISTIC SERVICE DEL PERU SAC','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZKUEHNE','KUEHNE + NAGEL S.A.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZKX SUP','KX SUPPORT LINE S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZLAT CARGO','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZMEDSHP','MEDITERRANIUN SHIPPING COMPANY','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZONETEX','OCEAN NETWORK EXPRESS (PERU) S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZPANALPINA','PANALPINA TRANSPORTES MUNDIALES S A','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZPERU CONT','PERU CONTAINER LINE E.I.R.L.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZPS GROUP','PACIFIC SHIPPING GROUP S.A.C','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZSAVINO','SAVINO DEL BENE DEL PERU S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZSAXIMAN','','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZSEA CARGO','SEA CARGO LOGISTICS PERU S.A.C','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZTCI CARGO','TCI CARGO GROUP S.A.C.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)),
('ZULOG','TRANSTOTAL AGENCIA MARITIMA S.A.','SQ1','500',CURRENT_TIMESTAMP,RIGHT(CURRENT_TIMESTAMP,5)) 