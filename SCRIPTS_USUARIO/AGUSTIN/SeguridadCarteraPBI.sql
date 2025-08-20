// Comparación de registros extraídos transaccion Mirroring contra Azure Data Lake
SELECT count(*) FROM MIRRORING.VW_FCT_LOG_ADH_CARTERA; // 2133

// Validar suma de indicadores

SELECT
    SUM(IND_BLOQUEADO_TON) as BLOQUEADO,
    SUM(IND_CONTROL_CALIDAD_TON) as CONTROL_CALIDAD,
    SUM(IND_COSTO_INV_MXN) as COSTO_INV_MXN,
    SUM(IND_CARTERA_FISICO_TON) as CARTERA_FISICO,
    SUM(IND_LIBRE_UTILIZACION_TON) as LIBRE_UTILIZACION,
    SUM(IND_LIBRE_VENTA_TON) as LIBRE_VENTA,
    SUM(IND_STOCK_SEGURIDAD_TON) as STOCK_SEGURIDAD,
    SUM(IND_TRANSITO_TON) as TRANSITO
FROM MIRRORING.VW_FCT_LOG_ADH_CARTERA;

---------------- SEGURIDAD PBI
// Roles y Dims 
SELECT * FROM MIRRORING.SEG_ROLE_DIMENSION
WHERE ROLE_NM = 'ADH_CARTERA_TEST';

SELECT * FROM MIRRORING.SEG_USUARIO_ROLE
WHERE USER_MAIL LIKE 'test%';
SELECT * FROM MIRRORING.SEG_USUARIO_DIMENSION;

// Validar Acceso sin Rol
DELETE FROM MIRRORING.SEG_USUARIO_ROLE
WHERE USER_MAIL LIKE 'test.pbi%'; // Eliminamos los roles a usuario

// Validar acceso con Rol y valor ALL

INSERT INTO MIRRORING.SEG_USUARIO_ROLE
VALUES ('test.pbi@grupolamosa.com', 'ADH_CARTERA_TEST'); // Asignamos Rol a usuario

INSERT INTO MIRRORING.SEG_ROLE_DIMENSION
VALUES
    ('ADH_CARTERA_TEST', 'CANALDISTRIBUCION_ID', 'ALL'),
    ('ADH_CARTERA_TEST', 'OFICINAVENTAS_ID', 'ALL'),
    ('ADH_CARTERA_TEST', 'SOCIEDAD_ID', 'ALL'); // Asignamos valor ALL a Rol

// Validar acceso con Rol y un valor por dimension 

DELETE FROM MIRRORING.SEG_ROLE_DIMENSION
WHERE ROLE_NM LIKE 'ADH_CARTERA%'; // Eliminamos permisos anteriores de Rol

INSERT INTO MIRRORING.SEG_ROLE_DIMENSION
VALUES
    ('ADH_CARTERA_TEST', 'CANALDISTRIBUCION_ID', 'NA'),
    ('ADH_CARTERA_TEST', 'OFICINAVENTAS_ID', 'AMX2'),
    ('ADH_CARTERA_TEST', 'SOCIEDAD_ID', 'A101'); // Asignamos valor a Rol

// Validar acceso con Rol y varios valores por dimension
INSERT INTO MIRRORING.SEG_ROLE_DIMENSION
VALUES
    ('ADH_CARTERA_TEST', 'CANALDISTRIBUCION_ID', 'AR'),
    ('ADH_CARTERA_TEST', 'OFICINAVENTAS_ID', 'ATP4'),
    ('ADH_CARTERA_TEST', 'SOCIEDAD_ID', 'A976'); // Asignamos valor a Rol

// Validar acceso con dimensión asignada a usario con valor 'ALL'
INSERT INTO MIRRORING.SEG_USUARIO_DIMENSION
VALUES
    ('test.pbi@grupolamosa.com', 'CANALDISTRIBUCION_ID', 'ALL'),
    ('test.pbi@grupolamosa.com', 'OFICINAVENTAS_ID', 'ALL'),
    ('test.pbi@grupolamosa.com', 'SOCIEDAD_ID', 'ALL'); // Asignamos valor a Rol

// Validar acceso con dimensión asignada a usuario con un valor
DELETE FROM MIRRORING.SEG_USUARIO_DIMENSION;
INSERT INTO MIRRORING.SEG_USUARIO_DIMENSION
VALUES
    ('test.pbi@grupolamosa.com', 'CANALDISTRIBUCION_ID', 'EX'),
    ('test.pbi@grupolamosa.com', 'OFICINAVENTAS_ID', 'AGU1'),
    ('test.pbi@grupolamosa.com', 'SOCIEDAD_ID', 'A201'); // Asignamos valor a Rol
SELECT * FROM MIRRORING.SEG_USUARIO_DIMENSION;

// Validar acceso con dimensión asignada a usuario con varios valores
INSERT INTO MIRRORING.SEG_USUARIO_DIMENSION
VALUES
    ('test.pbi@grupolamosa.com', 'CANALDISTRIBUCION_ID', 'NA'),
    ('test.pbi@grupolamosa.com', 'OFICINAVENTAS_ID', 'AMX2'),
    ('test.pbi@grupolamosa.com', 'SOCIEDAD_ID', 'A101'); // Asignamos valor a Rol
SELECT * FROM MIRRORING.SEG_USUARIO_DIMENSION;

// Validar acceso con Rol SAP_ROLE
DELETE FROM MIRRORING.SEG_USUARIO_DIMENSION; // Eliminamos restricciones de usuario

DELETE FROM MIRRORING.SEG_ROLE_DIMENSION
WHERE ROLE_NM LIKE 'ADH_CARTERA%'; // Eliminamos permisos anteriores de Rol

DELETE FROM MIRRORING.SEG_USUARIO_ROLE
WHERE USER_MAIL LIKE 'test.pbi%'; // Eliminamos los roles a usuario

INSERT INTO MIRRORING.SEG_USUARIO_ROLE
VALUES ('test.pbi@grupolamosa.com', 'SAP_ROLE'); // Asignamos Rol SAP_ROLE a usuario


---------------------