CREATE OR REPLACE TABLE RAW.FILE_CSV_SUBDIVISION(
    SUBDIVISION_ID VARCHAR(10),
    LENGUAJE VARCHAR(2),
    SUBDIVISION_TEXT VARCHAR(40)
);


INSERT INTO RAW.FILE_CSV_SUBDIVISION (
    SUBDIVISION_ID,
    LENGUAJE,
    SUBDIVISION_TEXT
) VALUES
('001','ES','Arrendamientos'),
('002','ES','Capacitacion'),
('003','ES','Combustibles'),
('004','ES','Comisiones'),
('005','ES','Comunicaciones'),
('006','ES','Depreciacion'),
('007','ES','Desarrollo de Produc'),
('008','ES','Dir. Negocio'),
('009','ES','Donativos'),
('010','ES','Energeticos'),
('011','ES','Gastos de viaje'),
('012','ES','Gastos filiales'),
('013','ES','Gastos no deducibles'),
('014','ES','Honorarios'),
('015','ES','Impuestos'),
('016','ES','Mantenimiento'),
('017','ES','Materiales diversos'),
('018','ES','Nomina'),
('019','ES','Otros'),
('020','ES','Publicidad'),
('021','ES','Reserva de incobrabl'),
('022','ES','Seg. Industrial'),
('023','ES','Seguros y Fianzas'),
('024','ES','Sistemas'),
('025','ES','Staff'),
('026','ES','Costo Variable'),
('027','ES','Deterioro'),
('028','ES','Fletes'),
('029','ES','ArrGastos Filiales y Otrosendamientos');