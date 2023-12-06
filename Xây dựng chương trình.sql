-- Tạo Database --
CREATE DATABASE dulieutu_TTTuyenDung;

-- Tạo bảng Stg_ThongTinBackupver9
CREATE TABLE Stg_ThongTinBackupver9 (
ID VARCHAR(15) NOT NULL,
Web VARCHAR(15),
Nganh VARCHAR(20),
Link VARCHAR(15),
TenCV VARCHAR(50),
CongTy VARCHAR(50),
DiaDiem VARCHAR(20),
Luong VARCHAR(20),
LoaiHinh VARCHAR(50),
KinhNghiem VARCHAR(50),
CapBac VARCHAR(50),
HanNopCV DATE,
YeuCau TEXT,
MoTa TEXT,
PhucLoi TEXT,
SoLuong INT(11),
PRIMARY KEY (ID)
);

-- Đổ dữ liệu vào Database
LOAD DATA INFILE 'D:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dulieutuyendung.csv'
INTO TABLE dulieutu_TTTuyenDung.Stg_ThongTinBackupver9 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

-- Thiết lập cấu hình
SET GLOBAL max_allowed_packet=10048576;
SET GLOBAL wait_timeout=3600;
SET GLOBAL net_read_timeout=3600;
SET GLOBAL net_write_timeout=3600;

SHOW VARIABLES LIKE 'secure_file_priv';

-- XÂY DỰNG VÙNG ĐỆM --

-- tạo bảng Stg_LoaiHinhMap
CREATE TABLE Stg_LoaiHinhMap (
ID INT(11) AUTO_INCREMENT,
Nganh TEXT,
Web TEXT,
LoaiHinh TEXT,
LoaiHinhMap TEXT,
LoaiHinh_ID INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_KinhNghiemMap
CREATE TABLE Stg_KinhNghiemMap (
ID INT(11) AUTO_INCREMENT,
Nganh TEXT,
Web TEXT,
KinhNghiem TEXT,
KinhNghiemMap TEXT,
KinhNghiem_ID INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_CapBacMap

CREATE TABLE Stg_CapBacMap (
ID INT(11) AUTO_INCREMENT,
Nganh TEXT,
Web TEXT,
CapBac TEXT,
CapBacMap TEXT,
CapBac_ID INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_DiaDiemMap
CREATE TABLE Stg_DiaDiemMap (
ID INT(11) AUTO_INCREMENT,
TinhThanh TEXT,
TinhThanhMap TEXT,
KhuVuc TEXT,
TinhThanh_ID INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_LuongMap
CREATE TABLE Stg_LuongMap (
Luong VARCHAR(20),
LuongMap VARCHAR(20),
IDLuong INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_TrungLap
CREATE TABLE Stg_TrungLap (
ID VARCHAR(20),
TenCV VARCHAR(50),
CongTy VARCHAR(50),
DiaDiem VARCHAR(20),
TrangThaiTrungLap VARCHAR(20),
ID_TrungLap INT(11),
PRIMARY KEY (ID)
);

-- tạo bảng Stg_Hastag
CREATE TABLE Stg_Hastag (
HastagName VARCHAR(50),
PRIMARY KEY (HastagName)
);

-- tạo bảng Stg_HastagMappingYeuCau
CREATE TABLE Stg_HastagMappingYeuCau (
HastagName VARCHAR(50),
NhomYeuCau VARCHAR(50),
ChiTietNhomYeuCau VARCHAR(100), 
NhomPhanLoaiYeuCau VARCHAR(100)
);

-- XÂY DỰNG KHO DỮ LIỆU --

-- Tạo các bảng Dim

CREATE TABLE Dim_LoaiHinh (
LoaiHinh_ID INT(11),
LoaiHinh VARCHAR(50),
PRIMARY KEY (LoaiHinh_ID)
);

CREATE TABLE Dim_CapBac (
CapBac_ID INT(11),
CapBac VARCHAR(20),
PRIMARY KEY (CapBac_ID)
);

CREATE TABLE Dim_KinhNghiem (
KinhNghiem_ID INT(11),
KinhNghiem VARCHAR(20),
PRIMARY KEY (KinhNghiem_ID)
);

CREATE TABLE Dim_TinhThanh (
TinhThanh_ID INT(11), 
TinhThanh VARCHAR(20), 
KhuVuc VARCHAR(5),
PRIMARY KEY (TinhThanh_ID)
);

CREATE TABLE Nganh (
Congviec_ID INT(11), 
Nganh VARCHAR(50),
NganhCon VARCHAR(100),
CongViec VARCHAR(100),
Nganh_ID INT(11)
);

CREATE TABLE Dim_CongTy (
CongTy_ID INT(11) AUTO_INCREMENT,
CongTy VARCHAR(50),
PRIMARY KEY (CongTy_ID)
);

CREATE TABLE Dim_ThoiGian (
HanNopCV VARCHAR(20),
ThoiGian INT NOT NULL,
Nam INT NOT NULL,
Quy INT NOT NULL ,
Thang INT NOT NULL,
PRIMARY KEY (HanNopCV)
);

CREATE TABLE Dim_KhoangLuong (
KhoangLuong_ID INT(11), 
KhoangLuong VARCHAR(20),
PRIMARY KEY (KhoangLuong_ID)
);

CREATE TABLE Dim_Web (
Web_ID INT(11) AUTO_INCREMENT, 
Web VARCHAR(15),
PRIMARY KEY (HanNopCV)
);

CREATE TABLE Dim_TrungLap (
TrangThai INT(3), 
MoTaTrangThai VARCHAR(11),
PRIMARY KEY (TrangThai)
);

CREATE TABLE Dim_ChiTietNhomYeuCau (
NhomYeuCau VARCHAR(50), 
ChiTietNhomYeuCau VARCHAR(100), 
NhomPhanLoaiYeuCau VARCHAR(100)
);

-- TẠO FACT

CREATE TABLE Fact (
ID varchar(15),
HanNopCV varchar(20),
SoLuong int(11),
ID_Nganh int(11), 
ID_Web int(11),
ID_LoaiHinh int(11), 
ID_CapBac int(11), 
ID_KinhNghiem int(11), 
ID_TinhThanh int(11),
ID_CongViec int(11),
CountTin int(11), 
TrangThaiTrungLap int(3), 
ID_KhoangLuong int(11),
LuongTB decimal(10,1),
ID_CongTy int(11),
PRIMARY KEY (ID)
);

-- Tạo kết nối giữa Dim và Fact

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_LoaiHinh
FOREIGN KEY (ID_LoaiHinh)
REFERENCES Dim_LoaiHinh(LoaiHinh_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_CapBac
FOREIGN KEY (ID_CapBac)
REFERENCES Dim_CapBac(CapBac_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_KinhNghiem
FOREIGN KEY (ID_KinhNghiem)
REFERENCES Dim_KinhNghiem(KinhNghiem_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_TinhThanh
FOREIGN KEY (ID_TinhThanh)
REFERENCES Dim_TinhThanh(TinhThanh_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_Nganh
FOREIGN KEY (ID_Nganh)
REFERENCES Dim_Nganh(Nganh_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_Web
FOREIGN KEY (ID_Web)
REFERENCES Dim_Web(Web_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_CongTy
FOREIGN KEY (ID_CongTy)
REFERENCES Dim_CongTy(CongTy_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_ThoiGian
FOREIGN KEY (HanNopCV)
REFERENCES Dim_ThoiGian(HanNopCV);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_KhoangLuong
FOREIGN KEY (ID_KhoangLuong)
REFERENCES Dim_KhoangLuong(KhoangLuong_ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_TrungLap
FOREIGN KEY (TrangThai)
REFERENCES Dim_TrungLap(TrangThaiTrungLap);

-- Tạo view kết nối giữa Fact với Dim_ChiTietNhomYeuCau

CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `dulieutu`@`%` 
    SQL SECURITY DEFINER
VIEW `v_ThongTinTD_YeuCauNew` AS
    SELECT 
        `sh`.`ID` AS `ID`,
        `sh`.`Nganh` AS `Nganh`,
        `sh`.`ID_CapBac` AS `ID_CapBac`,
        `sh`.`ID_KinhNghiem` AS `ID_KinhNghiem`,
        `sh`.`ID_CongViec` AS `ID_CongViec`,
        `shmyc`.`NhomYeuCau` AS `NhomYeuCau`,
        `shmyc`.`ChiTietNhomYeuCau` AS `ChiTietNhomYeuCau`,
        `shmyc`.`NhomPhanLoaiYeuCau` AS `NhomPhanLoaiYeuCau`
    FROM
        (`Stg_HastagChiTietNew` `sh`
        JOIN `Stg_HastagMappingYeuCau` `shmyc` ON (`sh`.`Hastag` = `shmyc`.`HastagName`));

-- Kết nối Dim_ChiTietNhomYeuCau với Fact thông qua v_ThongTinTD_YeuCauNew
ALTER TABLE FACT
ADD CONSTRAINT FK_v_ThongTinTD_YeuCauNew
FOREIGN KEY (ID)
REFERENCES v_ThongTinTD_YeuCauNew(ID);

ALTER TABLE FACT
ADD CONSTRAINT FK_Dim_ChiTietNhomYeuCau
FOREIGN KEY (ChiTietNhomYeuCau)
REFERENCES v_ThongTinTD_YeuCauNew(ChiTietNhomYeuCau);

-- Thủ tục đổ dữ liệu vào Dim 
CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_InsertData`()
BEGIN
	INSERT INTO Dim_LoaiHinh
	SELECT DISTINCT LoaiHinh_ID, LoaiHinhMap FROM Stg_LoaiHinhMap;

	INSERT INTO Dim_CapBac
	SELECT DISTINCT CapBac_ID, CapBacMap FROM Stg_CapBacMap;

	INSERT INTO Dim_KinhNghiem
	SELECT DISTINCT KinhNghiem_ID, KinhNghiemMap FROM Stg_KinhNghiemMap;

	INSERT INTO Dim_TinhThanh
	SELECT DISTINCT TinhThanh_ID, TinhThanhMap, KhuVuc FROM Stg_DiaDiemMap;
	
    INSERT INTO Dim_Web
	SELECT DISTINCT Web FROM Stg_ThongTinBackupver9;
    
    INSERT INTO Dim_CongTy
	SELECT DISTINCT CongTy FROM Stg_ThongTinBackupver9;
    
    INSERT INTO Dim_KhoangLuong
	SELECT DISTINCT IDLuong, LuongMapFROM Stg_LuongMap;
    
    INSERT INTO Dim_Nganh
	SELECT DISTINCT IDLuong, LuongMapFROM Stg_LuongMap;
    
    INSERT INTO Dim_TrungLap
	SELECT DISTINCT ID_TrungLap, TrangThaiTrungLap FROM Stg_TrungLap;
        
END;

-- Thủ tục thêm các trường ID vào Stg_ThongTinBackupver9

CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_ThemID`()
BEGIN
UPDATE Stg_ThongTinBackupver9 SET ID_Web = 
(SELECT ID_Web FROM Dim_Web WHERE Stg_ThongTinBackupver9.Web = Dim_Web.Web);

UPDATE Stg_ThongTinBackupver9 SET ID_LoaiHinh = 
(SELECT ID_LoaiHinh FROM Dim_LoaiHinh WHERE Stg_ThongTinBackupver9.LoaiHinh = Dim_LoaiHinh.LoaiHinh);

UPDATE Stg_ThongTinBackupver9 SET ID_CapBac = 
(SELECT ID_CapBac FROM Dim_CapBac WHERE Stg_ThongTinBackupver9.CapBac = Dim_CapBac.CapBac);

UPDATE Stg_ThongTinBackupver9 SET ID_KinhNghiem = 
(SELECT ID_KinhNghiem FROM Dim_KinhNghiem WHERE Stg_ThongTinBackupver9.KinhNghiem = Dim_KinhNghiem.KinhNgiem);

UPDATE Stg_ThongTinBackupver9 SET ID_CongTy = 
(SELECT ID_CongTy FROM Dim_CongTy WHERE Stg_ThongTinBackupver9.CongTy = Dim_CongTy.CongTy);

UPDATE Stg_ThongTinBackupver9 SET ID_KhoangLuong = 
(SELECT ID_KhoangLuong FROM Dim_KhoangLuong WHERE Stg_ThongTinBackupver9.KhoangLuong = Dim_KhoangLuong.KhoangLuong);

UPDATE Stg_ThongTinBackupver9 SET ID_TinhThanh = 
(SELECT ID_TinhThanh FROM Dim_TinhThanh WHERE Stg_ThongTinBackupver9.TinhThanh = Dim_TinhThanh.TinhThanh);

UPDATE Stg_ThongTinBackupver9 SET ID_CongViec = 
(SELECT ID_CongViec FROM Dim_Nganh WHERE Stg_ThongTinBackupver9.CongViec = Dim_Nganh.CongViec);

UPDATE Stg_ThongTinBackupver9 SET ID_TrungLap = 
(SELECT ID_TrungLap FROM Dim_TrungLap WHERE Stg_ThongTinBackupver9.TrangThaiTrungLap = Dim_TrungLap.TrangThaiTrungLap);

END;

