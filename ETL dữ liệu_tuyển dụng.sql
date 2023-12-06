-- Đổ dữ liệu và ETL dữ liệu vùng đệm --

-- Thủ tục ETL dữ liệu dạng text trường Yêu cầu
CREATE DEFINER=`dulieutu`@`%` PROCEDURE `pr_Stg_KDBH`()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE hastag_value VARCHAR(50);
    DECLARE cur CURSOR FOR SELECT HastagName FROM Stg_Hastag;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    -- Create a temporary table to store the matched results
    CREATE TABLE IF NOT EXISTS Stg_HastagChiTiet (
        ID VARCHAR(50),
        ID_CongViec INT(11),
        ID_KinhNghiem INT(11),
        ID_CapBac INT(11),
        Hastag VARCHAR(50)
    );

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO hastag_value;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Insert the matched records into the temporary table
        INSERT INTO Stg_HastagChiTiet (ID, ID_CongViec, ID_KinhNghiem, ID_CapBac, Hastag)
        SELECT ID, ID_CongViec, ID_KinhNghiem, ID_CapBac, hastag_value
        FROM Stg_ThongTinBackupver9
        WHERE lower(YeuCau) LIKE CONCAT('%', lower(hastag_value), '%');
    END LOOP;

    CLOSE cur;

    -- Select the results from the temporary table
    SELECT * FROM Stg_HastagChiTiet;
    
    -- You can perform further operations with the matched records if needed
    
    -- Drop the temporary table
    -- DROP TABLE IF EXISTS Stg_HastagChiTiet;
    
END

-- Thủ tục ETL trường Loại hình

CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_LoaiHinhMap`()
BEGIN
    INSERT INTO Stg_LoaiHinhMap (Nganh, Web, LoaiHinh)
    SELECT DISTINCT Nganh, Web, LoaiHinh
    FROM Stg_ThongTinBackupver9;

    UPDATE Stg_LoaiHinhMap
    SET LoaiHinhMap = 
        CASE LoaiHinh
            WHEN 'Toàn thời gian tạm thời' THEN 'Toàn thời gian'
            WHEN 'Toàn thời gian cố định' THEN 'Toàn thời gian'
            WHEN 'Thực tập' THEN 'Toàn thời gian'
            WHEN 'Thời vụ/ Nghề tự do' THEN 'Bán thời gian'
            WHEN 'Theo hợp đồng tư vấn' THEN 'Toàn thời gian'
            WHEN 'Remote' THEN 'Hình thức khác'
            WHEN 'Part_time' THEN 'Bán thời gian'
            WHEN 'Nhân viên toàn thời gian tạm thời' THEN 'Toàn thời gian'
            WHEN 'Nhân viên toàn thời gian' THEN 'Toàn thời gian'
            WHEN 'Nhân viên hợp đồng' THEN 'Toàn thời gian'
            WHEN 'Nhân viên chính thức, Thời vụ/ Nghề tự do' THEN 'Toàn thời gian'
            WHEN 'Nhân viên chính thức, Bán thời gian' THEN 'Bán thời gian'
            WHEN 'Nhân viên chính thức' THEN 'Toàn thời gian'
            WHEN 'Nhân viên bán thời gian' THEN 'Bán thời gian'
            WHEN 'Khác' THEN 'Hình thức khác'
            WHEN 'In Office' THEN 'Toàn thời gian'
            WHEN 'Hình thức khác' THEN 'Hình thức khác'
            WHEN 'Hybrid' THEN 'Hình thức khác'
            WHEN 'Fulltime' THEN 'Toàn thời gian'
            WHEN 'Bán thời gian tạm thời' THEN 'Bán thời gian'
            WHEN 'Bán thời gian cố định' THEN 'Bán thời gian'
            WHEN 'Bán thời gian' THEN 'Bán thời gian'
            ELSE 'Không xác định'
        END;

    UPDATE Stg_LoaiHinhMap
    SET LoaiHinh_ID =
        CASE LoaiHinhMap
            WHEN 'Toàn thời gian' THEN '1'
            WHEN 'Bán thời gian' THEN '2'
            WHEN 'Hình thức khác' THEN '3'
            ELSE 'Không xác định'
        END;
END;

-- Thủ tục ETL trường Kinh Nghiệm
CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_KinhNghiemMap`()
BEGIN
    INSERT INTO Stg_KinhNghiemMap (Nganh, Web, KinhNghiem)
    SELECT DISTINCT Nganh, Web, KinhNghiem
    FROM Stg_ThongTinBackupver9;

    UPDATE Stg_KinhNghiemMap
    SET KinhNghiemMap = 
        CASE KinhNghiem
            WHEN 'Trên 2 năm' THEN '1-3 năm'
			WHEN '1-2 năm' THEN '1-3 năm'
			WHEN '1-3 năm' THEN '1-3 năm'
			WHEN '3-5 năm' THEN '3-5 năm'
			WHEN '2-3 năm' THEN '1-3 năm'
			WHEN 'Lên đến 1 năm' THEN 'Dưới 1 năm'
			WHEN '2-5 năm' THEN '3-5 năm'
			WHEN 'Trên 1 năm' THEN '1-3 năm'
			WHEN 'Trên 3 năm' THEN '3-5 năm'
			WHEN '4-8 năm' THEN '3-5 năm'
			WHEN '3-7 năm' THEN '3-5 năm'
			WHEN '5-7 năm' THEN 'Trên 5 năm'
			WHEN '1-5 năm' THEN '1-3 năm'
			WHEN '3-10 năm' THEN '3-5 năm'
			WHEN '5-6 năm' THEN 'Trên 5 năm'
			WHEN 'Chưa có kinh nghiệm' THEN 'Không yêu cầu kinh nghiệm'
			WHEN '5-20 năm' THEN 'Trên 5 năm'
			WHEN 'Trên 5 năm' THEN 'Trên 5 năm'
			WHEN '2-8 năm' THEN '3-5 năm'
			WHEN '7-10 năm' THEN 'Trên 5 năm'
			WHEN '1-8 năm' THEN '1-3 năm'
			WHEN '4-7 năm' THEN '3-5 năm'
			WHEN '3-15 năm' THEN '3-5 năm'
			WHEN 'Trên 4 năm' THEN '3-5 năm'
			WHEN '4-10 năm' THEN '3-5 năm'
			WHEN '8-10 năm' THEN 'Trên 5 năm'
			WHEN '1-10 năm' THEN '1-3 năm'
			WHEN '2-10 năm' THEN '3-5 năm'
			WHEN '3-20 năm' THEN '3-5 năm'
			WHEN 'Trên 8 năm' THEN 'Trên 5 năm'
			WHEN 'Trên 7 năm' THEN 'Trên 5 năm'
			WHEN 'Trên 10 năm' THEN 'Trên 5 năm'
			WHEN '0 năm' THEN 'Không yêu cầu kinh nghiệm'
			WHEN 'Trên 6 năm' THEN 'Trên 5 năm'
			WHEN '5-15 năm' THEN 'Trên 5 năm'
			WHEN '2-4 năm' THEN '1-3 năm'
			WHEN '1-4 năm' THEN '1-3 năm'
			WHEN '3-4 năm' THEN '3-5 năm'
			WHEN 'Lên đến 5 năm' THEN '3-5 năm'
			WHEN '10-15 năm' THEN 'Trên 5 năm'
			WHEN '6-10 năm' THEN 'Trên 5 năm'
			WHEN '3-9 năm' THEN '3-5 năm'
			WHEN '2-15 năm' THEN '3-5 năm'
			WHEN 'Lên đến 3 năm' THEN '1-3 năm'
			WHEN '4-5 năm' THEN '3-5 năm'
			WHEN '1-20 năm' THEN '1-3 năm'
			WHEN '2-20 năm' THEN '3-5 năm'
			WHEN '5-8 năm' THEN 'Trên 5 năm'
			WHEN '3-6 năm' THEN '3-5 năm'
			WHEN '2-6 năm' THEN '3-5 năm'
			WHEN '1-15 năm' THEN '1-3 năm'
			WHEN 'Lên đến 2 năm' THEN '1-3 năm'
			WHEN 'Trên 15 năm' THEN 'Trên 5 năm'
			WHEN '4-6 năm' THEN '3-5 năm'
			WHEN '3-8 năm' THEN '3-5 năm'
			WHEN '0 - 1 ' THEN 'Dưới 1 năm'
			WHEN 'Không yêu cầu kinh nghiệm' THEN 'Không yêu cầu kinh nghiệm'
			WHEN 'Dưới 1 năm' THEN 'Dưới 1 năm'
			WHEN '2 năm' THEN '1-3 năm'
			WHEN '1 năm' THEN '1-3 năm'
			WHEN '3 năm' THEN '3-5 năm'
			WHEN '4 năm' THEN '3-5 năm'
			WHEN '5 năm' THEN 'Trên 5 năm'
            ELSE 'Không xác định'
        END;

    UPDATE Stg_KinhNghiemMap
    SET KinhNghiem_ID =
        CASE KinhNghiemMap
            WHEN 'Không yêu cầu kinh nghiệm' THEN '1'
            WHEN 'Dưới 1 năm' THEN '2'
            WHEN '1-3 năm' THEN '3'
			WHEN '3-5 năm' THEN '4'
			WHEN 'Trên 5 năm' THEN '5'
			ELSE 'Không xác định'
        END;
END;

-- ETL trường Cấp bậc
CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_CapBacMap`()
BEGIN
    INSERT INTO Stg_CapBacMap (Nganh, Web, CapBac)
    SELECT DISTINCT Nganh, Web, CapBac
    FROM Stg_ThongTinBackupver9;

    UPDATE Stg_CapBacMap
    SET CapBacMap = 
        CASE CapBac
            WHEN 'Nhân viên' THEN 'Nhân viên/ Chuyên viên'
			WHEN 'Quản lý' THEN 'Quản lý'
			WHEN 'Trưởng nhóm / Giám sát' THEN 'Trưởng phòng/ Phó phòng'
			WHEN 'Giám đốc' THEN 'Giám đốc/ Phó giám đốc'
			WHEN 'Phó Giám đốc' THEN 'Giám đốc/ Phó giám đốc'
			WHEN 'Mới tốt nghiệp' THEN 'Thực tập sinh'
			WHEN 'Sinh viên/ Thực tập sinh' THEN 'Thực tập sinh'
			WHEN 'Tổng giám đốc' THEN 'Giám đốc/ Phó giám đốc'
			WHEN 'Thực tập sinh' THEN 'Thực tập sinh'
			WHEN 'Trưởng nhóm' THEN 'Trưởng phòng/ Phó phòng'
			WHEN 'Quản lý / Giám sát' THEN 'Quản lý'
			WHEN 'Trưởng/Phó phòng' THEN 'Trưởng phòng/ Phó phòng'
			WHEN 'Trưởng chi nhánh' THEN 'Trưởng phòng/ Phó phòng'
			WHEN 'Chuyên viên- nhân viên' THEN 'Nhân viên/ Chuyên viên'
			WHEN 'Quản lý cấp trung' THEN 'Quản lý'
			WHEN 'Quản lý nhóm- giám sát' THEN 'Quản lý'
			WHEN 'Quản lý cấp cao' THEN 'Trưởng phòng/ Phó phòng'
			WHEN 'Cộng tác viên' THEN 'Nhân viên/ Chuyên viên'
            ELSE 'Không xác định'
        END;

    UPDATE Stg_CapBacMap
    SET CapBac_ID =
        CASE CapBacMap
            WHEN 'Nhân viên/ Chuyên viên' THEN '1'
			WHEN 'Quản lý' THEN '2'
			WHEN 'Thực tập sinh' THEN '3'
			WHEN 'Giám đốc/ Phó giám đốc' THEN '4'
			WHEN 'Trưởng phòng/ Phó phòng' THEN '5'
			WHEN 'Trưởng nhóm' THEN '6'
            ELSE 'Không xác định'
        END;
END;

-- ETL trường Tỉnh thành

CREATE DEFINER=`dulieutu`@`%` PROCEDURE `ETL_Stg_DiaDiemMap`()
BEGIN
UPDATE Stg_DiaDiem  
SET     TinhThanh =  CASE  
When lower(DiaDiem) Like '%an giang%'  THEN 'An Giang'
When lower(DiaDiem) Like '%bà r%'  THEN 'Bà Rịa – Vũng Tàu'
When lower(DiaDiem) Like '%vũng tàu%'  THEN 'Bà Rịa – Vũng Tàu'
When lower(DiaDiem) Like '%bắc giang%'  THEN 'Bắc Giang'
When lower(DiaDiem) Like '%bắc kạn%'  THEN 'Bắc Kạn'
When lower(DiaDiem) Like '%bạc liêu%'  THEN 'Bạc Liêu'
When lower(DiaDiem) Like '%bắc ninh%'  THEN 'Bắc Ninh'
When lower(DiaDiem) Like '%bến tre%'  THEN 'Bến Tre'
When lower(DiaDiem) Like '%bình định%'  THEN 'Bình Định'
When lower(DiaDiem) Like '%bình dương%'  THEN 'Bình Dương'
When lower(DiaDiem) Like '%bình phước%'  THEN 'Bình Phước'
When lower(DiaDiem) Like '%bình thuận%'  THEN 'Bình Thuận'
When lower(DiaDiem) Like '%cà mau%'  THEN 'Cà Mau'
When lower(DiaDiem) Like '%cần thơ%'  THEN 'Cần Thơ'
When lower(DiaDiem) Like '%cao bằng%'  THEN 'Cao Bằng'
When lower(DiaDiem) Like '%đà nẵng%'  THEN 'Đà Nẵng'
When lower(DiaDiem) Like '%đắk lắk%'  THEN 'Đắk Lắk'
When lower(DiaDiem) Like '%đắk nông%'  THEN 'Đắk Nông'
When lower(DiaDiem) Like '%điện biên%'  THEN 'Điện Biên'
When lower(DiaDiem) Like '%đồng nai%'  THEN 'Đồng Nai'
When lower(DiaDiem) Like '%đồng tháp%'  THEN 'Đồng Tháp'
When lower(DiaDiem) Like '%gia lai%'  THEN 'Gia Lai'
When lower(DiaDiem) Like '%hà giang%'  THEN 'Hà Giang'
When lower(DiaDiem) Like '%hà nam%'  THEN 'Hà Nam'
When lower(DiaDiem) Like '%hà nội%'  THEN 'Hà Nội'
When lower(DiaDiem) Like '%hn%'  THEN 'Hà Nội'
When lower(DiaDiem) Like '%hà tĩnh%'  THEN 'Hà Tĩnh'
When lower(DiaDiem) Like '%hải dương%'  THEN 'Hải Dương'
When lower(DiaDiem) Like '%hải phòng%'  THEN 'Hải Phòng'
When lower(DiaDiem) Like '%hậu giang%'  THEN 'Hậu Giang'
When lower(DiaDiem) Like '%a bình%'  THEN 'Hoà Bình'
When lower(DiaDiem) Like '%hưng yên%'  THEN 'Hưng Yên'
When lower(DiaDiem) Like '%khánh h%'  THEN 'Khánh Hoà'
When lower(DiaDiem) Like '%kiên giang%'  THEN 'Kiên Giang'
When lower(DiaDiem) Like '%kon tum%'  THEN 'Kon Tum'
When lower(DiaDiem) Like '%lai châu%'  THEN 'Lai Châu'
When lower(DiaDiem) Like '%lâm đồng%'  THEN 'Lâm Đồng'
When lower(DiaDiem) Like '%lạng sơn%'  THEN 'Lạng Sơn'
When lower(DiaDiem) Like '%lào cai%'  THEN 'Lào Cai'
When lower(DiaDiem) Like '%long an%'  THEN 'Long An'
When lower(DiaDiem) Like '%nam định%'  THEN 'Nam Định'
When lower(DiaDiem) Like '%nghệ an%'  THEN 'Nghệ An'
When lower(DiaDiem) Like '%ninh bình%'  THEN 'Ninh Bình'
When lower(DiaDiem) Like '%ninh thuận%'  THEN 'Ninh Thuận'
When lower(DiaDiem) Like '%phú thọ%'  THEN 'Phú Thọ'
When lower(DiaDiem) Like '%phú yên%'  THEN 'Phú Yên'
When lower(DiaDiem) Like '%quảng bình%'  THEN 'Quảng Bình'
When lower(DiaDiem) Like '%quảng nam%'  THEN 'Quảng Nam'
When lower(DiaDiem) Like '%quảng ngãi%'  THEN 'Quảng Ngãi'
When lower(DiaDiem) Like '%quảng ninh%'  THEN 'Quảng Ninh'
When lower(DiaDiem) Like '%quảng trị%'  THEN 'Quảng Trị'
When lower(DiaDiem) Like '%sóc trăng%'  THEN 'Sóc Trăng'
When lower(DiaDiem) Like '%sơn la%'  THEN 'Sơn La'
When lower(DiaDiem) Like '%tây ninh%'  THEN 'Tây Ninh'
When lower(DiaDiem) Like '%thái bình%'  THEN 'Thái Bình'
When lower(DiaDiem) Like '%thái nguyên%'  THEN 'Thái Nguyên'
When lower(DiaDiem) Like '%thanh h%'  THEN 'Thanh Hoá'
When lower(DiaDiem) Like '%thừa t%'  THEN 'Thừa Thiên Huế'
When lower(DiaDiem) Like '%tiền giang%'  THEN 'Tiền Giang'
When lower(DiaDiem) Like '%hồ%'  THEN 'Hồ Chí Minh'
When lower(DiaDiem) Like '%hcm%'  THEN 'Hồ Chí Minh'
When lower(DiaDiem) Like '%trà vinh%'  THEN 'Trà Vinh'
When lower(DiaDiem) Like '%tuyên quang%'  THEN 'Tuyên Quang'
When lower(DiaDiem) Like '%vĩnh long%'  THEN 'Vĩnh Long'
When lower(DiaDiem) Like '%vĩnh phúc%'  THEN 'Vĩnh Phúc'
When lower(DiaDiem) Like '%yên bái%'  THEN 'Yên Bái'
                        ELSE 'Khác'
END ;

UPDATE Stg_DiaDiem  
SET     KhuVuc =  CASE 
	When TinhThanh Like '%An Giang%'  THEN 'Nam'
When TinhThanh Like '%Bà Rịa – Vũng Tàu%'  THEN 'Nam'
When TinhThanh Like '%Bắc Giang%'  THEN 'Bắc'
When TinhThanh Like '%Bắc Kạn%'  THEN 'Bắc'
When TinhThanh Like '%Bạc Liêu%'  THEN 'Nam'
When TinhThanh Like '%Bắc Ninh%'  THEN 'Bắc'
When TinhThanh Like '%Bến Tre%'  THEN 'Nam'
When TinhThanh Like '%Bình Định%'  THEN 'Trung'
When TinhThanh Like '%Bình Dương%'  THEN 'Nam'
When TinhThanh Like '%Bình Phước%'  THEN 'Nam'
When TinhThanh Like '%Bình Thuận%'  THEN 'Trung'
When TinhThanh Like '%Cà Mau%'  THEN 'Nam'
When TinhThanh Like '%Cần Thơ%'  THEN 'Nam'
When TinhThanh Like '%Cao Bằng%'  THEN 'Bắc'
When TinhThanh Like '%Đà Nẵng%'  THEN 'Trung'
When TinhThanh Like '%Đắk Lắk%'  THEN 'Trung'
When TinhThanh Like '%Đắk Nông%'  THEN 'Trung'
When TinhThanh Like '%Điện Biên%'  THEN 'Bắc'
When TinhThanh Like '%Đồng Nai%'  THEN 'Nam'
When TinhThanh Like '%Đồng Tháp%'  THEN 'Nam'
When TinhThanh Like '%Gia Lai%'  THEN 'Trung'
When TinhThanh Like '%Hà Giang%'  THEN 'Bắc'
When TinhThanh Like '%Hà Nam%'  THEN 'Bắc'
When TinhThanh Like '%Hà Nội%'  THEN 'Bắc'
When TinhThanh Like '%Hà Tĩnh%'  THEN 'Trung'
When TinhThanh Like '%Hải Dương%'  THEN 'Bắc'
When TinhThanh Like '%Hải Phòng%'  THEN 'Bắc'
When TinhThanh Like '%Hậu Giang%'  THEN 'Nam'
When TinhThanh Like '%Hoà Bình%'  THEN 'Bắc'
When TinhThanh Like '%Hưng Yên%'  THEN 'Bắc'
When TinhThanh Like '%Khánh Hoà%'  THEN 'Trung'
When TinhThanh Like '%Kiên Giang%'  THEN 'Nam'
When TinhThanh Like '%Kon Tum%'  THEN 'Trung'
When TinhThanh Like '%Lai Châu%'  THEN 'Bắc'
When TinhThanh Like '%Lâm Đồng%'  THEN 'Trung'
When TinhThanh Like '%Lạng Sơn%'  THEN 'Bắc'
When TinhThanh Like '%Lào Cai%'  THEN 'Bắc'
When TinhThanh Like '%Long An%'  THEN 'Nam'
When TinhThanh Like '%Nam Định%'  THEN 'Bắc'
When TinhThanh Like '%Nghệ An%'  THEN 'Trung'
When TinhThanh Like '%Ninh Bình%'  THEN 'Bắc'
When TinhThanh Like '%Ninh Thuận%'  THEN 'Trung'
When TinhThanh Like '%Phú Thọ%'  THEN 'Bắc'
When TinhThanh Like '%Phú Yên%'  THEN 'Trung'
When TinhThanh Like '%Quảng Bình%'  THEN 'Trung'
When TinhThanh Like '%Quảng Nam%'  THEN 'Trung'
When TinhThanh Like '%Quảng Ngãi%'  THEN 'Trung'
When TinhThanh Like '%Quảng Ninh%'  THEN 'Bắc'
When TinhThanh Like '%Quảng Trị%'  THEN 'Trung'
When TinhThanh Like '%Sóc Trăng%'  THEN 'Nam'
When TinhThanh Like '%Sơn La%'  THEN 'Bắc'
When TinhThanh Like '%Tây Ninh%'  THEN 'Nam'
When TinhThanh Like '%Thái Bình%'  THEN 'Bắc'
When TinhThanh Like '%Thái Nguyên%'  THEN 'Bắc'
When TinhThanh Like '%Thanh Hoá%'  THEN 'Trung'
When TinhThanh Like '%Thừa Thiên Huế%'  THEN 'Trung'
When TinhThanh Like '%Tiền Giang%'  THEN 'Nam'
When TinhThanh Like '%Hồ Chí Minh%'  THEN 'Nam'
When TinhThanh Like '%Trà Vinh%'  THEN 'Nam'
When TinhThanh Like '%Tuyên Quang%'  THEN 'Bắc'
When TinhThanh Like '%Vĩnh Long%'  THEN 'Nam'
When TinhThanh Like '%Vĩnh Phúc%'  THEN 'Bắc'
When TinhThanh Like '%Yên Bái%'  THEN 'Bắc'
ELSE 'Khác'

END;


