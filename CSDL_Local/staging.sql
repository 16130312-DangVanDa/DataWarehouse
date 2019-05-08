create database staging
go
use staging

/*nhom 2_ca1*/
create table nhom2_ca1(	mssv text not null,
						hoten ntext null, 
						ngaysinh text null,
						gioitinh ntext null,
		);
bulk
INSERT nhom2_ca1
FROM 'D:\Data_Warehouse\ca1_nhom02_DuLieu_20190307.csv'
WITH
(FIRSTROW = 2,
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n'
)	
truncate table nhom2_ca1
select * from nhom2_ca1
drop table nhom2_ca1;
select mssv, hoten, ngaysinh, gioitinh from nhom2_ca1

create table nhom4_ca1(	mssv text not null,
						ho ntext null, 
						ten ntext null, 
						ngaysinh text null,
						gioitinh ntext null,
						diachi ntext null,
						dienthoai text null
						);
truncate table nhom4_ca1
drop table nhom4_ca1;
select * from nhom4_ca1
select mssv, cast([ho] as varchar(100)) + ' ' + cast([ten] as varchar(100))as hoten, ngaysinh, gioitinh from nhom4_ca1

create table nhom6_ca1(	mssv text not null,
						ho ntext null, 
						ten ntext null, 
						gioitinh ntext null,
						ngaysinh text null,
						);
truncate table nhom6_ca1
drop table nhom6_ca1;
select * from nhom6_ca1
select mssv, cast([ho] as varchar(100)) + ' ' + cast([ten] as varchar(100))as hoten, ngaysinh, gioitinh from nhom6_ca1

						
create table nhom8_ca2(	stt text null, 
						mssv text not null,
						hoten ntext null, 
						ngaysinh text null,
						diachi ntext null,
						gioitinh text null,
);
truncate table nhom8_ca2
drop table nhom8_ca2;
select * from nhom8_ca2
select mssv, hoten, ngaysinh, gioitinh from nhom8_ca2

create table DataSinhVien(stt text null, 
						mssv text not null,
						hoten ntext null, 
						ngaysinh text null,
						gioitinh ntext null,
						maLop ntext null,
						maNganh ntext null
						
);
truncate table DataSinhVien
drop table DataSinhVien;
select * from DataSinhVien		

create table nhom8_ca1(	mssv text not null,
						ho ntext null, 
						ten ntext null,
						ngaysinh text null, 
						gioitinh ntext null
						
						);					
truncate table nhom8_ca1
drop table nhom8_ca1;
select * from nhom8_ca1
	
create table nhom5_ca1(	mssv text not null,
						hoten ntext null, 
						ngaysinh text null,
						gioitinh ntext null,
						);
truncate table nhom5_ca1
drop table nhom5_ca1;
select * from nhom5_ca1
						
create table nhom2_ca2(	stt text null,mssv text not null,
						ho ntext null,
						ten ntext null, 
						gioitinh ntext null,
						ngaysinh text null
						);	
	
truncate table nhom2_ca2				
select * from nhom2_ca2
drop table nhom2_ca2;

--ca2_ nhom 4-- DATA.csv
create table nhom4_ca2(	mssv text not null,
						hoten ntext null,
						gioitinh ntext null,
						ngaysinh ntext null,
						diachi ntext null
						);			
truncate table nhom4_ca2				
select * from nhom4_ca2
drop table nhom4_ca2;

--nhom 10 ca 1: file.csv--
create table nhom10_ca1(mssv text not null,
						ho ntext null,
						ten ntext null,
						maLop ntext null,
						ngaysinh ntext null,
						gioitinh ntext null
						);			
truncate table nhom10_ca1				
select * from nhom10_ca1
drop table nhom10_ca1;	


