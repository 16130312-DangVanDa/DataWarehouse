  create database data_warehouse
go 
use data_warehouse

/*table date*/
set dateformat Mdy
create table Date_dim(Date_SK int primary key IDENTITY not null ,
 Full_date date null,
  DAY_SINCE_2005 int null, 
  Month_since_2005 int null, 
  Day_Of_Week text null,
  CALENDAR_MONTH text null,
  CALENDAR_YEAR int null,
  Calendar_Year_Month text null,
  Day_OF_Month int null,
  Day_of_year int null,
  week_of_year_sunday int null,
  year_week_sunday text null,
  WEEK_SUNDAY_START text null,
  WEEK_OF_YEAR_MONDAY text null,
  YEAR_WEEK_MONDAY text null,
  WEEK_MONDAY_START text null,
  HOLIDAY text null,
  DAY_TYPE text null,
  QUARTER_OF_YEAR text null, 
  QUARTER_SINCE_2005 text null
  );
  create nonclustered index nonclustered_Date_dim_Fulldate on Date_dim(Full_date);
  select Date_SK from Date_dim where Full_date = '2005-10-02'

  /*insert data for date table*/
  drop table Date_dim
  truncate table Date_dim
  select * from Date_dim where Date_SK = 100
  
  
BULK
INSERT Date_dim
FROM 'D:\Data_Dim.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n'
)

//**//
create table Student(sk int primary key not null IDENTITY,
						id varchar(25) null,
						full_name NVARCHAR(70) null,
						dob date null,
						index_ngaysinh int null,
						gender NVARCHAR(5) null,
						isActive bit default(1) null,
						date_insert datetime default(getDate()) null,
						date_change datetime null,
						file_src text null,
						id_file_group int null);
						
create index IX_Student_Id_IDFileGroup on Student(id, id_file_group);--tao index
DROP INDEX IX_Student_Id_IDFileGroup ON Student-- xoa index
Exec sp_helpindex 'Student'--kiem tra


/*************** Test code ****************/
select sk, id, full_name, dob, gender  from Student where sk = (select sk from Student where id = '152101094' and id_file_group = 4 and isActive = 1)
select * from Student where id = '16130490' and id_file_group=2
select * from Student where id like '18L11000260'
delete from Student where id_file_group=9041
drop table Student
SET DATEFORMAT ymd
truncate table Student
select  CONVERT(VARCHAR(10), date_insert , 111), COUNT(*) from Student where isActive = 1 GROUP BY CONVERT(VARCHAR(10), date_insert , 111)
select * from Student where isActive=1 and index_ngaysinh = (select Date_SK from Date_dim where Full_date like '1998-10-02');

--can chuyen ve date tai vi de datetime, moi giay no se tinh la khac nhau--
select  CONVERT(VARCHAR(10), date_insert , 111) as date, COUNT(*) as quantity from Student GROUP BY CONVERT(VARCHAR(10), date_insert , 111)
select  date_insert, COUNT(*) as quantity from Student GROUP BY date_insert