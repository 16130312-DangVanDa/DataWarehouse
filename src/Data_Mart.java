package model.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import model.connection.GetConnection;
//ap dung partition vao de quan thi theo tung qui (3 thang)

public class Data_Mart {
	public static void main(String[] args) {
		new Data_Mart().mainMethod_update();

	}
	public void mainMethod_update() {
		Connection conn_DataMart = null;
		try {
			// 1. Mở kết nối database checkennlu_data_mart
			conn_DataMart = new GetConnection().getConnection("mart");// mo DB mart
			//** set auto commit to false
			conn_DataMart.setAutoCommit(false);
			// 2. tạo table tạm tên aggregated_temp trong database checken_data_mart
			PreparedStatement pre_DataMart = null;
			pre_DataMart = conn_DataMart
					.prepareStatement("CREATE TABLE `aggregated_temp` ( `id` int NOT NULL AUTO_INCREMENT, \r\n"
							+ "`ngay_insert_DW` date NOT NULL, \r\n" + "`so_luong_sv` int DEFAULT NULL, \r\n"
							+ " UNIQUE KEY `key_id_temp` (`id`, `ngay_insert_DW`)) \r\n"
							+ "PARTITION BY RANGE (month(`ngay_insert_DW`)) \r\n"
							+ "(PARTITION quater_1 VALUES LESS THAN (3), \r\n"
							+ "PARTITION quater_2 VALUES LESS THAN (6), \r\n"
							+ "PARTITION quater_3 VALUES LESS THAN (9), \r\n"
							+ "PARTITION quater_4 VALUES LESS THAN (12)\r\n" + ")");
			pre_DataMart.executeUpdate();
			// 3. Mở nối DB data_warehouse
			Connection con_DW = new GetConnection().getConnection("warehouse");// luu TG add vao DW
			// 4. Truy vấn data_warehouse để đếm số sinh viên được thêm vào Student table
			// theo từng ngày
			PreparedStatement pre_DW = con_DW
					.prepareStatement("select  CONVERT(VARCHAR(10), date_insert , 111) as date, COUNT(*) as quantity "
							+ "from Student GROUP BY CONVERT(VARCHAR(10), date_insert , 111)");
			// 5. Nhận được ResultSet chứa các record thỏa điều kiện
			ResultSet re = pre_DW.executeQuery();

			SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
			Date parsed;
			java.sql.Date sqlDate;
			// PreparedStatement pre_DataMart2 = null;
			// 6. Chạy từng record
			while (re.next()) {// Record?
				String dateText = re.getString("date");// ngay trong DW: la chuoi
				int quantity_New = re.getInt("quantity");
				try {
					// 7. Chuyển dateText từ chuỗi thành java.sql.Date
					parsed = format.parse(dateText);
					sqlDate = new java.sql.Date(parsed.getTime());
					// 8. Thêm record vào table tạm aggregated_temp trong database
					// checkennlu_data_mart
					pre_DataMart = conn_DataMart
							.prepareStatement("insert into aggregated_temp(ngay_insert_DW, so_luong_sv) values(?,?)");
					pre_DataMart.setDate(1, sqlDate);
					pre_DataMart.setInt(2, quantity_New);
					pre_DataMart.executeUpdate();
				} catch (ParseException e) {
					e.printStackTrace();
				}

			} // end while select... data_warehouse

			// 9. Ngắt kết nỗi tới database data_warehouse
			pre_DW.close();
			con_DW.close();

			// 10. Trong database chickennlu_data_mart đổi tên table aggregated thành abc
			pre_DataMart = conn_DataMart.prepareStatement("RENAME TABLE aggregated TO abc");
			pre_DataMart.executeUpdate();

			// 11.Trong database chickennlu_data_mart đổi tên table aggregated_temp thành
			// aggregated
			pre_DataMart = conn_DataMart.prepareStatement("RENAME TABLE aggregated_temp TO aggregated");
			pre_DataMart.executeUpdate();

			// 12. Trong database chickennlu_data_mart, xóa table abc
			pre_DataMart = conn_DataMart.prepareStatement("drop table abc");
			pre_DataMart.executeUpdate();

			// 13. commit transaction
			conn_DataMart.commit();// bat dau commit

			// 14. đóng kết nối tới database chickennlu_data_mart
			pre_DataMart.close();
			conn_DataMart.close();
		} catch (SQLException ex) {
			// condition: Lỗi kết nối
			// roll back the transaction
			try {
				if (conn_DataMart != null)
					conn_DataMart.rollback();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			System.out.println(ex.getMessage());
		}
	}

	/***************************************************************************/
	/***************************************************************************/
	/************************BO BO BO BO BO BO***********************************/
	/***************************************************************************/
	/***************************************************************************/
	// table: ngay them vao DW + so luong sinh vien them vao
	// 1. can lien ket Control voi DW theo id_file ==> group by tung ngay them vao
	// DW count so dong
	// 2. them du lieu vao Aggre...
	public void mainMethod() {
		try {

			// *** 1. kết nối DB data_warehouse
			Connection con_DW = new GetConnection().getConnection("warehouse");// luu TG add vao DW

			// *** 2. cau lenh

			// lay du lieu trong DW ==> bo vao cai bang tam DM, tao PArtition do san luon

			PreparedStatement pre_control = con_DW
					.prepareStatement("select  CONVERT(VARCHAR(10), date_insert , 111) as date, COUNT(*) as quantity "
							+ "from Student GROUP BY CONVERT(VARCHAR(10), date_insert , 111)");
			// can group by theo ngay
			ResultSet re = pre_control.executeQuery();

			// *** 3. chay tung record
			while (re.next()) {// tung ngay trong DW
				// bat dau dem so dong trong theo ma id
				String dateText = re.getString("date");// ngay trong DW: la chuoi
				int quantity_New = re.getInt("quantity");
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
				Date parsed;
				try {
					parsed = format.parse(dateText);
					java.sql.Date sql = new java.sql.Date(parsed.getTime());

					// ***4. ket noi DB chickennlu_data_mart
					Connection conn = new GetConnection().getConnection("mart");// mo DB mart

					// ***5. truy vấn
					PreparedStatement preMonth = conn.prepareStatement("SELECT EXTRACT(MONTH FROM ?) as month");// cat
																												// chi
																												// lay
																												// thang
																												// thoi
					preMonth.setString(1, dateText);
					ResultSet reMonth = preMonth.executeQuery();
					String month = null;
					while (reMonth.next()) {
						month = reMonth.getString("month");
					}

					PreparedStatement pre1 = conn.prepareStatement("select * from aggregated  PARTITION(month_" + month
							+ ") where ngay_insert_DW like '" + sql + "'");
					ResultSet re1 = pre1.executeQuery();
					boolean exis = false;
					int id = 0;
					while (re1.next()) {// da ton tai ngay nay

						int quntityOld = re1.getInt("so_luong_sv");
						id = re1.getInt("id");
						// ***6.1. Kiem tra so luong sinh vien co thay doi khong
						if (quntityOld != quantity_New) {// ngay da ton tai trong DM va so
							// luong khac
							exis = true;
							break;
						} else {
							exis = true;
						}
					}

					if (exis == true) {
						// *** 6.1.1. update lai ngay dong do so luong sinh vien
						PreparedStatement pre_Mart = conn.prepareStatement(
								"update  aggregated set so_luong_sv = " + quantity_New + " where id = " + id);
						pre_Mart.executeUpdate();
					} else {
						// 6.2. chen dong moi vao
						PreparedStatement pre_Mart = conn
								.prepareStatement("insert into aggregated(ngay_insert_DW, so_luong_sv) values(?,?)");
						pre_Mart.setDate(1, sql);
						pre_Mart.setInt(2, quantity_New);

						pre_Mart.executeUpdate();

					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			// dong
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// CREATE TABLE aggregated ( `id` int NOT NULL AUTO_INCREMENT, `ngay_insert_DW`
	// date not null, `so_luong_sv` int null, PRIMARY KEY(`id`, `ngay_insert_DW`) )
	// PARTITION BY RANGE( MONTH(ngay_insert_DW) )( PARTITION from_3_or_less VALUES
	// LESS THAN (3), PARTITION from_4 VALUES LESS THAN (4), PARTITION from_5 VALUES
	// LESS THAN (5), PARTITION from_5_and_up VALUES LESS THAN (12) )

	// SELECT * FROM `aggregated` PARTITION(from_4)

	// khong con xai nua//
	public void demo() {
		try {

			Connection con_DW = new GetConnection().getConnection("warehouse");// luu TG add vao DW
			PreparedStatement pre_control = con_DW.prepareStatement(
					"select  CONVERT(VARCHAR(10), date_insert , 111) as date, COUNT(*) as quantity from Student where isActive = 1 GROUP BY CONVERT(VARCHAR(10), date_insert , 111)");
			// can group by theo ngay
			ResultSet re = pre_control.executeQuery();
			while (re.next()) {
				// bat dau dem so dong trong theo ma id
				String dateText = re.getString("date");
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
				Date parsed;
				try {
					parsed = format.parse(dateText);
					java.sql.Date sql = new java.sql.Date(parsed.getTime());

					Connection conn = new GetConnection().getConnection("mart");// mo DB mart
					PreparedStatement pre_Mart = conn
							.prepareStatement("insert into aggregated(ngay_insert_DW, so_luong_sv) values(?,?)");

					pre_Mart.setDate(1, sql);
					pre_Mart.setInt(2, re.getInt("quantity"));

					pre_Mart.executeUpdate();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			// dong
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
