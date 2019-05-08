package model.database;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import model.connection.GetConnection;

//Chuyen Staging vao Data_Warehouse
public class DataWarehouse {

	public static void main(String[] args) {
		new DataWarehouse().insertFromStagingToDW_OPTIMIZE();
	}

	public void insertFromStagingToDW_OPTIMIZE() {
		// ket noi voi Control
		Connection con_Control = null;
		PreparedStatement pre_control = null;
		// ket noi cho Staging
		Connection conn_staging = null;
		PreparedStatement pre_staging = null;
		// mo cho Datawarehouse
		Connection conn_DW1 = null;
		PreparedStatement pre_DW = null;

		try {
			// 1. Kết nối database chickennlu_Control_DB
			con_Control = new GetConnection().getConnection("control");

			pre_control = con_Control.prepareStatement("SELECT * FROM hard_code");
			ResultSet re_hardCode = pre_control.executeQuery();
			String success = null, error = null;
			String time_datawarehouse_update = null, status_file_update = null;
			while (re_hardCode.next()) {
				success = re_hardCode.getString("status_file_WAREHOUSE_success");
				error = re_hardCode.getString("status_file_WAREHOUSE_ERROR");
				time_datawarehouse_update = re_hardCode.getString("time_datawarehouse_update");
				status_file_update = re_hardCode.getString("status_file_update");
			}

			// 2. Lấy các file có trạng thái là OK Staging tại các nhóm có isActive = 1
			pre_control = con_Control.prepareStatement(
					"select data_file_logs.id ,data_file_logs.ID_host, data_file_configuaration.data_warehouse_sql,"
							+ " data_file_configuaration.insert_DW_sql, data_file_configuaration.table_staging_load"
							+ " from data_file_logs JOIN data_file_configuaration "
							+ "on data_file_logs.ID_host=data_file_configuaration.id "
							+ "where data_file_configuaration.isActive=1 AND data_file_logs.status_file like 'Ok Staging'");
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?

				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				int maGroup = re.getInt("ID_host");// ma group
				// data_warehouse_sql: mssv, cast([ho] as varchar(100)) + ' ' + cast([ten] as
				// varchar(100))as hoten,
				// ngaysinh, gioitinh
				String sql = re.getString("data_warehouse_sql");
				String sql_DW = re.getString("insert_DW_sql");
				String table_src = re.getString("table_staging_load");// from + table staging

				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy từng mssv, hoten, ngay sinh, gioitinh trong table của database Staging
				pre_staging = conn_staging.prepareStatement("select " + sql + " from " + table_src);
				// 7. Trả về Result Set chứa các record thỏa điều kiện
				ResultSet re_staging = pre_staging.executeQuery();
				boolean err = false;

				while (re_staging.next()) {// chay tung Record?
					String id = re_staging.getString("mssv");
					String full_name = re_staging.getString("hoten");
					String ngay = re_staging.getString("ngaysinh");
					String gender = re_staging.getString("gioitinh");
					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {
						java.util.Date date = formatter.parse(ngay);
						sqlDate = new java.sql.Date(date.getTime());
						System.out.println(sqlDate);// ngay trong Staging

						// 8.1.1. Mở connection database data_warehouse
						conn_DW1 = new GetConnection().getConnection("warehouse");
						// 8.1.2. Truy xuất SK của sinh viên có mã SV là id và các file của nhóm là
						// maGroup tại các sinh viên đang active
						String sql_exceute = "select sk, id, full_name, dob, gender from Student where sk = (select sk from Student where id = '"
								+ id + "' and id_file_group = " + maGroup + " and isActive = 1)";
						pre_DW = conn_DW1.prepareStatement(sql_exceute);
						// 8.1.3. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
						ResultSet re_DW = pre_DW.executeQuery();
						int sk_DW = 0;
						String checkExist = "NO";
						String nameTemp = null;
						Date dob_Temp = null;
						String gender_Temp = null;
						String id_temp = null;
						while (re_DW.next()) { // Record?
							// Yes:
							id_temp = re_DW.getString("id");
							nameTemp = re_DW.getString("full_name");
							dob_Temp = re_DW.getDate("dob");
							gender_Temp = re_DW.getString("gender");
							// 11.2.1 So sách các trường còn lại của SV Staging có gì khác không so với
							// SV trong DataWarehouse không?
							if (nameTemp.equalsIgnoreCase(full_name) && dob_Temp.equals(sqlDate)
									&& gender_Temp.equalsIgnoreCase(gender)) {
								checkExist = "NOCHANGE";
							} else {
								// 11.2.2.1. Lấy sk của sinh viên đó
								sk_DW = re_DW.getInt("sk");
								checkExist = "YES";
							}
						} // end while

						if (checkExist.equalsIgnoreCase("YES")) {
							// *** Yes: Tồn tại + có thay đổi: Nhánh 11.2.2.

							// 11.2.2.2. In thôn báo thay đổi thông tin SV có mã của nhóm
							System.out.println("==> Thay đôi TTSV mã: " + id_temp + ", " + nameTemp + ", " + gender_Temp
									+ ", " + dob_Temp + " , cua nhom " + maGroup + " THANH " + id + ", " + full_name
									+ ", " + gender + ", " + sqlDate);
							// 11.2.2.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
							// ngày giờ hiện tại
							pre_DW = conn_DW1.prepareStatement(
									"update Student set isActive = 0, date_change=getDate() where sk = " + sk_DW);
							pre_DW.executeUpdate();
							// 11.2.2.4.Thêm dòng SV vào table Student của data_warehouse
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
							// 11.2.2.5. tăng số dòng cập nhật lên
							count_UPDATE++;

						} else if (checkExist.equalsIgnoreCase("NO")) {
							// **** NO: them moi hoan toan

							// 8.1.4.2.Thêm mới dòng đó vào Student table của database data_warehouse
							System.out.println("==> them moi SV: " + id + ", " + full_name + ", " + gender + ", "
									+ sqlDate + ", cua nhom " + maGroup);
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
							// 11.1..3. tăng số dòng thêm mới lên
							count_NEW++;

						} else if (checkExist.equalsIgnoreCase("NOCHANGE")) {
							// *** NOCHANGE: giong y chang, khong co gi thay doi
							System.out.println("==> KHONG CO GI THAY DOI: TT trong DW" + id_temp + ", " + nameTemp
									+ ", " + gender_Temp + ", " + dob_Temp + " , cua nhom " + maGroup
									+ " TT trong Staging " + id + ", " + full_name + ", " + gender + ", " + sqlDate);
							// 11.2.1.1. Tăng số dòng không cần thêm vào data_warehouse lên 1
							countEXIST++;
						}
					} catch (ParseException e) {
						e.printStackTrace();
						err = true;
						System.out.println("ngay khong dung dinh dang");
					}

				} // end while: 1 SV staging

				// kiem tra ERR eps kieu cho ngay thoi
				if (err == true) {
					// 12.b. Update trạng thái file là ERROR_DATE_DW và time_data_warehouse là TG
					// hiện tại
					pre_control = con_Control
							.prepareStatement("update data_file_logs set " + status_file_update + "'" + error + "' ,"
									+ time_datawarehouse_update + "now(), exist_row_DW =" + countEXIST + ", row_new_DW="
									+ count_NEW + ", row_update_DW = " + count_UPDATE + " where id=" + id_file);
					pre_control.executeUpdate();
					System.out.println("update error!! " + id_file);
				} else {
					// 12.a. Update trạng thái file là OK DW và time_data_warehouse là TG hiện
					// tại
					pre_control = con_Control.prepareStatement("update data_file_logs set " + status_file_update + "'"
							+ success + "' ," + time_datawarehouse_update + "now(), exist_row_DW =" + countEXIST
							+ " , row_new_DW=" + count_NEW + ", row_update_DW = " + count_UPDATE + "   where id="
							+ id_file);
					pre_control.executeUpdate();
					System.out.println("update staus_file = OK DW " + id_file);
				}

				// 13 truncate table
				pre_staging = conn_staging.prepareStatement("truncate table " + table_src);
				pre_staging.executeUpdate();
				System.out.println("truncate done!! " + table_src);

			} // end while control

			// 14. Đóng tất cả kết nối
			pre_control.close();
			pre_DW.close();
			pre_DW.close();
			con_Control.close();
			conn_DW1.close();
			conn_staging.close();

		} catch (SQLException e1) {
			// loi ket noi toi DB
			// In ra man hình lỗi kết nối
			e1.printStackTrace();
			System.out.println(e1.getMessage());

		}
	}

	// chen 1 dong vao DW
	public boolean insertALine(Connection conn_DW, String sql, String id, String full_name, String gender, Date dob,
			String table_Staging, int idHostGroup) {

		int i = 0;
		PreparedStatement pre = null;
		try {
			// 11.1.1. Tìm Date_SK của ngày sinh sinh trong Date_dim table
			pre = conn_DW.prepareStatement("select Date_SK from Date_dim where Full_date like ?");
			pre.setDate(1, dob);
			ResultSet re_date = pre.executeQuery();
			int sk = 0;
			while (re_date.next()) {
				sk = re_date.getInt("Date_SK");
			}

			// 11.1.2. Thêm thông tin cần thiết của SV đó vào Student table của database
			// data_warehouse
			pre = conn_DW.prepareStatement("insert into " + sql);
			pre.setString(1, id);// ma sv
			pre.setString(2, full_name);// hoten
			pre.setDate(3, dob);// ngay sinh
			pre.setInt(4, sk);// index_ngaysinh
			pre.setString(5, gender);// gioitinh
			pre.setString(6, table_Staging);// file_source
			pre.setInt(7, idHostGroup);

			i = pre.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (i > 0) {
			return true;
		}
		return false;
	}

	/*********************************************************************************/
	/*********************************************************************************/
	/*********************************************************************************/
	/*******************************************************************************/

	public void insertFromStagingToDW() {

		try {
			// 1. Kết nối database chickennlu_Control_DB
			Connection con_Control = new GetConnection().getConnection("control");
			// 2. Lấy các file có trạng thái là OK Staging tại các nhóm có isActive = 1
			PreparedStatement pre_control = con_Control.prepareStatement(
					"select data_file_logs.id ,data_file_logs.ID_host, data_file_configuaration.data_warehouse_sql,"
							+ " data_file_configuaration.insert_DW_sql, data_file_configuaration.table_staging_load, status_file_update, time_datawarehouse_update"
							+ " from data_file_logs JOIN data_file_configuaration "
							+ "on data_file_logs.ID_host=data_file_configuaration.id "
							+ "where data_file_configuaration.isActive=1 AND data_file_logs.status_file like 'Ok Staging'");
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {

				int id_file = re.getInt("id"); // ma file
				int maGroup = re.getInt("ID_host");// ma group
				int status_file_update = re.getInt("status_file_update");
				int time_datawarehouse_update = re.getInt("time_datawarehouse_update");
				String sql = re.getString("data_warehouse_sql");// sql select theo 1 cau truc nhat dinh
				// ket noi DW
				String sql_DW = re.getString("insert_DW_sql");
				String table_src = re.getString("table_staging_load");

				// 5. Mở connection của database Staging
				Connection conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy từng mssv, hoten, ngay sinh, gioitinh trong table của database Staging
				PreparedStatement pre_staging = conn_staging.prepareStatement(sql);
				// 7. Trả về Result Set chứa các record thỏa điều kiện
				ResultSet re_staging = pre_staging.executeQuery();

				boolean err = false;
				while (re_staging.next()) {// chay tung Record?

					String id = re_staging.getString("mssv");
					String full_name = re_staging.getString("hoten");
					String ngay = re_staging.getString("ngaysinh");
					String gender = re_staging.getString("gioitinh");
					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {

						java.util.Date date = formatter.parse(ngay);
						sqlDate = new java.sql.Date(date.getTime());
						System.out.println(sqlDate);// ngay trong Staging
						// **Muc tieu: so sanh tung dong trong DW
						// *** 1. neu khac msv => them vao
						// *** 2. giong ma sv thi => isActive cai cu + add cai moi vao

						// 8.1.1. Mở connection database data_warehouse
						Connection conn_DW1 = new GetConnection().getConnection("warehouse");
						// 8.1.2. Truy xuất SK của sinh viên có mã SV là id và các file của nhóm là
						// maGroup tại các sinh viên đang active
						String sql_exceute = "select sk, id, full_name, dob, gender from Student where sk = (select sk from Student where id = '"
								+ id + "' and id_file_group = " + maGroup + " and isActive = 1)";
						Statement pre_DW = conn_DW1.createStatement();
						// 8.1.3. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
						ResultSet re_DW = pre_DW.executeQuery(sql_exceute);
						int sk_DW = 0;
						String checkExist = "NO";

						while (re_DW.next()) { // Record?
							// Yes
							String nameTemp = re_DW.getString("full_name");
							Date dob_Temp = re_DW.getDate("dob");
							String gender_Temp = re_DW.getString("gender");
							// 8.1.4.1.1 So sách các trường còn lại của SV Staging có gì khác không so với
							// SV
							// trong DataWarehouse không?
							if (nameTemp.equalsIgnoreCase(full_name) && dob_Temp.equals(sqlDate)
									&& gender_Temp.equalsIgnoreCase(gender)) {
								checkExist = "NOCHANGE";
							} else {
								// 8.1.4.1.2 Lấy sk của sinh viên đó
								sk_DW = re_DW.getInt("sk");
								checkExist = "YES";
							}
						}

						if (checkExist.equalsIgnoreCase("YES")) {
							// *** YES
							// 8.1.4.1.3. Giải pháp thêm SV đã tồn tại
							System.out.println("==> ton tai tu truoc roi + co thay doi: " + id);
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
							PreparedStatement pre = conn_DW1.prepareStatement(
									"update Student set isActive = 0, date_change=getDate() where sk = " + sk_DW);
							pre.executeUpdate();
						} else if (checkExist.equalsIgnoreCase("NO")) {
							// **** NO
							// 8.1.4.2.Thêm mới dòng đó vào Student table của database data_warehouse
							System.out.println("==> them moi lan dau");
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
						} else if (checkExist.equalsIgnoreCase("NOCHANGE")) {
							// *** NOCHANGE
							System.out.println("Khong lam gi==> khong co thay doi");

						}

					} catch (ParseException e) {
						e.printStackTrace();
						err = true;
						System.out.println("ngay khong dung dinh dang");
					}

				} // end while: staging

				// kiem tra ERR eps kieu cho ngay thoi
				if (err == true) {
					// 8.2.1.b. Update trạng thái file là ERROR_DATE_DW và time_data_warehouse là TG
					// hiện tại
					PreparedStatement pre_Control_update = con_Control
							.prepareStatement("update data_file_logs set " + status_file_update + "'ERROR_DATE_DW',"
									+ time_datawarehouse_update + "now() where id=" + id_file);
					pre_Control_update.executeUpdate();
					System.out.println("update error!! " + id_file);
				} else {
					// 8.2.1.a. Update trạng thái file là OK DW và time_data_warehouse là TG hiện
					// tại
					PreparedStatement pre_Control_update = con_Control
							.prepareStatement("update data_file_logs set " + status_file_update + "'OK DW',"
									+ time_datawarehouse_update + "now()  where id=" + id_file);
					pre_Control_update.executeUpdate();
					System.out.println("update staus_file = OK DW " + id_file);
				}
				// 8.2. xong 1 bang thi truncate table ==>
				PreparedStatement pre_Truncate_Staging_Table = conn_staging
						.prepareStatement("truncate table " + table_src);
				pre_Truncate_Staging_Table.executeUpdate();
				System.out.println("truncate done!! " + table_src);

			} // end while control

		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	// b1: add cai date vao Dateware house

	// add lan dau tien => add vo luon khong can xet: chuyen date vao cai

	// tu lan 2 tro di => lay tung hang trong Staging so vs tung hang trong DW
	// 1. neu giong ma, bat ki cot nao khac => isActive <- false; insert moi
	// 2. khac ma so => insert binh thuong

	// ***************//
	// 1. add lan dau tien
	// public void insertFromStagingToDW_First() {
	// // 1. ket noi toi Control DB==> lay cac table
	// // 2. chay tưng result set de lay cau lenh sql
	// // 3. ket noi toi staging
	// // 4. chay sql => resultset => add tung (sk, mssv, ho ten, index_ngaysinh
	// ,ngay
	// // sinh, gioi tinh)
	//
	// try {
	// Connection con_Control = new GetConnection().getMySQLConnection_Control();
	// PreparedStatement pre_control = con_Control.prepareStatement(
	// "select data_file_logs.id,data_file_logs.ID_host,
	// data_file_configuaration.data_warehouse_sql,"
	// + " data_file_configuaration.insert_DW_sql,
	// data_file_configuaration.table_staging_load"
	// + " from data_file_logs JOIN data_file_configuaration "
	// + "on data_file_logs.ID_host=data_file_configuaration.id "
	// + "where data_file_configuaration.isActive=1 AND data_file_logs.status_file
	// like 'OK Staging'");
	//
	// ResultSet re = pre_control.executeQuery();
	//
	// while (re.next()) {
	// int id_Host = re.getInt("id");// ma thu tu file trong logs
	// int maGroup = re.getInt("ID_host");
	// String sql = re.getString("data_warehouse_sql");// co ca cau lenh sql
	// // ket noi voi staging ==> mo cai table tuong ung ra
	// Connection conn_staging = new GetConnection().getConnection_Staging();
	// PreparedStatement pre_staging = conn_staging.prepareStatement(sql);
	// ResultSet re_staging = pre_staging.executeQuery();// tung dong trong staging
	//
	// // ket noi DW
	// String sql_DW = re.getString("insert_DW_sql");
	// String table_src = re.getString("table_staging_load");
	// // Connection conn_DW = new GetConnection().getConnection_DW();
	// // PreparedStatement pre_DW = conn_DW.prepareStatement(sql_DW);
	//
	// boolean err = false;
	// while (re_staging.next()) {// chay tung hang torng table staging
	// String id = re_staging.getString("mssv");
	// String full_name = re_staging.getString("hoten");
	// String ngay = re_staging.getString("ngaysinh");
	// String gender = re_staging.getString("gioitinh");
	// // chuyen chuoi thanh ngay
	// SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
	// java.sql.Date sqlDate = null;
	// try {
	//
	// java.util.Date date = formatter.parse(ngay);
	// sqlDate = new java.sql.Date(date.getTime());
	// // vua them vua do ngay voi bang Date_dim
	// insertALine(sql_DW, id, full_name, gender, sqlDate, table_src, id_Host);
	//
	// } catch (ParseException e) {
	// e.printStackTrace();
	// err = true;
	// System.out.println("ngay khong dung dinh dang");
	// }
	// }
	// if (err == true) {
	// // update vao logs la file bi loi
	// PreparedStatement pre_Control_update = con_Control.prepareStatement(
	// "update data_file_logs set
	// status_file='error_date_DW',data_file_logs.time_data_warehouse=now() where
	// id="
	// + id_Host);
	// pre_Control_update.executeUpdate();
	// System.out.println("update error!! " + id_Host);
	//
	// } else {
	// // xong 1 bang thi truncate table ==>
	// PreparedStatement pre_Truncate_Staging_Table = conn_staging
	// .prepareStatement("truncate table " + table_src);
	// pre_Truncate_Staging_Table.executeUpdate();
	// System.out.println("truncate done!! " + table_src);
	// // update trang thai file: OK DW
	// PreparedStatement pre_Control_update = con_Control.prepareStatement(
	// "update data_file_logs set status_file='OK
	// DW',data_file_logs.time_data_warehouse=now() where id="
	// + id_Host);
	// pre_Control_update.executeUpdate();
	// System.out.println("update staus_file = OK DW " + id_Host);
	// }
	// }
	//
	// } catch (ClassNotFoundException e1) {
	// e1.printStackTrace();
	// } catch (SQLException e1) {
	// e1.printStackTrace();
	// }
	// }
	//

	// // lan 2 thi can lay tung dong o staging so sanh voi tung dong trong
	//
	// public void insertFromStagingToDW_Orther() {
	// // 1. ket noi toi Control DB==> lay cac table
	// // 2. chay tưng result set de lay cau lenh sql
	// // 3. ket noi toi staging
	// // 4. chay sql => resultset => add tung (sk, mssv, ho ten, index_ngaysinh
	// ,ngay
	// // sinh, gioi tinh)
	// try {
	// Connection con_Control = new GetConnection().getMySQLConnection_Control();
	// PreparedStatement pre_control = con_Control.prepareStatement(
	// "select data_file_logs.id,data_file_logs.ID_host,
	// data_file_configuaration.data_warehouse_sql,"
	// + " data_file_configuaration.insert_DW_sql,
	// data_file_configuaration.table_staging_load"
	// + " from data_file_logs JOIN data_file_configuaration "
	// + "on data_file_logs.ID_host=data_file_configuaration.id "
	// + "where data_file_configuaration.isActive=1 AND data_file_logs.status_file
	// like 'Ok Staging'");
	//
	// ResultSet re = pre_control.executeQuery();
	//
	// while (re.next()) {
	// int id_Host = re.getInt("id");
	// int maGroup = re.getInt("ID_host");
	// String sql = re.getString("data_warehouse_sql");// co ca cau lenh sql
	// // ket noi voi staging ==> mo cai table tuong ung ra
	// Connection conn_staging = new GetConnection().getConnection_Staging();
	// PreparedStatement pre_staging = conn_staging.prepareStatement(sql);
	// ResultSet re_staging = pre_staging.executeQuery();// tung dong trong staging
	//
	// // ket noi DW
	// String sql_DW = re.getString("insert_DW_sql");
	// String table_src = re.getString("table_staging_load");
	//
	// boolean err = false;
	// while (re_staging.next()) {// chay tung hang torng table staging
	// String id = re_staging.getString("mssv");
	// String full_name = re_staging.getString("hoten");
	// String ngay = re_staging.getString("ngaysinh");
	// String gender = re_staging.getString("gioitinh");
	// // chuyen chuoi thanh ngay
	// SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
	// java.sql.Date sqlDate = null;
	// try {
	//
	// java.util.Date date = formatter.parse(ngay);
	// sqlDate = new java.sql.Date(date.getTime());
	//
	// // so sanh tung dong trong DW
	// // 1. neu khac msv => them vao
	// // 2. giong ma sv thi => isActive cai cu + add cai moi vao
	// Connection conn_DW = new GetConnection().getConnection_DW();
	// PreparedStatement pre_DW = conn_DW.prepareStatement("select * from Student
	// where isActive=0");
	// ResultSet re_DW = pre_DW.executeQuery();// chay tung dong trong DW
	//
	// boolean checkExit = false;// kiem tra ton tai
	// int sk_DW = 0;
	// while (re_DW.next()) { // ma sv khac, them dong dos vao
	// sk_DW = re_DW.getInt("sk");// sk cua cai dang xet
	// String id_DW = re_DW.getString("id");// mssv DW
	// if (id.equalsIgnoreCase(id_DW)) {
	// checkExit = true;// da ton tai dong nay
	// System.out.println("co ton tai chua " + checkExit);
	// break;
	//
	// }
	// }
	// if (checkExit == false) {
	// // them vao dong moi hoan toan
	// insertALine(sql_DW, id, full_name, gender, sqlDate, table_src, id_Host);
	// } else {
	// // isActive = 1 + insert cai moi vao
	// insertALine(sql_DW, id, full_name, gender, sqlDate, table_src, id_Host);
	// PreparedStatement pre = conn_DW.prepareStatement(
	// "update Student set isActive = 1, date_change=getDate() where sk = " +
	// sk_DW);
	// // pre.setInt(1, sk);
	// pre.executeUpdate();
	//
	// }
	//
	// } catch (ParseException e) {
	// e.printStackTrace();
	// err = true;
	// System.out.println("ngay khong dung dinh dang");
	//
	// }
	//
	// }
	// if (err == true) {
	// // update vao logs la file bi loi
	// PreparedStatement pre_Control_update = con_Control.prepareStatement(
	// "update data_file_logs set
	// status_file='error_date_DW',data_file_logs.time_data_warehouse=now() where
	// id="
	// + id_Host);
	// pre_Control_update.executeUpdate();
	// System.out.println("update error!! " + id_Host);
	//
	// } else {
	// // xong 1 bang thi truncate table ==>
	// PreparedStatement pre_Truncate_Staging_Table = conn_staging
	// .prepareStatement("truncate table " + table_src);
	// pre_Truncate_Staging_Table.executeUpdate();
	// System.out.println("truncate done!! " + table_src);
	// // update trang thai file: OK DW
	// PreparedStatement pre_Control_update = con_Control.prepareStatement(
	// "update data_file_logs set status_file='OK
	// DW',data_file_logs.time_data_warehouse=now() where id="
	// + id_Host);
	// pre_Control_update.executeUpdate();
	// System.out.println("update staus_file = OK DW " + id_Host);
	// }
	//
	// }
	//
	// } catch (ClassNotFoundException e1) {
	// e1.printStackTrace();
	// } catch (SQLException e1) {
	// e1.printStackTrace();
	// }
	// }

	// so ngay sinh nay co index la bao nhieu roi tim trong bang sinh vien ngay ngay
	// sinh co sk do laf ok
	public void insertFromStagingToDW_Orther_DEMO() {
		// 1. ket noi toi Control DB==> lay cac table
		// 2. chay tưng result set de lay cau lenh sql
		// 3. ket noi toi staging
		// 4. chay sql => resultset => add tung (sk, mssv, ho ten, index_ngaysinh ,ngay
		// sinh, gioi tinh)
		try {
			// 1. ket noi DB chickennlu_Control_DB
			Connection con_Control = new GetConnection().getConnection("control");// ket noi Control
			// 2. Truy vấn câu lệnh: lay cac file phu hop
			PreparedStatement pre_control = con_Control.prepareStatement(
					"select data_file_logs.id ,data_file_logs.ID_host, data_file_configuaration.data_warehouse_sql,"
							+ " data_file_configuaration.insert_DW_sql, data_file_configuaration.table_staging_load"
							+ " from data_file_logs JOIN data_file_configuaration "
							+ "on data_file_logs.ID_host=data_file_configuaration.id "
							+ "where data_file_configuaration.isActive=1 AND data_file_logs.status_file like 'Ok Staging'");

			// 3. tra ve result set
			ResultSet re = pre_control.executeQuery();
			// 4. chạy từng record: tung cai ten tablename
			while (re.next()) {// chạy tung file
				int id_Host = re.getInt("id"); // ma file
				int maGroup = re.getInt("ID_host");// ma group moi dung cua cai file table
				String sql = re.getString("data_warehouse_sql");// co ca cau lenh sql
				// 5. ket noi voi staging ==> mo cai table tuong ung ra
				Connection conn_staging = new GetConnection().getConnection("staging");

				// 6. Lấy mssv, hoten, ngay sinh, gioitinh trong từng dòng dữ liệu trong từng
				// table
				PreparedStatement pre_staging = conn_staging.prepareStatement(sql);

				ResultSet re_staging = pre_staging.executeQuery();// tung dong trong staging

				// ket noi DW
				String sql_DW = re.getString("insert_DW_sql");
				String table_src = re.getString("table_staging_load");

				boolean err = false;
				while (re_staging.next()) {// chay tung hang trong table staging

					String id = re_staging.getString("mssv");
					String full_name = re_staging.getString("hoten");
					String ngay = re_staging.getString("ngaysinh");
					String gender = re_staging.getString("gioitinh");
					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {

						java.util.Date date = formatter.parse(ngay);
						sqlDate = new java.sql.Date(date.getTime());
						System.out.println(sqlDate);// ngay trong Staging
						// **Muc tieu: so sanh tung dong trong DW
						// *** 1. neu khac msv => them vao
						// *** 2. giong ma sv thi => isActive cai cu + add cai moi vao

						// 7.1.1. ket noi DB data_warehouse
						Connection conn_DW1 = new GetConnection().getConnection("warehouse");
						// 7.1.2. lay tat ca sinh vien co index_ngay trong table Student DW
						Statement pre_DW = conn_DW1.createStatement();
						ResultSet re_DW = pre_DW
								.executeQuery("select * from Student where isActive=1 and index_ngaysinh = "
										+ "(select Date_SK from Date_dim where Full_date like '" + sqlDate + "')");

						boolean checkExit = false;// kiem tra ton tai

						System.out.println("aaa");
						int sk_DW = 0;

						// 7.1.3. chay tung record
						while (re_DW.next()) { // ma sv khac, them dong dos vao

							sk_DW = re_DW.getInt("sk");// sk cua cai dang xet
							String id_DW = re_DW.getString("id");// mssv trong DW
							int hostAdd = re_DW.getInt("id_file_group");// id_host

							// 7.1.3.1. so mssv trong Staging vs DW && mafile neu giong nhau
							if (id.equalsIgnoreCase(id_DW)) { // ma bang va idHost dung file
								if (hostAdd == maGroup) {
									checkExit = true;// da ton tai dong nay
									System.out.println("co ton tai tu truoc chua ==> " + checkExit);
									break;
								}
							}
						}
						if (checkExit == false) {
							// 7.1.3.2. them vao dong moi hoan toan
							System.out.println("==> them moi lan dau");
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
						} else {
							// 7.1.3.1.1. isActive = 1 + insert cai moi vao
							System.out.println("==> ton tai tu truoc chua ");
							insertALine(conn_DW1, sql_DW, id, full_name, gender, sqlDate, table_src, maGroup);
							PreparedStatement pre = conn_DW1.prepareStatement(
									"update Student set isActive = 0, date_change=getDate() where sk = " + sk_DW);
							// pre.setInt(1, sk);
							pre.executeUpdate();
						}

						// dong DW

					} catch (ParseException e) {
						e.printStackTrace();
						err = true;
						System.out.println("ngay khong dung dinh dang");
					}
				}
				if (err == true) {
					// update vao logs la file bi loi
					PreparedStatement pre_Control_update = con_Control.prepareStatement(
							"update data_file_logs set status_file='error_date_DW',data_file_logs.time_data_warehouse=now() where id="
									+ id_Host);
					pre_Control_update.executeUpdate();
					System.out.println("update error!! " + id_Host);

				} else {
					// 7.2. xong 1 bang thi truncate table ==>
					PreparedStatement pre_Truncate_Staging_Table = conn_staging
							.prepareStatement("truncate table " + table_src);
					pre_Truncate_Staging_Table.executeUpdate();
					System.out.println("truncate done!! " + table_src);
					// update trang thai file: OK DW
					PreparedStatement pre_Control_update = con_Control.prepareStatement(
							"update data_file_logs set status_file='OK DW',data_file_logs.time_data_warehouse=now()  where id="
									+ id_Host);
					pre_Control_update.executeUpdate();
					System.out.println("update staus_file = OK DW " + id_Host);
				}
			}

		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

}
