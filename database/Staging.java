package model.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import model.connection.GetConnection;

//xu li data tu LOCAL vao STAGING
public class Staging {

	public void demo() {
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// 1. Kết nối tới chickennlu_Control_DB
			conn = new GetConnection().getConnection("control");

			pre_control = conn.prepareStatement("SELECT * FROM hard_code");
			ResultSet re_hardCode = pre_control.executeQuery();
			String success = null, error = null;
			String status_file_update = null, time_staing_update = null;
			while (re_hardCode.next()) {
				success = re_hardCode.getString("status_file_STAGING_sucsess");
				error = re_hardCode.getString("status_file_STAGING_ERROR");
				status_file_update = re_hardCode.getString("status_file_update");
				time_staing_update = re_hardCode.getString("time_staging_update");
			}

			// 2. Tìm các file có trạng thái OK download ở các nhóm đang active
			pre_control = conn.prepareStatement(
					"SELECT data_file_logs.id ,data_file_logs.ID_host,data_file_logs.your_filename, table_staging_load, "
							+ " data_file_logs.delimiter, data_file_configuaration.download_to_dir_local,encode,"
							+ "data_file_configuaration.insert_staging, data_file_configuaration.number_column from data_file_logs "
							+ "JOIN data_file_configuaration ON data_file_logs.ID_host = data_file_configuaration.id"
							+ " where "
							+ "data_file_logs.status_file like 'OK download' AND data_file_configuaration.isActive=1 ");
			// 3. Nhận được ResultSet chứa các record thỏa điều kiện truy xuất
			ResultSet re = pre_control.executeQuery();

			int id;
			String filename = null;
			// 4. chạy từng record trong resultset
			while (re.next()) {
				// mo file
				id = re.getInt("id");
				String encode = re.getString("encode");

				String valuesList = re.getString("insert_staging");// valuesList
				String table_staging = re.getString("table_staging_load");// valuesList
				String dir = re.getString("download_to_dir_local");
				filename = re.getString("your_filename");

				String delimiter = re.getString("delimiter");// dau phan cac cac phan tu
				int number_column = re.getInt("number_column");// so cot

				// 5. Kiểm tra file có tồn tại trên folder local "Data_Warehouse" chưa
				String path = "D:\\" + dir + "\\" + filename;
				System.out.println(path);
				File file = new File(path);// mo file
				if (!file.exists()) {
					// 6.1.1. Thông báo file không tồn tại ra màn hình
					System.out.println(path + "khong ton tai");
					// 6.1.2. Cập nhật status_file là ERROR Staging, time_staging là ngày giờ hiện
					// tại
					String sql2 = "UPDATE data_file_logs SET " + status_file_update + "'ERROR at Staging', "
							+ time_staing_update + "now() WHERE id=" + id;
					pre_control = conn.prepareStatement(sql2);
					pre_control.executeUpdate();
				} else {

					try {
						// 6.2.1. Mở file để đọc dữ liệu lên, có kèm theo encoding
						BufferedReader reader = new BufferedReader(
								new InputStreamReader(new FileInputStream(file), encode));
						// 6.2.2. Đọc bỏ phần header
						reader.readLine();

						// 6.2.3. Bắt đầu từ hàng thứ 2, đọc từng hàng dữ liệu đến khi cuối file
						String data;
						data = reader.readLine();
						System.out.println(data);
						int count = 0;
						while (data != null) {// còn hàng?
							// 6.2.4. cắt hàng theo delimeter lưu trên data_file_logs
							StringTokenizer st = new StringTokenizer(data, delimiter);
							// 6.2.5. Lưu hàng sinh viên đó vào bảng tương ứng của nhóm trong database
							// staging
							System.out.println("count: " + st.countTokens());
							boolean check = addA_StudentOnTable(table_staging, valuesList, number_column, st);
							System.out.println("check: " + check);
							if (check == true) {
								// 6.2.6. Tăng số hàng đọc vào staging của nhóm lên 1
								count++;
							}
							// lay hang tiep theo len
							data = reader.readLine();
							System.out.println(data);
						} // end while row of file

						// 6.2.7. Đóng nguồn file
						reader.close();
						// update: cot staging_load_count trong logs
						System.out.println(
								"Thanh Cong:\t" + "file name: " + filename + " ==> So dong thanh cong: " + count);
						// 6.2.8. Kiểm tra sô dòng đọc được vào staging của file
						if (count > 0) {
							String sql2 = "UPDATE data_file_logs SET staging_load_count=" + count + ", "
									+ status_file_update + "'" + success + "', " + time_staing_update
									+ "now()  WHERE id=" + id;
							pre_control = conn.prepareStatement(sql2);
							pre_control.executeUpdate();

						} else {
							String sql2 = "UPDATE data_file_logs SET staging_load_count=" + count + ", "
									+ status_file_update + "'" + error + "'," + time_staing_update + "now() WHERE id="
									+ id;
							pre_control = conn.prepareStatement(sql2);
							pre_control.executeUpdate();
						}

					} catch (IOException e) {
						e.printStackTrace();
					}
				} // else file exist end

			} // end while file

			// dong
			re.close();
			pre_control.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// ghi 1 SV vao DB phai tuong ung voi tung table trong DB Staging
	public static boolean addA_StudentOnTable(String table_Staging, String valuesList, int number_column,
			StringTokenizer st) {
		// mo database
		int i = 0;
		if (st.countTokens() == number_column) {
			Connection conn = new GetConnection().getConnection("staging");
			try {

				PreparedStatement pre = conn.prepareStatement("insert into " + table_Staging + " " + valuesList);

				for (int j = 1; j <= number_column; j++) {
					String value = st.nextToken();
					pre.setNString(j, value);
				}

				i = pre.executeUpdate();

				// dong
				pre.close();
				conn.close();

			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}

		if (i > 0) {
			return true;
		} else {
			return false;
		}
	}

	// nhom 10 => file dinh dang ki cuc
	// nhom2_ca2 => cat file bang cai gi k nhan dang duoc
	// nhom6_ca2 : khong co file
	public static void main(String[] args) {
		new Staging().demo();

	}
}
