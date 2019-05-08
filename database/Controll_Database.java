package model.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import model.connection.GetConnection;

public class Controll_Database {

	public boolean insert(int ID_host, String source_folder, String your_filename, String status_file, String delimiter,
			int total_row) {
		int i = 0;
		try {
			Connection conn = new GetConnection().getConnection("control");
			String sql = "insert into data_file_logs (ID_host, source_folder, your_filename, status_file, time_update,delimiter ,total_row) values(?,?,?,?,now(),?,?)";
			PreparedStatement pre = conn.prepareStatement(sql);
			pre.setInt(1, ID_host);
			pre.setString(2, source_folder);
			pre.setString(3, your_filename);
			pre.setString(4, status_file);
			pre.setString(5, delimiter);
			pre.setInt(6, total_row);
			i = pre.executeUpdate();

			// dong
			pre.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (i > 0) {
			return true;
		}

		return false;
	}

	public static void main(String[] args) {
		Controll_Database control = new Controll_Database();
		System.out.println(control.insert(5, "", "file2.csv", "ER", ",", 2));
	}

}
