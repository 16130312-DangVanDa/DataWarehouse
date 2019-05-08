package model.database;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.net.ftp.FTPClient;

import model.connection.GetConnection;

public class DownloadFileFromFtpToLocal {

	public static void main(String[] args) {
		// ca2_nhom6: khong co file
		// ca2_nhom 4: azure k chay
		DownloadFileFromFtpToLocal.saveDataFromFTPToLocal();
	}

	// nhap vao ten file, IP FTP server, port, username, pass
	// local luu vao thu muc D:\Datawarehouse
	public static String downloadAFile(String local_download_dir, String remoteFilePath, String serverAddress, int port,
			String username, String password) {
		FTPClient ftpClient = new FTPClient();
		try {
			// 5. Kêt nối FTP Server
			ftpClient.connect(serverAddress, port);
			ftpClient.login(username, password);

			ftpClient.enterLocalPassiveMode();
			// tao file o local
			String substring = remoteFilePath.substring(remoteFilePath.lastIndexOf("/") + 1, remoteFilePath.length());
			// System.out.println(substring);
			// mo file dich
			File localfile = new File(local_download_dir + "\\" + substring);
			// System.out.println(localfile.getAbsolutePath());
			OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localfile));

			// 6.2.1. Gọi retrieveFile(remoteFilePath, outputStream) để download file
			ftpClient.retrieveFile(remoteFilePath, outputStream);// luon luon bang true
			outputStream.close();

			if (localfile.length() > 0) {
				return "TRUE";
			} else {
				return "FALSE";
			}

		} catch (IOException ex) {
			// loi ket noi
			System.out.println("Error occurs in downloading files from ftp Server : " + ex.getMessage());
			return "CONNECT";
		} finally {
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

	// ***** DOWNLOAD TAT CA CAC FILE CUA CAC NHOM VE LOCAL ********//
	public static void saveDataFromFTPToLocal() {
		Connection conn = null;
		PreparedStatement pre = null;

		try {
			// 1. Mở kết nối tới database chickennlu_Control_DB
			conn = new GetConnection().getConnection("control");

			pre = conn.prepareStatement("SELECT * FROM hard_code");
			ResultSet re_hardCode = pre.executeQuery();
			String success = null, error = null;
			String time_download_update = null, status_file_update = null, mail = null, password = null;
			password = "Da10a21998321123";
			
			while (re_hardCode.next()) {
				success = re_hardCode.getString("status_file_DOWNLOAD_sucsess");
				error = re_hardCode.getString("status_file_DOWNLOAD_ERROR");
				time_download_update = re_hardCode.getString("time_download_update");
				status_file_update = re_hardCode.getString("status_file_update");
				mail = re_hardCode.getString("mail");
				
			}

			// 2. Tìm các file có trạng thái là 'ready' tại các nhóm active
			String sql = "SELECT data_file_logs.id, ID_host, data_file_configuaration.group_session, source_folder, your_filename,"
					+ " data_file_configuaration.ip_address, "
					+ " data_file_configuaration.username, data_file_configuaration.password, "
					+ "data_file_configuaration.port, data_file_configuaration.download_to_dir_local "
					+ " FROM data_file_logs " + "JOIN data_file_configuaration ON data_file_logs.ID"
					+ "_host=data_file_configuaration.id where data_file_logs.status_file like 'ready"
					+ "' AND data_file_configuaration.isActive=1";
			pre = conn.prepareStatement(sql);
			// 3. Nhận được result set chứa các record thỏa yêu cầu truy xuất
			ResultSet re = pre.executeQuery();
			// 4. Duyệt record trong result set
			while (re.next()) { // reocrd?

				int idFile = re.getInt("id");
				int idHost = re.getInt("ID_host");

				String nameFile = re.getString("your_filename");
				String import_dir = re.getString("source_folder");// thu muc tren FTP
				String s = re.getString("group_session");
				String src_Server = re.getString("ip_address");
				String username = re.getString("username");
				String pass = re.getString("password");
				int port = re.getInt("port");
				String local_download_dir = re.getString("download_to_dir_local");// thu muc bo vao local
				String des = "D:\\" + local_download_dir;
				String source_dl = import_dir + nameFile;
				// bat dau download file ve
				String download = DownloadFileFromFtpToLocal.downloadAFile(des, source_dl, src_Server, port, username,
						pass);

				// bao thanh cong
				if (download.equalsIgnoreCase("TRUE")) {

					// 6.2.3.2.1. Thông báo thành công ra màn hình
					System.out.println("Dowload success file name: " + nameFile + " || idFile: " + idFile + " ||group: "
							+ s + " " + idHost);
					// 6.2.3.2.2. Cập nhật status_file là OK Download và thời gian download là thời
					// gian hiện tại
					pre = conn.prepareStatement("update data_file_logs set " + time_download_update + "now(), "
							+ status_file_update + "'" + success + "' where id=" + idFile);
					pre.executeUpdate();
				} else if (download.equalsIgnoreCase("FALSE")) {

					// 6.2.3.1.1. In dòng thông báo file Không tồn tại
					System.out.println("File không tồn tại, idFile: " + idFile + " ,group: " + idHost);
					// 6.2.3.1.2.Gửi mail thông báo lỗi download file
					sendMail("16130312@st.hcmuaf.edu.vn", "Data Warehouse",
							"DOWNLOAD FILE FAIL OF ID_FILE  " + idFile + " _GROUP: " + idHost, mail, password);
					// 6.2.3.1.3. Cập nhật status_file là ERROR Dowload và thời gian download llà
					// thời gian hiện tại
					pre = conn.prepareStatement("update data_file_logs set " + time_download_update + "now(),"
							+ status_file_update + "'" + error + "' where id=" + idFile);
					pre.executeUpdate();

				} else if (download.equalsIgnoreCase("CONNECT")) {
					// 6.1.1. In dòng thông báo lỗi kết nối ra màn hình
					System.out.println("LOI KET NOI");
					// 6.1.2.Gửi mail thông báo lỗi kết nối
					sendMail("16130312@st.hcmuaf.edu.vn", "Data Warehouse",
							"ERROR CONNECT ID_FILE " + idFile + " _GROUP: " + idHost, mail, password);
					// 6.1.3. Cập nhật trạng thái file là ERROR_CONNECT và thời gian download là
					// thời gian hiện tại
					pre = conn.prepareStatement("update data_file_logs set " + time_download_update + "now(), "
							+ status_file_update + "'ERROR_CONNECT' where id=" + idFile);
					pre.executeUpdate();
				}

			} // end while

			// 7. Đóng tất cả kế nối tới database chickennlu_Control_DB
			re.close();
			pre.close();
			conn.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	public static boolean sendMail(String to, String subject, String bodyMail, String mail, String pass) {
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(mail, pass);
			}
		});
		try {
			MimeMessage message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			message.setFrom(new InternetAddress("dangvanda.itnlu@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			message.setSubject(subject, "UTF-8");
			message.setText(bodyMail, "UTF-8");
			Transport.send(message);
		} catch (MessagingException e) {
			return false;
		}
		return true;
	}
	// gui mai thong bao khi downloaf khong thanh cong

}
