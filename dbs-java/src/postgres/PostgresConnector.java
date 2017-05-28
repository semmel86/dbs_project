package postgres;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;

public class PostgresConnector {

	private String user;
	private String password;
	private String host;
	private String database;

	public PostgresConnector(String user, String password, String host, String database) {
		super();
		this.user = user;
		this.password = password;
		if (host == null)
			this.host = "localhost";
		else
			this.host = host;
		if (database == null)
			database = "";
		else
			this.database = database;
	}

	public Connection getConnection() {
		Connection connection = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("jdbc:postgresql://"); // JDBC -PSQLSetting
			sb.append(host);
			sb.append(":5432/");// Port Setting
			sb.append(database);
			connection = DriverManager.getConnection(sb.toString(), user, password);

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

}
