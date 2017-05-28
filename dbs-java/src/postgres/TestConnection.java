package postgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestConnection {


	public static void main(String[] argv) throws SQLException {

		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");

// Get Connection to the Database (logic in Class PostgresConnector for reuseability)
		PostgresConnector pc= new PostgresConnector("election","elect2016","178.254.35.26","Election");
		Connection connection = pc.getConnection();

		// try the query
		if (connection != null) {
			
			PreparedStatement st = connection.prepareStatement("SELECT * FROM twitteraccount WHERE handle like 'HillaryClinton';");
			
			ResultSet rs = st..executeQuery();
			while (rs.next())
			{
//				System.out.println(rs.);
			    System.out.println("Column 1 returned :");
			    System.out.println(rs.getInt(1)+", "+rs.getString(2));
			}
			rs.close();
			st.close();
			
		} else {
			System.out.println("Cannot connect!");
		}
	}

}
