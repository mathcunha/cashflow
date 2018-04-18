package cashflow.datastore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionFactory {

	private static HikariDataSource ds;

	private static HikariDataSource getDS() throws IOException {
		if (ds == null) {
			Properties props = new Properties();
			props.load(ConnectionFactory.class.getResourceAsStream("/hikari.properties"));
			HikariConfig config = new HikariConfig(props);
			ds = new HikariDataSource(config);
		}
		return ds;
	}

	public static Connection getConnection() throws SQLException, IOException {
		return getDS().getConnection();
	}

}
