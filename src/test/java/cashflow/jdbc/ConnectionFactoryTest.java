package cashflow.jdbc;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import cashflow.datastore.ConnectionFactory;

public class ConnectionFactoryTest {
	@Test
	public void getConnection() throws SQLException, IOException {
		Connection con = ConnectionFactory.getConnection();
		assertNotNull(con);
		con.close();
	}

}
