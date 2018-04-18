package cashflow.datastore;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

import cashflow.Aggs;
import scala.Tuple2;

public class TransactionDatastore {

	private static final Logger logger = Logger.getLogger(TransactionDatastore.class.getName());

	public static final String APPEND_TRANSACTION_SQL = "WITH upd AS (\r\n" + "	UPDATE Transactions SET \r\n"
			+ "		Value = Value+:value, Count = Count+:count \r\n"
			+ "		WHERE Account = :account AND Costumer = :costumer AND date = to_date(:date, 'YYYY/MM/DD') AND city = :city\r\n"
			+ "		RETURNING *\r\n" + ")\r\n"
			+ "INSERT INTO Transactions (date, costumer, account, value, count, city) \r\n"
			+ "	select to_date(:date, 'YYYY/MM/DD'), :costumer, :account, :value, :count, :city WHERE NOT EXISTS (SELECT * FROM upd)";

	public static Boolean appendTransaction(Iterator<Tuple2<String, Aggs>> rddPartition) {
		Connection conn = null;
		PreparedBatch batch = null;
		try {
			conn = ConnectionFactory.getConnection();

			Handle handle = Jdbi.open(conn);
			batch = handle.prepareBatch(TransactionDatastore.APPEND_TRANSACTION_SQL);
			boolean hasUpdate = false;
			while (rddPartition.hasNext()) {
				hasUpdate = true;
				Tuple2<String, Aggs> tuple = rddPartition.next();
				String[] key = tuple._1.split("_");
				batch.bind("date", key[1]);
				batch.bind("account", key[0]);
				batch.bind("city", "Fortaleza");
				batch.bind("costumer", "Math");
				batch.bind("value", tuple._2.sum);
				batch.bind("count", tuple._2.count);
			}
			if (hasUpdate) {
				int[] retorno = batch.execute();
				String strRetorno = Arrays.asList(retorno).stream().collect(Collectors.toList()).toString();
				logger.info("batch result:" + strRetorno);
			}
			return Boolean.TRUE;
		} catch (Throwable e) {
			if (batch != null) {
				//TODO print statements logger.info(batch.toString());
			}
			logger.log(Level.SEVERE, "error persisting transactions ", e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.log(Level.SEVERE, "error closing connection", e);
				}
			}
		}

		return Boolean.FALSE;
	}
}
