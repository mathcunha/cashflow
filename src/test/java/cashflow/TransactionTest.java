package cashflow;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class TransactionTest {
	@Test
	public void loadTransactionFromJson() {
		Transaction t  = Transaction.fromJson("{\r\n" + 
				"  \"date\": \"2018-02-20\",\r\n" + 
				"  \"costumer\": \"costumer_1\",\r\n" + 
				"  \"account\": \"12345\",\r\n" + 
				"  \"city\": \"Santa Barbara\",\r\n" + 
				"  \"value\": 1234\r\n" + 
				"}");
		assertNotNull(t);
	}

}
