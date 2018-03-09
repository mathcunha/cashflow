package cashflow.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cashflow.Transaction;
import receiver.jms.JMSReciever;

public class PrintTransactionStreamJob {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("PrintTransactionStreamJob");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));		
		//JavaReceiverInputDStream<String> transactions = jssc.receiverStream(new JMSReciever("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616", "TEST-QUEUE", StorageLevels.MEMORY_AND_DISK_SER_2));
		JavaReceiverInputDStream<String> transactions = jssc.receiverStream(JMSReciever.buildJMSActiveMQReciever(loadProperties("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616", "TEST-QUEUE"), StorageLevels.MEMORY_AND_DISK_SER_2));
		transactions.map(Transaction::fromJson).print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
	
	public static Map<String, String> loadProperties(String brokerURL, String queue){
		Map<String, String> map = new HashMap<>();
		map.put("brokerURL", brokerURL);
		map.put("queue", queue);
		map.put("user", "admin");
		map.put("password", "admin");		
		return map;
	}
	
}
