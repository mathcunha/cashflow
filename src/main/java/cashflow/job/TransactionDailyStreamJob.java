package cashflow.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cashflow.Transaction;
import cashflow.queue.JMSReciever;

public class TransactionDailyStreamJob {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("TransactionDailyStreamJob");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));		
		JavaReceiverInputDStream<String> transactions = jssc.receiverStream(new JMSReciever("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616", "TEST-QUEUE", StorageLevels.MEMORY_AND_DISK_SER_2));
		transactions.map(Transaction::fromJson).print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
