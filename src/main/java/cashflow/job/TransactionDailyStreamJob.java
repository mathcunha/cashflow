package cashflow.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cashflow.Transaction;
import cashflow.queue.JMSReciever;
import scala.Tuple2;

public class TransactionDailyStreamJob {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("TransactionDailyStreamJob");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));		
		JavaReceiverInputDStream<String> transactionsBody = jssc.receiverStream(new JMSReciever("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616", "ACCOUNT-QUEUE", StorageLevels.MEMORY_AND_DISK_SER_2));
		
		JavaDStream<Transaction> transactions = transactionsBody.map(Transaction::fromJson).repartition(2);
		transactions = transactions.map(t -> {
			t.setKey(String.format("%s_%s", t.getAccount(), t.getDate().toString()));
			return t;
		});
		JavaPairDStream<String, Float> tPair = transactions.mapToPair(t -> new Tuple2<>(t.getKey(), t.getValue()));
		JavaPairDStream<String, Float> reduced = tPair.reduceByKey((v1, v2) -> v1+v2); 
		
		reduced.print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
