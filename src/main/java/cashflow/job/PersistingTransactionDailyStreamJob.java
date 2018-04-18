package cashflow.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

import cashflow.Aggs;
import cashflow.Transaction;
import cashflow.datastore.TransactionDatastore;
import receiver.jms.JMSReciever;
import scala.Tuple2;

public class PersistingTransactionDailyStreamJob {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("PersistingTransactionDailyStreamJob");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
		LongAccumulator errors = jssc.ssc().sc().longAccumulator();
		errors.add(0);

		JavaReceiverInputDStream<String> transactionsBody = jssc.receiverStream(JMSReciever.buildJMSActiveMQReciever(
				loadProperties("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616", "ACCOUNT-QUEUE"),
				StorageLevels.MEMORY_AND_DISK_SER_2));

		JavaDStream<Transaction> transactions = transactionsBody.map(Transaction::fromJson).repartition(2);
		transactions = transactions.map(t -> {
			t.setKey(String.format("%s_%s", t.getAccount(), t.getDate().toString()));
			return t;
		});
		JavaPairDStream<String, Aggs> tPair = transactions
				.mapToPair(t -> new Tuple2<>(t.getKey(), new Aggs(1, t.getValue())));
		JavaPairDStream<String, Aggs> reduced = tPair.reduceByKey(Aggs::add);

		new Thread(() -> {
			while (errors.value() == 0) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			jssc.close();
		}, "abort").start();

		reduced.foreachRDD(rdd -> {
			rdd.foreachPartition(rddPartition -> {
				if (!TransactionDatastore.appendTransaction(rddPartition)) {
					errors.add(1);					
				}
			});
		});

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}

	public static Map<String, String> loadProperties(String brokerURL, String queue) {
		Map<String, String> map = new HashMap<>();
		map.put("brokerURL", brokerURL);
		map.put("queue", queue);
		map.put("user", "admin");
		map.put("password", "admin");
		return map;
	}
}
