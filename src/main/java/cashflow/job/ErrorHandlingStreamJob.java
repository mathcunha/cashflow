package cashflow.job;

import java.io.Serializable;
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

import cashflow.Transaction;
import receiver.jms.JMSReciever;
import scala.Tuple2;

public class ErrorHandlingStreamJob{
	
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("ErrorHandlingStreamJOB");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
		LongAccumulator errors = jssc.ssc().sc().longAccumulator();
		errors.add(0);
		
		// JavaReceiverInputDStream<String> transactionsBody = jssc.receiverStream(new
		// JMSReciever("tcp://" + System.getenv("ACTIVEMQ_IP") + ":61616",
		// "ACCOUNT-QUEUE", StorageLevels.MEMORY_AND_DISK_SER_2));
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

		Runnable abortMethod = () -> {
			while (errors.value() == 0) {
				try {
					System.out.println("ErrorHandlingStreamJOB erros:0");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("ErrorHandlingStreamJOB CALLING CLOSE "+errors.value());
			jssc.close();
		};
		new Thread(abortMethod).start();
		
		reduced.foreachRDD(rdd -> {
			rdd.foreachPartition(rddPartition -> {
				if (rddPartition.hasNext()) {
					// simulating an error during saving process
					System.out.println("ErrorHandlingStreamJOB ABORTING MESSAGE");
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

	public static class Aggs implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		Float sum;
		Integer count;

		public Aggs(Integer count, Float sum) {
			this.sum = sum;
			this.count = count;
		}

		public Aggs add(Aggs v) {
			return new Aggs(count + v.count, sum + v.sum);
		}

		public String toString() {
			return String.format("{\"count\":%d, \"sum\":%f}", count, sum);
		}
	}
}
