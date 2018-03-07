package cashflow.queue;

import javax.jms.JMSException;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;


public class JMSReciever extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String brokerURL;
	private String queue;
	private JMSActiveMQReciever receiver;

	public JMSReciever(String brokerURL, String queue, StorageLevel storageLevel) {
		super(storageLevel);
		this.brokerURL = brokerURL;
		this.queue = queue;
		System.out.println("JMSReciever created");
	}

	@Override
	public void onStart() {
		receiver = JMSActiveMQReciever.getMessageReciever(this::onException, super::store, brokerURL, queue);
		while (receiver.hasError()) {
			receiver = JMSActiveMQReciever.getMessageReciever(this::onException, super::store, brokerURL, queue);
		}
		(new Thread(receiver)).start();
	}

	@Override
	public void onStop() {
		receiver.setDone();
	}

	public synchronized void onException(JMSException ex) {
		super.restart(ex.getMessage(), ex);
	}

}
