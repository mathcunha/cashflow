package cashflow.queue;

import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JMSActiveMQReciever implements Runnable {

	private Boolean done = Boolean.FALSE;
	private Boolean error = Boolean.FALSE;
	private Logger logger = Logger.getLogger(getClass().getName());
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageConsumer consumer;
	private Consumer<String> store;

	public static JMSActiveMQReciever getMessageReciever(ExceptionListener listener, Consumer<String> store,
			String brokerURL, String queue) {
		return new JMSActiveMQReciever(listener, store, brokerURL, queue);
	}

	private JMSActiveMQReciever(ExceptionListener listener, Consumer<String> store, String brokerURL, String queue) {
		this.store = store;
		logger.info("Creating MessageReciever for broker " + brokerURL + " and queue " + queue);
		// Create a ConnectionFactory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", brokerURL);
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			connection.setExceptionListener(listener);
		} catch (JMSException e) {
			setError("error starting connection", e);
		}

		// Create a Session
		try {
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		} catch (JMSException e) {
			setError("error starting session", e);
			try {
				connection.close();
			} catch (JMSException e1) {
				setError("error closing connection", e1);
			}
		}

		try {
			// Create the destination (Topic or Queue)
			destination = session.createQueue(queue);
			// Create a MessageConsumer from the Session to the Topic or Queue
			consumer = session.createConsumer(destination);
		} catch (JMSException e) {
			setError("error starting destination/consumer", e);
			try {
				session.close();
			} catch (JMSException e1) {
				setError("error closing session", e1);
			}
			try {
				connection.close();
			} catch (JMSException e1) {
				setError("error closing connection", e1);
			}
		}
	}

	@Override
	public void run() {
		while (!done && !error) {
			try {
				logger.info("waiting message");
				Message message = consumer.receive();

				if (message instanceof TextMessage) {
					TextMessage textMessage = (TextMessage) message;
					String text = textMessage.getText();
					store.accept(text);
					logger.info("Received: " + text);
				} else {
					logger.info("Received: " + message);
				}
				message.acknowledge();
			} catch (JMSException e) {
				setError("Error receiving message", e);
			}
		}
	}

	public void setDone() {
		this.done = Boolean.TRUE;
		try {
			consumer.close();
		} catch (JMSException e) {
			setError("error closing consumer", e);
		}
		try {
			session.close();
		} catch (JMSException e1) {
			setError("error closing session", e1);
		}
		try {
			connection.close();
		} catch (JMSException e1) {
			setError("error closing connection", e1);
		}
	}

	public void setError(String message, Exception ex) {
		this.error = Boolean.TRUE;
		logger.log(Level.SEVERE, message, ex);
	}

	public Boolean hasError() {
		return this.error;
	}

}
