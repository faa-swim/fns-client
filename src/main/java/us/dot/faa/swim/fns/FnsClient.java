package us.dot.faa.swim.fns;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Hashtable;
import java.util.Map;
import java.util.stream.Collectors;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;

import us.dot.faa.swim.jms.JmsClient;
import us.dot.faa.swim.utilities.MissedMessageTracker;

public class FnsClient implements ExceptionListener {
	private static Logger logger = LoggerFactory.getLogger(FnsClient.class);
	private static FilClient filClient = null;
	private static JmsClient jmsClient = null;
	private static NotamDb notamDb = new NotamDb();
	private static FnsJmsMessageProcessor fnsJmsMessageProcessor = new FnsJmsMessageProcessor(notamDb);
	private static MissedMessageTracker missedMessageTracker = null;
	private static Hashtable<String, Object> jndiProperties;
	private static String jmsConnectionFactoryName = "";
	private static String jmsQueueName = "";
	private static Config config;
	private static boolean isRunning;
	private static Instant lastValidationCheckTime = Instant.now();

	public static void main(final String[] args) throws InterruptedException {

		config = ConfigFactory.parseFile(new File(
				"fnsClient.conf"));

		jmsConnectionFactoryName = config.getString("jms.connectionFactory");

		try {
			filClient = new FilClient(config.getString("fil.sftp.host"), config.getString("fil.sftp.username"),
					config.getString("fil.sftp.certFilePath"));
			filClient.setFilFileSavePath("");

			final FnsClient fnsClient = new FnsClient();
			try {
				fnsClient.start();
			} catch (InterruptedException e) {
				throw e;
			} finally {
				fnsClient.stop();
			}
		} catch (JSchException e1) {
			logger.error("Failed to initalize FIL Client", e1);
		}
	}

	public void start() throws InterruptedException {

		logger.info("Starting FnsClient v1.0");

		jndiProperties = new Hashtable<>();
		for (final Object jndiPropsObject : config.getList("jms.jndiProperties").toArray()) {
			final ConfigList jndiProps = (ConfigList) jndiPropsObject;
			jndiProperties.put(jndiProps.get(0).render().toString().replace("\"", ""),
					jndiProps.get(1).render().toString().replace("\"", ""));
		}

		jmsQueueName = config.getString("jms.destination");
		
		missedMessageTracker = new MissedMessageTracker() {
			@Override
			public void onMissed(Map<Long, Instant> missedMessages) {
				String cachedCorellationIds = missedMessages.entrySet().stream()
						.map(kvp -> kvp.getKey() + ":" + missedMessages.get(kvp.getKey()))
						.collect(Collectors.joining(", ", "{", "}"));

				logger.warn(
						"Missed Message Identified, setting NotamDb to Invalid and ReInitalizing from FNS Initial Load | Missed Messages "
								+ cachedCorellationIds);

				try {
					if (notamDb.isValid()) {
						notamDb.setInvalid();
						initalizeNotamDbFromFil();
					}
				} catch (Exception e) {
					logger.error("Failed to ReInitialize NotamDb due to: " + e.getMessage(), e);
				}
			}
			
			@Override
			public void onStale(Long lastRecievedId, Instant lastRecievedTime)
			{
				logger.warn("Have not recieved a JMS message in "
						+ config.getInt("fnsClient.messageTracker.staleMessageTriggerTime")
						+ " minutes, last message recieved at "
						+ lastRecievedTime
						+ " Setting NotamDb to Invalid and ReInitalizing from FNS Initial Load");

				try {
					if (notamDb.isValid()) {
						notamDb.setInvalid();
						initalizeNotamDbFromFil();
					}
				} catch (Exception e) {
					logger.error("Failed to ReInitialize NotamDb due to: " + e.getMessage(), e);
				}
			}
		};
		
		missedMessageTracker.start();
		fnsJmsMessageProcessor.setMissedMessageTracker(missedMessageTracker);

		connectJmsClient();

		Thread.sleep(30 * 1000);		
		
		initalizeNotamDbFromFil();

		FnsRestApi fnsRestApi = null;
		if (config.getBoolean("restapi.enabled")) {
			logger.info("Starting REST API");
			fnsRestApi = new FnsRestApi(notamDb, config.getInt("restapi.port"));
		}

		isRunning = true;

		while (isRunning) {			
			Thread.sleep(15 * 1000);
			if (notamDb.isValid()) {				

				 if (lastValidationCheckTime.atZone(ZoneId.systemDefault()).getDayOfWeek() != Instant.now()
						.atZone(ZoneId.systemDefault()).getDayOfWeek()) {

					logger.info("Performing database validation check against FIL");
					try {

						if (validateDatabase()) {
							logger.info("Database validated against FIL");
						} else {
							logger.warn("Validation with Notam Database Failed, setting NotamDB to invalid");
							notamDb.setInvalid();
						}

					} catch (Exception e) {
						logger.error("Failed to validate database due to: " + e.getMessage()
								+ ", setting NotamDB to invalid", e);
						notamDb.setInvalid();
					}

					logger.info("Removing old NOTAMS from database");
					try {
						int notamsRemoved = notamDb.removeOldNotams();
						logger.info("Removed " + notamsRemoved + " Notams");
					} catch (final Exception e) {
						logger.error(
								"Failed to remove old notams from database due to: " + e.getMessage() + ", Closing", e);
					}
				}
			}
		}

		if (jmsClient != null) {
			logger.info("Destroying JmsClient");
			try {

				jmsClient.close();
			} catch (final Exception e) {
				logger.error("Unable to destroy JmsClient due to: " + e.getMessage() + ", Closing", e);
			}
		}

		if (fnsRestApi != null) {
			logger.info("Stopping REST API");
			fnsRestApi.terminate();
		}
	}

	private void connectJmsClient() {

		logger.info("Starting JMS Consumer");
		boolean jmsConsumerStarted = false;

		while (!jmsConsumerStarted) {
			try {
				jmsClient = new JmsClient(jndiProperties);
				jmsClient.connect(jmsConnectionFactoryName, this);
				jmsClient.createConsumer(jmsQueueName, fnsJmsMessageProcessor, Session.AUTO_ACKNOWLEDGE);

				jmsConsumerStarted = true;

			} catch (final Exception e) {
				logger.error("JmsClient failed to start due to: " + e.getMessage(), e);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					logger.warn("Thread interupded");
				}
			}
		}
		logger.info("JMS Consumer Started");
	}

	private void initalizeNotamDbFromFil() {
		logger.info("Initalizing Database");
		missedMessageTracker.clearAllMessages();

		boolean successful = false;
		while (!successful) {
			try {
				filClient.connectToFil();
				notamDb.initalizeNotamDb(filClient.getFnsInitialLoad());
				successful = true;
			} catch (Exception e) {
				logger.error("Failed to Initialized NotamDb due to: " + e.getMessage(), e);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					logger.warn("Thread interupded");
				}
			} finally {
				filClient.close();
			}
		}
	}

	private boolean validateDatabase() throws Exception {
		try {
			filClient.connectToFil();

			Map<String, Timestamp> missMatchedMap = notamDb.validateDatabase(filClient.getFnsInitialLoad());
			if (!missMatchedMap.isEmpty()) {

				String missMatches = missMatchedMap.keySet().stream().map(key -> key + ":" + missMatchedMap.get(key))
						.collect(Collectors.joining(", ", "{", "}"));

				logger.debug("Missing NOTAMs: " + missMatches);
				return false;
			} else {				
				return true;
			}
		} catch (SQLException | ParserConfigurationException | IOException | SAXException | SftpException
				| ParseException | InterruptedException e) {
			throw e;
		} finally {
			lastValidationCheckTime = Instant.now();
			filClient.close();
		}
	}

	public void stop() {
		isRunning = false;
	}

	@Override
	public void onException(final JMSException e) {
		logger.error("JmsClient Failure due to : " + e.getMessage() + ". Resarting JmsClient", e);
		try {
			jmsClient.close();
		} catch (final Exception e1) {
			logger.error(
					"Failed to JmsClient JmsClient due to : " + e1.getMessage() + ". Continuing with JmsClient Restart",
					e1);
		}

		connectJmsClient();
	}

}
