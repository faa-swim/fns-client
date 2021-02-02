package us.dot.faa.swim.fns;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.stream.Collectors;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import us.dot.faa.swim.fns.fil.FilClient;
import us.dot.faa.swim.fns.jms.FnsJmsMessageProcessor;
import us.dot.faa.swim.fns.notamdb.NotamDb;
import us.dot.faa.swim.fns.rest.FnsRestApi;
import us.dot.faa.swim.jms.JmsClient;
import us.dot.faa.swim.utilities.MissedMessageTracker;

public class FnsClient implements ExceptionListener {
	private static Logger logger = LoggerFactory.getLogger(FnsClient.class);
	private static FilClient filClient = null;
	private static JmsClient jmsClient = null;
	private static NotamDb notamDb = null;
	private static FnsJmsMessageProcessor fnsJmsMessageProcessor = null;
	private static MissedMessageTracker missedMessageTracker = null;
	private static FnsClientConfig fnsClientConfig;
	private static boolean isRunning;
	private static Instant lastValidationCheckTime = Instant.now();

	private static MissedMessageTracker createMissedMessagTracker() {
		return new MissedMessageTracker(fnsClientConfig.getMissedMessageTrackerScheduleRate(),
				fnsClientConfig.getMissedMessageTriggerTime(), fnsClientConfig.getStaleMessageTriggerTime()) {
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
					} else if (notamDb.isInitializing()) {
						notamDb.setMissedMessageDuringInitialization();
					}
				} catch (Exception e) {
					logger.error("Failed to ReInitialize NotamDb due to: " + e.getMessage(), e);
				}
			}

			@Override
			public void onStale(Long lastRecievedId, Instant lastRecievedTime) {
				logger.warn("Have not recieved a JMS message in " + this.getStaleMessageTriggerTimeInMinutes()
						+ " minutes, last message recieved at " + lastRecievedTime
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

	}

	private void connectJmsClient() {

		logger.info("Starting JMS Consumer");
		boolean jmsConsumerStarted = false;

		while (!jmsConsumerStarted) {
			try {
				jmsClient = new JmsClient(fnsClientConfig.getJmsClientConfig());
				jmsClient.connect(fnsClientConfig.getJmsConnectionFactoryName(), this);
				jmsClient.createConsumer(fnsClientConfig.getJmsDestination(), fnsJmsMessageProcessor,
						Session.AUTO_ACKNOWLEDGE);
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

	private static void initalizeNotamDbFromFil() {
		logger.info("Initalizing Database");
		missedMessageTracker.clearOnlyMissedMessages();

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

	public void start() throws InterruptedException {

		logger.info("Starting FnsClient");

		missedMessageTracker.start();
		fnsJmsMessageProcessor = new FnsJmsMessageProcessor(notamDb, fnsClientConfig.getJmsProcessingThreads());
		fnsJmsMessageProcessor.setMissedMessageTracker(missedMessageTracker);

		connectJmsClient();

		Thread.sleep(30 * 1000);

		initalizeNotamDbFromFil();

		FnsRestApi fnsRestApi = null;
		if (fnsClientConfig.getRestApiIsEnabled()) {
			logger.info("Starting REST API");
			fnsRestApi = new FnsRestApi(notamDb, fnsClientConfig.getRestApiPort());
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
			} else if (!notamDb.isInitializing()) {
				initalizeNotamDbFromFil();
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

	public void stop() {
		logger.info("Stopping FnsClient");
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

	public static void main(final String[] args) throws InterruptedException {

		logger.info("Loading FnsClient Config and Initalizing");

		Config typeSafeConfig;
		// load FnsClient Config
		if (Files.exists(Paths.get("fnsClient.conf"))) {
			typeSafeConfig = ConfigFactory.parseFile(new File("fnsClient.conf"));
		} else {
			typeSafeConfig = ConfigFactory.load();
		}

		fnsClientConfig = new FnsClientConfig(typeSafeConfig);

		// create FilClient
		try {
			filClient = new FilClient(fnsClientConfig.getFilClientConfig());
		} catch (JSchException e) {
			logger.error("Failed to initalize FIL Client", e);
			System.exit(1);
		}

		// create JmsClient
		try {
			jmsClient = new JmsClient(fnsClientConfig.jmsClientConfig);
		} catch (NamingException e) {
			logger.error("Failed to create JMS Client", e);
			System.exit(1);
		}

		// create NotamDb
		try {
			notamDb = new NotamDb(fnsClientConfig.notamDbConfig);
		} catch (Exception e) {
			logger.error("Failed to create NotamDB", e);
			System.exit(1);
		}

		// create MissedMessageTracker
		missedMessageTracker = createMissedMessagTracker();

		// start FnsClient
		final FnsClient fnsClient = new FnsClient();
		try {
			fnsClient.start();
		} catch (InterruptedException e) {
			throw e;
		} finally {
			fnsClient.stop();
		}
	}
}
