package us.dot.faa.swim.fns;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.jcraft.jsch.SftpException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import us.dot.faa.swim.fns.FnsMessage.FnsMessageParseException;
import us.dot.faa.swim.fns.FnsMessage.NotamStatus;
import us.dot.faa.swim.fns.fil.FilClient;
import us.dot.faa.swim.fns.fil.FilParser;
import us.dot.faa.swim.fns.fil.FilParserWorker;
import us.dot.faa.swim.fns.jms.FnsJmsMessageWorker;
import us.dot.faa.swim.fns.notamdb.NotamDb;
import us.dot.faa.swim.fns.rest.FnsRestApi;
import us.dot.faa.swim.jms.JmsClient;
import us.dot.faa.swim.jms.JmsMessageProcessor;
import us.dot.faa.swim.utilities.MissedMessageTracker;

public class FnsClient implements ExceptionListener {
	private static Logger logger = LoggerFactory.getLogger(FnsClient.class);
	private final static CountDownLatch latch = new CountDownLatch(1);

	private final FnsClientConfig config;
	private final FilClient filClient;
	private final JmsClient jmsClient;
	private final FnsJmsMessageWorker fnsJmsMessageWorker;
	private final NotamDb notamDb;
	private final Timer removeOldNotamsTimer = new Timer();
	private final MissedMessageTracker missedMessageTracker;

	private FnsRestApi fnsRestApi;
	private JmsMessageProcessor fnsJmsProcessor;
	private boolean missedMessageDuringInitialization = false;

	public Queue<FnsMessage> pendingJmsMessages = new ConcurrentLinkedQueue<FnsMessage>();

	public FnsClient(FnsClientConfig config) throws Exception {
		this.config = config;

		filClient = new FilClient(config.getFilClientConfig());
		jmsClient = new JmsClient(config.jmsClientConfig);
		notamDb = new NotamDb(config.notamDbConfig);

		if (config.removeOldNotams) {
			final TimerTask removeOldNotamsTimerTask = new TimerTask() {

				@Override
				public void run() {
					try {
						removeOldNotams();
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}
			};
			Timer removeOldNotamsTimer = new Timer(true);
			long removeNotamScheduleFrequencyInMilliseconds = TimeUnit.MILLISECONDS
					.convert(config.getRemoveOldNotamsFrequency(), TimeUnit.HOURS);
			removeOldNotamsTimer.scheduleAtFixedRate(removeOldNotamsTimerTask,
					removeNotamScheduleFrequencyInMilliseconds, removeNotamScheduleFrequencyInMilliseconds);
		}

		missedMessageTracker = createMissedMessagTracker();
		fnsJmsMessageWorker = new FnsJmsMessageWorker(notamDb, pendingJmsMessages);
		fnsJmsMessageWorker.setMissedMessageTracker(missedMessageTracker);

	}

	private MissedMessageTracker createMissedMessagTracker() {
		return new MissedMessageTracker(config.getMissedMessageTrackerScheduleRate(),
				config.getMissedMessageTriggerTime(), config.getStaleMessageTriggerTime()) {
			@Override
			public void onMissed(Map<Long, Instant> missedMessages) {
				String cachedCorellationIds = missedMessages.entrySet().stream()
						.map(kvp -> kvp.getKey() + ":" + missedMessages.get(kvp.getKey()))
						.collect(Collectors.joining(", ", "{", "}"));

				logger.warn(
						"Missed Message Identified, setting NotamDb to Invalid and ReInitalizing from FNS Initial Load | Missed Messages "
								+ cachedCorellationIds);

				this.clearOnlyMissedMessages();
				try {
					if (notamDb.isValid()) {
						notamDb.setInvalid();
						initalizeNotamDbFromFil();
					} else if (notamDb.isInitializing()) {
						missedMessageDuringInitialization = true;
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
				jmsClient.connect(config.getJmsConnectionFactoryName(), this);
				fnsJmsProcessor = new JmsMessageProcessor(
						jmsClient.createConsumer(config.getJmsDestination(), Session.CLIENT_ACKNOWLEDGE),
						config.jmsProcessingThreads, 100, fnsJmsMessageWorker);
				fnsJmsProcessor.start();

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

	private void initalizeNotamDbFromFil() throws InterruptedException {
		missedMessageDuringInitialization = false;
		logger.info("Initalizing Database");
		missedMessageTracker.clearOnlyMissedMessages();
		Date refDate = new Date(System.currentTimeMillis());

		boolean successful = false;
		while (!successful) {
			try {
				filClient.connectToFil();

				if (notamDb.isInitializing()) {
					return;
				} else {
					this.missedMessageDuringInitialization = false;
					notamDb.setInitializing(true);
				}

				InputStream filFileInputStream = null;

				try {

					notamDb.dropNotamTable();
					notamDb.createNotamTable();

					logger.info("Initizliaing NotamDb from FIL File");

					filFileInputStream = filClient.getFnsInitialLoad(refDate);
					final int notamCount = loadNotams(filFileInputStream);

					if (!this.missedMessageDuringInitialization) {						
						logger.info("Loaded " + notamCount + " Notams");

						loadQueuedMessages();

						notamDb.setValid();
						logger.info("NotamDb initalized");	
						successful = true;
					} else {
						logger.error(
								"NotamDb initalization failed due to missed message identified during initalization process.");
					}
				} catch (SQLException | IOException | SAXException | ParserConfigurationException sqle) {
					throw sqle;
				} finally {
					notamDb.setInitializing(false);
					try {						
						if (filFileInputStream != null) {
							filFileInputStream.close();
						}
					} catch (IOException ioe) {
						logger.error(ioe.getMessage(), ioe);
					}
				}
				
			} catch (Exception e) {
				logger.error("Failed to Initialized NotamDb due to: " + e.getMessage(), e);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					logger.warn("Thread interupded");
					throw e1;
				}
			} finally {
				filClient.close();
			}
		}
	}

	private void loadQueuedMessages() throws SQLException {

		logger.info("Loading " + pendingJmsMessages.size() + " queued notams");

		// update with pending
		FnsMessage messageToProcesses = pendingJmsMessages.poll();

		while (messageToProcesses != null) {
			if (notamDb.checkIfNotamIsNewer(messageToProcesses)) {
				notamDb.putNotam(messageToProcesses);
			} else {
				logger.debug("NOTAM with FNS_ID:" + messageToProcesses.getFNS_ID() + " and CorrelationId: "
						+ messageToProcesses.getCorrelationId() + " and LastUpdateTime: "
						+ messageToProcesses.getUpdatedTimestamp().toString()
						+ " discarded due to Notam in database has newer LastUpdateTime");
			}
			messageToProcesses = pendingJmsMessages.poll();
		}

		logger.info("Queued notams loaded");

	}

	private int loadNotams(InputStream inputStream) throws Exception {

		notamDb.setInitializing(true);
		AtomicInteger notamCount = new AtomicInteger();

		final FilParser parser = new FilParser(config.getFilParserThreadCount(), config.getFilParserMaxWorkQueueSize());
		parser.parseFilFile(inputStream, new FilParserWorker() {

			@Override
			public void processesMessage(String aixmMessage) {

				try {
					final FnsMessage fnsMessage = new FnsMessage((long) -1, aixmMessage);
					fnsMessage.setStatus(NotamStatus.ACTIVE);
					notamDb.putNotam(fnsMessage);
					notamCount.incrementAndGet();

				} catch (FnsMessageParseException | SQLException e) {
					throw new RuntimeException(e);
				}
			}
		});

		notamDb.setInitializing(false);

		return notamCount.get();
	}

	public boolean validateNotamDb() throws Exception {

		logger.info("Validating NotamDb");
		Map<String, Timestamp> missingNotamsMap = new HashMap<String, Timestamp>();

		try {
			filClient.connectToFil();

			logger.info("Generating Validation Map from FIL");
			final Map<String, Timestamp> filValidationMap = new ConcurrentHashMap<String, Timestamp>();
			new FilParser(config.filParserThreadCount, config.filParserWorkQueueSize).parseFilFile(
					filClient.getFnsInitialLoad(new Date(System.currentTimeMillis())), new FilParserWorker() {

						@Override
						public void processesMessage(String aixmMessage) {
							try {
								final FnsMessage fnsMessage = new FnsMessage((long) -1, aixmMessage);
								filValidationMap.put(String.valueOf(fnsMessage.getFNS_ID()),
										fnsMessage.getUpdatedTimestamp());
							} catch (FnsMessageParseException e) {
								logger.error("Failed to create FnsMessage due to: " + e.getMessage(), e);
								throw new RuntimeException(e);
							}
						}
					});

			logger.info("Getting Validation Map from NotamDb");
			Map<String, Timestamp> notamDbValidationMap = notamDb.getValidationMap();
			for (Map.Entry<String, Timestamp> entry : filValidationMap.entrySet()) {
				Timestamp dbUpdateTime = notamDbValidationMap.get(entry.getKey());
				if (dbUpdateTime == null) {
					missingNotamsMap.put("Missing-" + entry.getKey(), entry.getValue());
				} else {
					if (!entry.getValue().equals(dbUpdateTime) && !entry.getValue().before(dbUpdateTime)) {
						missingNotamsMap.put("Newer-" + entry.getKey(), entry.getValue());
					}
				}
			}

			if (!missingNotamsMap.isEmpty()) {

				logger.warn("NotamDb Validation Failed");

				String missMatches = missingNotamsMap.keySet().stream()
						.map(key -> key + ":" + missingNotamsMap.get(key)).collect(Collectors.joining(", ", "{", "}"));

				logger.debug("Missing NOTAMs: " + missMatches);
				return false;
			} else {
				logger.info("NotamDb Validation Passed");
				return true;
			}
		} catch (SQLException | ParserConfigurationException | IOException | SAXException | SftpException
				| ParseException | InterruptedException e) {
			throw e;
		} finally {
			filClient.close();
		}
	}

	public void start() throws SQLException, InterruptedException {

		logger.info("Starting FnsClient");

		boolean notamTableExists = notamDb.notamTableExists();
		if (!notamTableExists) {
			notamDb.createNotamTable();
		}

		AbstractMap.SimpleEntry<Long, Instant> lastCorrelationId = notamDb.getLastCorrelationId();
		if (lastCorrelationId != null && lastCorrelationId.getKey() > 0
				&& Duration.between(lastCorrelationId.getValue(), Instant.now())
						.toMinutes() < config.missedMessageTriggerTime - 1) {
			notamDb.setValid();
			missedMessageTracker.setLastRecievedTrackingId(lastCorrelationId.getKey());
		} else {
			lastCorrelationId = null;
		}

		missedMessageTracker.start();
		connectJmsClient();

		if (lastCorrelationId == null) {
			logger.info("Recent Correlation Id not found in NotamDb, starting NotamDb initalization from FIL");
			Thread.sleep(60 * 1000);

			initalizeNotamDbFromFil();

		} else {
			logger.info("Recent Correlation Id Imported from NotamDb, skipping initialization");
		}

		if (config.getRestApiIsEnabled()) {
			logger.info("Starting REST API");
			fnsRestApi = new FnsRestApi(notamDb, config.getRestApiPort());
		}

	}

	public void stop() {
		logger.info("Stopping FnsClient");
		removeOldNotamsTimer.cancel();

		if (jmsClient != null) {
			logger.info("Destroying JmsClient");
			try {

				fnsJmsProcessor.stop();
				jmsClient.close();
			} catch (final Exception e) {
				logger.error("Unable to destroy JmsClient due to: " + e.getMessage(), e);
			}
		}

		if (fnsRestApi != null) {
			logger.info("Stopping REST API");
			fnsRestApi.terminate();
		}
	}

	public int removeOldNotams() throws SQLException {
		logger.info("Removing old NOTAMS from database");
		int notamsRemoved = 0;
		try {
			notamsRemoved = notamDb.removeOldNotams();
			logger.info("Removed " + notamsRemoved + " Notams");
			return notamsRemoved;
		} catch (final SQLException e) {
			throw e;
		}
	}

	@Override
	public void onException(final JMSException e) {
		logger.error("JmsClient Failure due to : " + e.getMessage() + ". Resarting JmsClient", e);
		try {
			jmsClient.reInitialize();
		} catch (final Exception e1) {
			logger.error(
					"Failed to JmsClient JmsClient due to : " + e1.getMessage() + ". Continuing with JmsClient Restart",
					e1);
		}

		connectJmsClient();
	}

	private static class ShutdownHook extends Thread {

		final FnsClient fnsClient;

		ShutdownHook(FnsClient fnsClient) {
			this.fnsClient = fnsClient;
		}

		@Override
		public void run() {
			logger.info("Shutting Down...");

			fnsClient.stop();
		}
	}

	public static void main(final String[] args) throws Exception {

		logger.info("Loading FnsClient Config and Initalizing");

		Config typeSafeConfig;
		// load FnsClient Config
		if (Files.exists(Paths.get("fnsClient.conf"))) {
			typeSafeConfig = ConfigFactory.parseFile(new File("fnsClient.conf"));
		} else {
			typeSafeConfig = ConfigFactory.load();
		}

		FnsClient fnsClient = new FnsClient(new FnsClientConfig(typeSafeConfig));
		Runtime.getRuntime().addShutdownHook(new ShutdownHook(fnsClient));
		fnsClient.start();

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.info("Main Thread Interupted, Exiting...");
		}
	}
}
