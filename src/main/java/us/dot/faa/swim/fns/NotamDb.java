package us.dot.faa.swim.fns;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import us.dot.faa.swim.fns.FnsMessage.FnsMessageParseException;
import us.dot.faa.swim.fns.FnsMessage.NotamStatus;
import us.dot.faa.swim.utilities.xml.SaxParserErrorHandler;
import us.dot.faa.swim.utilities.xml.XmlSplitterSaxParser;

public class NotamDb {
	private final static Logger logger = LoggerFactory.getLogger(NotamDb.class);
	private String connectionUrl = "jdbc:h2:./Notams;mode=MySQL;AUTO_SERVER=TRUE";
	private String driver = "org.postgresql.Driver";
	private String username = "";
	private String password = "";
	private String schema = "PUBLIC";
	private String table = "NOTAMS";
	private boolean isValid = false;
	private boolean isInitializing = false;

	public Queue<FnsMessage> pendingMessages = new ConcurrentLinkedQueue<FnsMessage>();

	public void setConfig(final String dbConnectionUrl, final String dbDriver, final String dbUsername,
			final String dbPassword, final String dbSchema, final String notamTableName) throws Exception {

		if (!dbDriver.startsWith("jdbc:h2") || !dbDriver.startsWith("jdbc:postgresql")) {
			throw new Exception(
					"DB Driver: " + dbDriver + " currently not supported. Only h2 and postgresql are supported.");
		}

		this.connectionUrl = dbConnectionUrl;
		this.driver = dbDriver;
		this.username = dbUsername;
		this.password = dbPassword;
		this.schema = dbSchema;
		this.table = notamTableName;
	}

	public boolean isValid() {
		return this.isValid;
	}

	public void setInvalid() {
		this.isValid = false;
	}

	public void initalizeNotamDb(String filePath) throws FileNotFoundException, IOException, SQLException, SAXException,
			ParserConfigurationException, Exception {
		initalizeNotamDb(new GZIPInputStream(new FileInputStream(filePath)));
	};

	public void initalizeNotamDb(InputStream inputStream)
			throws SQLException, IOException, SAXException, ParserConfigurationException, Exception {

		if (this.isInitializing) {
			return;
		} else {
			this.isInitializing = true;
		}

		try {

			dropAndCreateNotamTable();

			logger.info("Initizliaing NotamDb from FIL File");

			final int notamCount = loadNotams(inputStream);

			this.isValid = true;

			logger.info("Loaded " + notamCount + " Notams");

			loadQueuedMessages();

			logger.info("NotamDb initalized");
		} catch (SQLException | IOException | SAXException | ParserConfigurationException sqle) {
			throw sqle;
		} finally {
			try {
				this.isInitializing = false;
				inputStream.close();
			} catch (IOException ioe) {
				logger.error(ioe.getMessage(), ioe);
			}
		}
	}

	private void dropAndCreateNotamTable() throws SQLException {

		final Connection conn = getDBConnection();
		logger.info("Creating NOTAMS Table");
		logger.info("Looking for existing NOTAMS Table");

		try {
			if (this.connectionUrl.startsWith("jdbc:h2")) {
				final ResultSet rset = conn.getMetaData().getTables(null, schema, table, null);
				if (rset.next()) {
					logger.info("Existing NOTAMS Table found, dropping");
					final String dropQuery = "DROP TABLE " + table;
					conn.prepareStatement(dropQuery).execute();
				}
			} else if (this.connectionUrl.startsWith("jdbc:postgresql")) {
				final ResultSet rset = conn.getMetaData().getTables(null, schema, table, null);
				if (rset.next()) {
					logger.info("Existing NOTAMS Table found, dropping");
					final String dropQuery = "DROP TABLE " + table;
					conn.prepareStatement(dropQuery).execute();
				}
			}

			if (this.connectionUrl.startsWith("jdbc:h2")) {
				final String createQuery = "CREATE TABLE " + table + "(fnsid int primary key, "
						+ "correlationId bigint, issuedTimestamp timestamp, storedTimeStamp timestamp, "
						+ "updatedTimestamp timestamp, validFromTimestamp timestamp, validToTimestamp timestamp, "
						+ "classification varchar(4), locationDesignator varchar(12), notamAccountability varchar(12), "
						+ "notamText text, aixmNotamMessage clob, status varchar(12))";
				conn.prepareStatement(createQuery).execute();

				final String CreateDesignatorIndex = "CREATE INDEX index_locationDesignator ON NOTAMS (locationDesignator)";
				conn.prepareStatement(CreateDesignatorIndex).execute();

			} else if (this.connectionUrl.startsWith("jdbc:postgresql")) {
				final String createQuery = "CREATE TABLE " + table + "(fnsid int primary key, "
						+ "correlationId bigint, issuedTimestamp timestamp, storedTimeStamp timestamp, "
						+ "updatedTimestamp timestamp, validFromTimestamp timestamp, validToTimestamp timestamp, "
						+ "classification varchar(4), locationDesignator varchar(12), notamAccountability varchar(12), "
						+ "notamText text, aixmNotamMessage xml, status varchar(12))";
				conn.prepareStatement(createQuery).execute();

				final String createDesignatorIndex = "CREATE INDEX index_locationDesignator ON " + table
						+ " (locationDesignator)";
				conn.prepareStatement(createDesignatorIndex).execute();
			}
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	private void loadQueuedMessages() throws SQLException {

		logger.info("Loading " + pendingMessages.size() + " queued notams");

		// update with pending
		FnsMessage messageToProcesses = pendingMessages.poll();

		while (messageToProcesses != null) {
			if (checkIfNotamIsNewer(messageToProcesses)) {
				putNotam(messageToProcesses);
			} else {
				logger.debug("NOTAM with FNS_ID:" + messageToProcesses.getFNS_ID() + " and CorrelationId: "
						+ messageToProcesses.getCorrelationId() + " and LastUpdateTime: "
						+ messageToProcesses.getUpdatedTimestamp().toString()
						+ " discarded due to Notam in database has newer LastUpdateTime");
			}
			messageToProcesses = pendingMessages.poll();
		}

		logger.info("Queued notams loaded");

	}

	@SuppressWarnings("serial")
	public void putNotam(final FnsMessage fnsMessage) throws SQLException {

		logger.debug("Putting NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
				+ fnsMessage.getCorrelationId() + " in Database");

		final Connection conn = getDBConnection();

		try {
			if (!checkIfNotamIsNewer(fnsMessage)) {
				logger.debug("NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
						+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
						+ fnsMessage.getUpdatedTimestamp().toString()
						+ " discarded due to Notam in database has newer LastUpdateTime");
				return;
			}

			putNotams(conn, new ArrayList<FnsMessage>() {
				{
					add(fnsMessage);
				}
			});
		} catch (SQLException e) {
			isValid = false;
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}

	}

	public void putNotams(final List<FnsMessage> fnsMessages) throws SQLException {
		final Connection conn = getDBConnection();

		try {
			putNotams(conn, fnsMessages);
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	private void putNotams(Connection conn, final List<FnsMessage> fnsMessages) throws SQLException {

		PreparedStatement putMessagePreparedStatement = null;

		try {

			if (this.connectionUrl.startsWith("jdbc:h2")) {

				putMessagePreparedStatement = conn.prepareStatement("INSERT INTO " + table
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " + "ON DUPLICATE KEY UPDATE "
						+ "correlationId = ?, " + "updatedTimestamp = ?," + "validFromTimestamp =?, "
						+ "validToTimestamp =?, " + "classification =?, " + "locationDesignator =?, "
						+ "notamAccountability =?, " + "notamText =?, " + "aixmNotamMessage =?," + "status =?");

			} else if (this.connectionUrl.startsWith("jdbc:postgresql")) {
				putMessagePreparedStatement = conn.prepareStatement("INSERT INTO " + table
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " + "ON CONFLICT (fnsid) DO UPDATE SET "
						+ "correlationId = ?, " + "updatedTimestamp = ?, " + "validFromTimestamp =?, "
						+ "validToTimestamp =?, " + "classification =?, " + "locationDesignator =?, "
						+ "notamAccountability =?, " + "notamText =?, " + "aixmNotamMessage =?," + "status =?");
			}

			for (FnsMessage fnsMessage : fnsMessages) {

				// insert if new
				putMessagePreparedStatement.setLong(1, fnsMessage.getFNS_ID());
				putMessagePreparedStatement.setLong(2, fnsMessage.getCorrelationId());
				putMessagePreparedStatement.setTimestamp(3, fnsMessage.getIssuedTimestamp());
				putMessagePreparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
				putMessagePreparedStatement.setTimestamp(5, fnsMessage.getUpdatedTimestamp());
				putMessagePreparedStatement.setTimestamp(6, fnsMessage.getValidFromTimestamp());
				putMessagePreparedStatement.setTimestamp(7, fnsMessage.getValidToTimestamp());
				putMessagePreparedStatement.setString(8, fnsMessage.getClassification());
				putMessagePreparedStatement.setString(9, fnsMessage.getLocationDesignator());
				putMessagePreparedStatement.setString(10, fnsMessage.getNotamAccountability());
				putMessagePreparedStatement.setString(11, fnsMessage.getNotamText());

				if (this.connectionUrl.startsWith("jdbc:h2")) {
					final Clob aixmNotamMessageClob = conn.createClob();
					aixmNotamMessageClob.setString(1, fnsMessage.getAixmNotamMessage());

					putMessagePreparedStatement.setClob(12, aixmNotamMessageClob);
				} else if (this.connectionUrl.startsWith("jdbc:postgresql")) {
					SQLXML aixmNotamMessageSqlXml = conn.createSQLXML();
					aixmNotamMessageSqlXml.setString(fnsMessage.getAixmNotamMessage());

					putMessagePreparedStatement.setSQLXML(12, aixmNotamMessageSqlXml);
				}

				putMessagePreparedStatement.setString(13, fnsMessage.getStatus().toString());

				// update if exists
				putMessagePreparedStatement.setLong(14, fnsMessage.getCorrelationId());
				putMessagePreparedStatement.setTimestamp(15, new Timestamp(System.currentTimeMillis()));
				putMessagePreparedStatement.setTimestamp(16, fnsMessage.getValidFromTimestamp());
				putMessagePreparedStatement.setTimestamp(17, fnsMessage.getValidToTimestamp());
				putMessagePreparedStatement.setString(18, fnsMessage.getClassification());
				putMessagePreparedStatement.setString(19, fnsMessage.getLocationDesignator());
				putMessagePreparedStatement.setString(20, fnsMessage.getNotamAccountability());
				putMessagePreparedStatement.setString(21, fnsMessage.getNotamText());

				if (this.connectionUrl.startsWith("jdbc:h2")) {
					final Clob aixmNotamMessageClob = conn.createClob();
					aixmNotamMessageClob.setString(1, fnsMessage.getAixmNotamMessage());

					putMessagePreparedStatement.setClob(22, aixmNotamMessageClob);
				} else if (this.connectionUrl.startsWith("jdbc:postgresql")) {
					SQLXML aixmNotamMessageSqlXml = conn.createSQLXML();
					aixmNotamMessageSqlXml.setString(fnsMessage.getAixmNotamMessage());

					putMessagePreparedStatement.setSQLXML(22, aixmNotamMessageSqlXml);
				}

				putMessagePreparedStatement.setString(23, fnsMessage.getStatus().toString());

				putMessagePreparedStatement.addBatch();

			}

			putMessagePreparedStatement.executeBatch();
		} catch (SQLException sqle) {
			throw sqle;
		} finally {

			try {
				putMessagePreparedStatement.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}

	}

	public boolean checkIfNotamIsNewer(final FnsMessage fnsMessage) throws SQLException {

		final Connection conn = getDBConnection();

		try {
			PreparedStatement checkIfNotamIsNewerPreparedStatement = null;
			logger.debug("Looking up up if NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
					+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
					+ fnsMessage.getUpdatedTimestamp().toString());

			checkIfNotamIsNewerPreparedStatement = conn
					.prepareStatement("SELECT updatedtimestamp FROM NOTAMS WHERE fnsid=" + fnsMessage.getFNS_ID());

			ResultSet rset = checkIfNotamIsNewerPreparedStatement.executeQuery();

			if (!rset.next()) {
				return true;
			} else if (rset.getTimestamp("updatedtimestamp").getTime() < fnsMessage.getUpdatedTimestamp().toInstant()
					.toEpochMilli()) {
				return true;
			}

			checkIfNotamIsNewerPreparedStatement.close();
		} catch (final SQLException sqle) {
			throw sqle;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
		return false;
	}

	public int removeOldNotams() throws SQLException {
		final Connection conn = getDBConnection();
		PreparedStatement putMessagePreparedStatement;
		try {
			putMessagePreparedStatement = conn.prepareStatement("DELETE FROM NOTAMS WHERE validtotimestamp < NOW()");
			int recordsDeleted = putMessagePreparedStatement.executeUpdate();

			putMessagePreparedStatement = conn.prepareStatement("DELETE FROM NOTAMS WHERE status != 'ACTIVE'");
			recordsDeleted = recordsDeleted + putMessagePreparedStatement.executeUpdate();

			putMessagePreparedStatement.close();
			return recordsDeleted;
		} catch (final SQLException sqle) {
			throw sqle;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	private Connection getDBConnection() throws SQLException {
		Connection dbConnection = null;
		try {
			Class.forName(driver);
		} catch (final ClassNotFoundException e) {
			logger.error("Unable to Load Driver do to: " + e.getMessage(), e);
		}
		try {
			dbConnection = DriverManager.getConnection(connectionUrl, username, password);
			dbConnection.setSchema(schema);
			return dbConnection;
		} catch (SQLException sqle1) {
			try {
				dbConnection.close();
			} catch (SQLException sqle2) {
				logger.error(sqle2.getMessage(), sqle2);
			}

			throw sqle1;
		}
	}

	private int loadNotams(InputStream inputStream) throws Exception {

		int loadedMessages = 0;
		
		CopyOnWriteArrayList<String> notamCount = new CopyOnWriteArrayList<String>();
		final Connection conn = getDBConnection();

		try {

			final XmlSplitterSaxParser parser = new XmlSplitterSaxParser(msg -> {
				try {
					final FnsMessage fnsMessage = new FnsMessage((long) -1, msg);
					fnsMessage.setStatus(NotamStatus.ACTIVE);

					putNotam(fnsMessage);
					notamCount.add(String.valueOf(fnsMessage.getFNS_ID()));
									
				} catch (FnsMessageParseException | SQLException e) {
					logger.error("Failed to load notam due to: " + e.getMessage(), e);
					throw new RuntimeException(e);
				}
			}, 4);

			final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
			saxParserFactory.setNamespaceAware(true);
			saxParserFactory.setFeature("http://xml.org/sax/features/namespaces", true);
			saxParserFactory.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
			saxParserFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			saxParserFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

			SAXParser saxParser = saxParserFactory.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			xmlReader.setContentHandler(parser);
			SaxParserErrorHandler parsingErrorHandeler = new SaxParserErrorHandler();
			xmlReader.setErrorHandler(parsingErrorHandeler);
			xmlReader.parse(new InputSource(inputStream));
			if (!parsingErrorHandeler.isValid()) {				
				throw new Exception("Failed to Parse");
			}
			
			loadedMessages = notamCount.size();

		} catch (final IOException | SAXException | ParserConfigurationException e) {
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}

		return loadedMessages;
	}

	// db lookups
	public AbstractMap.SimpleEntry<Integer, String> getByLocationDesignator(String locationDesignator)
			throws SQLException, JAXBException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		AbstractMap.SimpleEntry<Integer, String> results = null;
		try {
			selectPreparedStatement = conn
					.prepareStatement("select fnsid, aixmNotamMessage from " + table + " where locationDesignator = ?"
							+ " AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)");
			selectPreparedStatement.setString(1, locationDesignator);

			results = createResponse(selectPreparedStatement);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}

		return results;

	}

	public AbstractMap.SimpleEntry<Integer, String> getByClassification(String classification, int lastFnsId)
			throws SQLException, JAXBException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		AbstractMap.SimpleEntry<Integer, String> results = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + table
					+ " WHERE fnsid > " + lastFnsId
					+ " AND classification = ? AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)"
					+ " ORDER BY fnsid" + " LIMIT 1000");
			selectPreparedStatement.setString(1, classification);

			results = createResponse(selectPreparedStatement);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		}

		try {
			conn.close();
		} catch (SQLException e) {
			logger.error("Closing DB Connection: " + e.getMessage());
		}

		return results;

	}

	public AbstractMap.SimpleEntry<Integer, String> getDelta(String deltaTime, int lastFnsId)
			throws SQLException, JAXBException {
		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		AbstractMap.SimpleEntry<Integer, String> results = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + table
					+ " WHERE fnsid > " + lastFnsId + " AND updatedTimestamp >= ? OR validtotimestamp is null"
					+ " order by fnsid" + " LIMIT 1000");
			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(deltaTime));

			results = createResponse(selectPreparedStatement);
		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}

		return results;
	}

	public AbstractMap.SimpleEntry<Integer, String> getByTimeRange(String fromDateTime, String toDateTime,
			int lastFnsId) throws SQLException, JAXBException {

		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		SimpleEntry<Integer, String> results = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage from " + table
					+ " WHERE fnsid > " + lastFnsId
					+ " AND validFromTimestamp >= ? AND (validToTimestamp <= ? OR validToTimestamp is null) AND status = 'ACTIVE'"
					+ " ORDER BY fnsid" + " LIMIT 1000");

			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(fromDateTime));
			selectPreparedStatement.setTimestamp(2, Timestamp.valueOf(toDateTime));

			results = createResponse(selectPreparedStatement);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e);
		} finally {
			conn.close();
		}

		return results;
	}

	public AbstractMap.SimpleEntry<Integer, String> getAllNotams(int lastFnsId) throws SQLException, JAXBException {
		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		SimpleEntry<Integer, String> results = null;
		try {
			selectPreparedStatement = conn
					.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + table + " WHERE fnsid > " + lastFnsId
							+ " AND status = 'ACTIVE' AND (validtotimestamp > NOW()  OR validtotimestamp is null)"
							+ " ORDER BY fnsid" + " LIMIT 1000");

			results = createResponse(selectPreparedStatement);
		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}

		return results;
	}

	private AbstractMap.SimpleEntry<Integer, String> createResponse(PreparedStatement selectPreparedStatement)
			throws SQLException, JAXBException {
		final long startTime = System.nanoTime();
		final ResultSet resultSet = selectPreparedStatement.executeQuery();

		final List<String> aixmMessageStringList = new ArrayList<String>();

		int lastFnsId = 0;

		while (resultSet.next()) {

			String aixmNotam = resultSet.getString("aixmNotamMessage");

			aixmMessageStringList.add(aixmNotam);

			lastFnsId = resultSet.getInt("fnsId");
		}

		selectPreparedStatement.close();

		final long endTime = System.nanoTime();
		final long totalTime = endTime - startTime;
		logger.info("Selected and Marshaled " + aixmMessageStringList.size() + " Messages from DB. Took "
				+ TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

		final String result = FnsMessage.createAixmBasicMessageCollectionMessage(aixmMessageStringList);

		return new AbstractMap.SimpleEntry<Integer, String>(lastFnsId, result);
	}

	public Map<String, Timestamp> validateDatabase(InputStream inputStream)
			throws SQLException, ParserConfigurationException, IOException, SAXException {
		Map<String, Timestamp> missingNotamsMap = new HashMap<String, Timestamp>();

		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("select fnsid, updatedtimestamp from " + table);

			Map<String, Timestamp> databaseValidationMap = createValidationMapFromDatabase(selectPreparedStatement);
			Map<String, Timestamp> filValidationMap = createValidationMapFromFil(inputStream);

			for (Map.Entry<String, Timestamp> entry : filValidationMap.entrySet()) {
				Timestamp dbUpdateTime = databaseValidationMap.get(entry.getKey());
				if (dbUpdateTime == null) {
					missingNotamsMap.put("Missing-" + entry.getKey(), entry.getValue());
				} else {
					if (!entry.getValue().equals(dbUpdateTime) && !entry.getValue().before(dbUpdateTime)) {
						missingNotamsMap.put("Newer-" + entry.getKey(), entry.getValue());
					}
				}
			}

		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}

		return missingNotamsMap;

	}

	private Map<String, Timestamp> createValidationMapFromDatabase(PreparedStatement selectPreparedStatement)
			throws SQLException {
		Map<String, Timestamp> validationMap = new HashMap<String, Timestamp>();

		final ResultSet resultSet = selectPreparedStatement.executeQuery();

		while (resultSet.next()) {

			String fnsId = resultSet.getString("fnsid");
			Timestamp updatedTimestamp = resultSet.getTimestamp("updatedTimestamp");

			validationMap.put(fnsId, updatedTimestamp);

		}

		return validationMap;
	}

	private Map<String, Timestamp> createValidationMapFromFil(InputStream filInputSteam)
			throws ParserConfigurationException, IOException, SAXException {

		final Map<String, Timestamp> validationMap = new HashMap<String, Timestamp>();

		logger.info("Getting most recent FNS Initial Load File from SFTP server");

		final XmlSplitterSaxParser parser = new XmlSplitterSaxParser(msg -> {
			try {
				final FnsMessage fnsMessage = new FnsMessage((long) -1, msg);

				validationMap.put(String.valueOf(fnsMessage.getFNS_ID()), fnsMessage.getUpdatedTimestamp());

			} catch (Exception e) {
				logger.error("Failed to create FnsMessage from Split due to: " + e.getMessage(), e);
				logger.debug(msg);
			}
		}, 4);

		final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		saxParserFactory.setNamespaceAware(true);
		saxParserFactory.setFeature("http://xml.org/sax/features/namespaces", true);
		saxParserFactory.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
		saxParserFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
		saxParserFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

		SAXParser saxParser = saxParserFactory.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(parser);
		xmlReader.parse(new InputSource(filInputSteam));

		return validationMap;
	}
}