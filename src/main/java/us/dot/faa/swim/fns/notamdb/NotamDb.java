package us.dot.faa.swim.fns.notamdb;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.JAXBException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.faa.swim.fns.FnsMessage;

public class NotamDb {
	private final static Logger logger = LoggerFactory.getLogger(NotamDb.class);
	private NotamDbConfig config;
	private boolean isValid = false;
	private boolean isInitializing = false;

	private BasicDataSource notamDbDataSource = new BasicDataSource();

	public NotamDb(NotamDbConfig config) throws Exception {
		this.config = config;

		if (!config.getDriver().equals("org.h2.Driver") && !config.getDriver().equals("org.postgresql.Driver")) {
			throw new Exception("DB Driver: " + config.getDriver()
					+ " currently not supported. Only h2 and postgresql are supported.");
		}

		notamDbDataSource.setDriverClassName(config.getDriver());
		notamDbDataSource.setUrl(config.getConnectionUrl());
		notamDbDataSource.setUsername(config.getUsername());
		notamDbDataSource.setPassword(config.getPassword());
		notamDbDataSource.setMinIdle(0);
		notamDbDataSource.setMaxIdle(10);
		notamDbDataSource.setMaxOpenPreparedStatements(100);
	}

	public boolean isValid() {
		return this.isValid;
	}

	public void setValid() {
		this.isValid = true;
	}

	public void setInvalid() {
		this.isValid = false;
	}

	public boolean isInitializing() {
		return this.isInitializing;
	}

	public void setInitializing(boolean isInitalizing) {
		this.isInitializing = isInitalizing;
	}

	public NotamDbConfig getConfig() {
		return this.config;
	}

	public boolean notamTableExists() throws SQLException {
		final Connection conn = getDBConnection();
		try {
			if (this.config.connectionUrl.startsWith("jdbc:h2")) {
				final ResultSet rset = conn.getMetaData().getTables(null, this.config.schema, this.config.table, null);
				if (rset.next()) {
					return true;
				}
			} else if (this.config.connectionUrl.startsWith("jdbc:postgresql")) {
				final ResultSet rset = conn.getMetaData().getTables(null, this.config.schema, this.config.table, null);
				if (rset.next()) {
					return true;
				}
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
		return false;
	}

	public AbstractMap.SimpleEntry<Long, Instant> getLastCorrelationId() throws SQLException {
		Connection conn = null;
		try {
			conn = getDBConnection();
			PreparedStatement getLastCorrelationIdPreparedStatement = conn.prepareStatement(
					"SELECT storedTimeStamp, correlationid FROM NOTAMS ORDER BY correlationid DESC LIMIT 1");
			ResultSet rs = getLastCorrelationIdPreparedStatement.executeQuery();
			if (rs.next()) {
				return new AbstractMap.SimpleEntry<Long, Instant>(rs.getLong("correlationid"),
						rs.getTimestamp("storedTimeStamp").toInstant());
			} else {
				return null;
			}
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	public void dropNotamTable() throws SQLException {
		final Connection conn = getDBConnection();

		try {			
			if (notamTableExists()) {
				logger.info("Dropping NOTAMS Table");
				final String dropQuery = "DROP TABLE " + this.config.table;
				conn.prepareStatement(dropQuery).execute();
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

	public void createNotamTable() throws SQLException {

		final Connection conn = getDBConnection();

		try {
			logger.info("Creating new NOTAMS Table");
			if (this.config.connectionUrl.startsWith("jdbc:h2")) {
				final String createQuery = "CREATE TABLE " + this.config.table + "(fnsid int primary key, "
						+ "correlationId bigint, issuedTimestamp timestamp, storedTimeStamp timestamp, "
						+ "updatedTimestamp timestamp, validFromTimestamp timestamp, validToTimestamp timestamp, "
						+ "classification varchar(4), locationDesignator varchar(12), notamAccountability varchar(12), "
						+ "notamText text, aixmNotamMessage clob, status varchar(12))";
				conn.prepareStatement(createQuery).execute();

				// final String CreateDesignatorIndex = "CREATE INDEX index_locationDesignator ON NOTAMS (locationDesignator)";
				// conn.prepareStatement(CreateDesignatorIndex).execute();

			} else if (this.config.connectionUrl.startsWith("jdbc:postgresql")) {
				final String createQuery = "CREATE TABLE " + this.config.table + "(fnsid int primary key, "
						+ "correlationId bigint, issuedTimestamp timestamp, storedTimeStamp timestamp, "
						+ "updatedTimestamp timestamp, validFromTimestamp timestamp, validToTimestamp timestamp, "
						+ "classification varchar(4), locationDesignator varchar(12), notamAccountability varchar(12), "
						+ "notamText text, aixmNotamMessage xml, status varchar(12))";
				conn.prepareStatement(createQuery).execute();

				// final String createDesignatorIndex = "CREATE INDEX index_locationDesignator ON " + this.config.table
				// 		+ " (locationDesignator)";
				// conn.prepareStatement(createDesignatorIndex).execute();
			}
		} catch (

		SQLException sqle) {
			throw sqle;
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	public void putNotam(final FnsMessage fnsMessage) throws SQLException {
		Connection conn = null;
		try {
			conn = getDBConnection();
			putNotam(conn, fnsMessage);

		} catch (SQLException e) {
			isValid = false;
			throw e;
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException sqle) {
				logger.error(sqle.getMessage(), sqle);
			}
		}
	}

	public void putNotam(final Connection conn, final FnsMessage fnsMessage) throws SQLException {
		PreparedStatement putNotamPreparedStatement = null;
		try {
			if (!this.isInitializing && !checkIfNotamIsNewer(fnsMessage)) {
				logger.debug("NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
						+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
						+ fnsMessage.getUpdatedTimestamp().toString()
						+ " discarded due to Notam in database has newer LastUpdateTime");
				return;
			}

			putNotamPreparedStatement = createPutNotamPreparedStatement(conn);
			populatePutNotamPreparedStatement(putNotamPreparedStatement, fnsMessage);
			putNotamPreparedStatement.executeUpdate();
		} catch (SQLException e) {
			throw e;
		} finally {
			if (putNotamPreparedStatement != null) {
				putNotamPreparedStatement.close();
			}
		}
	}

	public void putBulkNotam(List<FnsMessage> fnsMessageList) throws SQLException {
		Connection conn = null;
		try {
			conn = getDBConnection();
			putBulkNotam(conn, fnsMessageList);
		} catch (SQLException e) {
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

	}

	public void putBulkNotam(final Connection conn, List<FnsMessage> fnsMessageList) throws SQLException {
		PreparedStatement putNotamPreparedStatement = null;
		conn.setAutoCommit(false);
		try {
			putNotamPreparedStatement = createPutNotamPreparedStatement(conn);
			for (FnsMessage fnsMessage : fnsMessageList) {

				if (!this.isInitializing && !checkIfNotamIsNewer(fnsMessage)) {
					logger.debug("NOTAM with FNS_ID:" + fnsMessage.getFNS_ID() + " and CorrelationId: "
							+ fnsMessage.getCorrelationId() + " and LastUpdateTime: "
							+ fnsMessage.getUpdatedTimestamp().toString()
							+ " discarded due to Notam in database has newer LastUpdateTime");
					return;
				}
				populatePutNotamPreparedStatement(putNotamPreparedStatement, fnsMessage);
				putNotamPreparedStatement.addBatch();
			}
			putNotamPreparedStatement.executeUpdate();
			conn.commit();

		} catch (SQLException e) {
			throw e;
		} finally {
			if (putNotamPreparedStatement != null) {
				putNotamPreparedStatement.close();
			}
			conn.setAutoCommit(false);

		}
	}

	private PreparedStatement createPutNotamPreparedStatement(Connection conn) throws SQLException {

		String putNotamSql = "";
		if (this.config.connectionUrl.startsWith("jdbc:h2")) {

			putNotamSql = "INSERT INTO " + this.config.table + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
					+ "ON DUPLICATE KEY UPDATE " + "correlationId = ?, " + "updatedTimestamp = ?,"
					+ "validFromTimestamp =?, " + "validToTimestamp =?, " + "classification =?, "
					+ "locationDesignator =?, " + "notamAccountability =?, " + "notamText =?, " + "aixmNotamMessage =?,"
					+ "status =?";

		} else if (this.config.connectionUrl.startsWith("jdbc:postgresql")) {
			putNotamSql = "INSERT INTO " + this.config.table + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
					+ "ON CONFLICT (fnsid) DO UPDATE SET " + "correlationId = ?, " + "updatedTimestamp = ?, "
					+ "validFromTimestamp =?, " + "validToTimestamp =?, " + "classification =?, "
					+ "locationDesignator =?, " + "notamAccountability =?, " + "notamText =?, " + "aixmNotamMessage =?,"
					+ "status =?";
		}

		PreparedStatement putMessagePreparedStatement = conn.prepareStatement(putNotamSql);

		return putMessagePreparedStatement;

	}

	private void populatePutNotamPreparedStatement(PreparedStatement putNotamPreparedStatement,
			final FnsMessage fnsMessage) throws SQLException {
		putNotamPreparedStatement.setLong(1, fnsMessage.getFNS_ID());
		putNotamPreparedStatement.setLong(2, fnsMessage.getCorrelationId());
		putNotamPreparedStatement.setTimestamp(3, fnsMessage.getIssuedTimestamp());
		putNotamPreparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
		putNotamPreparedStatement.setTimestamp(5, fnsMessage.getUpdatedTimestamp());
		putNotamPreparedStatement.setTimestamp(6, fnsMessage.getValidFromTimestamp());
		putNotamPreparedStatement.setTimestamp(7, fnsMessage.getValidToTimestamp());
		putNotamPreparedStatement.setString(8, fnsMessage.getClassification());
		putNotamPreparedStatement.setString(9, fnsMessage.getLocationDesignator());
		putNotamPreparedStatement.setString(10, fnsMessage.getNotamAccountability());
		putNotamPreparedStatement.setString(11, fnsMessage.getNotamText());

		if (this.config.connectionUrl.startsWith("jdbc:h2")) {
			final Clob aixmNotamMessageClob = putNotamPreparedStatement.getConnection().createClob();
			aixmNotamMessageClob.setString(1, fnsMessage.getAixmNotamMessage());

			putNotamPreparedStatement.setClob(12, aixmNotamMessageClob);
		} else if (this.config.connectionUrl.startsWith("jdbc:postgresql")) {
			SQLXML aixmNotamMessageSqlXml = putNotamPreparedStatement.getConnection().createSQLXML();
			aixmNotamMessageSqlXml.setString(fnsMessage.getAixmNotamMessage());

			putNotamPreparedStatement.setSQLXML(12, aixmNotamMessageSqlXml);
		}

		putNotamPreparedStatement.setString(13, fnsMessage.getStatus().toString());

		// update if exists
		putNotamPreparedStatement.setLong(14, fnsMessage.getCorrelationId());
		putNotamPreparedStatement.setTimestamp(15, new Timestamp(System.currentTimeMillis()));
		putNotamPreparedStatement.setTimestamp(16, fnsMessage.getValidFromTimestamp());
		putNotamPreparedStatement.setTimestamp(17, fnsMessage.getValidToTimestamp());
		putNotamPreparedStatement.setString(18, fnsMessage.getClassification());
		putNotamPreparedStatement.setString(19, fnsMessage.getLocationDesignator());
		putNotamPreparedStatement.setString(20, fnsMessage.getNotamAccountability());
		putNotamPreparedStatement.setString(21, fnsMessage.getNotamText());

		if (this.config.connectionUrl.startsWith("jdbc:h2")) {
			final Clob aixmNotamMessageClob = putNotamPreparedStatement.getConnection().createClob();
			aixmNotamMessageClob.setString(1, fnsMessage.getAixmNotamMessage());

			putNotamPreparedStatement.setClob(22, aixmNotamMessageClob);
		} else if (this.config.connectionUrl.startsWith("jdbc:postgresql")) {
			SQLXML aixmNotamMessageSqlXml = putNotamPreparedStatement.getConnection().createSQLXML();
			aixmNotamMessageSqlXml.setString(fnsMessage.getAixmNotamMessage());

			putNotamPreparedStatement.setSQLXML(22, aixmNotamMessageSqlXml);
		}

		putNotamPreparedStatement.setString(23, fnsMessage.getStatus().toString());
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

	public Connection getDBConnection() throws SQLException {
		return notamDbDataSource.getConnection();
	}

	// db lookups
	public void getByLocationDesignator(String locationDesignator, OutputStream output, boolean asJson)
			throws SQLException, JAXBException, IOException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement(
					"select fnsid, aixmNotamMessage from " + this.config.table + " where locationDesignator = ?"
							+ " AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)");
			selectPreparedStatement.setString(1, locationDesignator);

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}
	}

	public void getByClassification(String classification, OutputStream output, boolean asJson)
			throws SQLException, JAXBException, IOException {

		Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + this.config.table
					+ " WHERE classification = ? AND status = 'ACTIVE' AND (validtotimestamp > NOW() OR validtotimestamp is null)");
			selectPreparedStatement.setString(1, classification);

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		}

		try {
			conn.close();
		} catch (SQLException e) {
			logger.error("Closing DB Connection: " + e.getMessage());
		}

	}

	public void getDelta(String deltaTime, OutputStream output, boolean asJson)
			throws SQLException, JAXBException, IOException {
		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage FROM " + this.config.table
					+ " WHERE updatedTimestamp >= ? OR validtotimestamp is null");
			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(deltaTime));

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}
	}

	public void getByTimeRange(String fromDateTime, String toDateTime, OutputStream output, boolean asJson)
			throws SQLException, JAXBException, IOException {

		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT fnsid, aixmNotamMessage from " + this.config.table
					+ " WHERE validFromTimestamp >= ? AND (validToTimestamp <= ? OR validToTimestamp is null) AND status = 'ACTIVE'");

			selectPreparedStatement.setTimestamp(1, Timestamp.valueOf(fromDateTime));
			selectPreparedStatement.setTimestamp(2, Timestamp.valueOf(toDateTime));

			writeResponseToSteam(selectPreparedStatement, output, asJson);
		} catch (SQLException e) {
			logger.error("Createing Select Statement: " + e.getMessage());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e);
		} finally {
			conn.close();
		}
	}

	public void getAllNotams(OutputStream output, boolean asJson) throws SQLException, JAXBException, IOException {
		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("SELECT aixmNotamMessage FROM " + this.config.table
					+ " WHERE status = 'ACTIVE' AND (validtotimestamp > NOW()  OR validtotimestamp is null)");

			writeResponseToSteam(selectPreparedStatement, output, asJson);

		} catch (SQLException e) {
			logger.error("[DB] Error Createing Select Statement: " + e.getMessage());
		} finally {
			conn.close();
		}
	}

	private void writeResponseToSteam(PreparedStatement selectPreparedStatement, OutputStream output, boolean asJson)
			throws SQLException, IOException {
		final ResultSet resultSet = selectPreparedStatement.executeQuery();

		OutputStream bos = new BufferedOutputStream(output);
		if (asJson) {
			bos.write("{\n\"AixmBasicMessageCollection\": [\n".getBytes());
		} else {
			bos.write("<AixmBasicMessageCollection>".getBytes());
		}

		boolean first = true;
		while (resultSet.next()) {
			if (asJson) {
				if (!first) {
					bos.write(",\n".getBytes());
				}
				first = false;
				bos.write(XML.toJSONObject(resultSet.getString("aixmNotamMessage")).toString().getBytes());
			} else {
				bos.write(resultSet.getString("aixmNotamMessage").replaceAll("\\<\\?xml(.+?)\\?\\>", "").trim().getBytes());
			}
		}

		if (asJson) {
			bos.write("\n]\n}".getBytes());
		} else {
			bos.write("</AixmBasicMessageCollection>".getBytes());
		}

		bos.flush();
		bos.close();
	}

	public Map<String, Timestamp> getValidationMap() throws Exception {

		final Connection conn = getDBConnection();
		PreparedStatement selectPreparedStatement = null;
		try {
			selectPreparedStatement = conn.prepareStatement("select fnsid, updatedtimestamp from " + this.config.table);

			return createValidationMapFromDatabase(selectPreparedStatement);

		} catch (SQLException e) {
			throw e;
		} finally {
			conn.close();
		}
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

}