package us.dot.faa.swim.fns;

import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.faa.swim.fns.FnsMessage.NotamStatus;
import us.dot.faa.swim.utilities.MissedMessageTracker;

public class FnsJmsMessageProcessor implements MessageListener {
	private static final Logger logger = LoggerFactory.getLogger(FnsJmsMessageProcessor.class);

	private NotamDb notamDb = null;
	private MissedMessageTracker missedMessageTracker = null;
	private final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

	public FnsJmsMessageProcessor(NotamDb notamDb)
	{
		this.notamDb = notamDb;
	}
	
	public FnsJmsMessageProcessor(NotamDb notamDb, MissedMessageTracker missedMessageTracker)
	{
		this.notamDb = notamDb;
		this.missedMessageTracker = missedMessageTracker;
	}
	
	public void setMissedMessageTracker(MissedMessageTracker missedMessageTracker)
	{
		this.missedMessageTracker = missedMessageTracker;
	}

	@Override
	public void onMessage(Message jmsMessage) {
		try {						

			FnsMessage fnsMessage = parseFnsJmsMessage(jmsMessage);
			
			logger.debug("Recieved JMS FNS Message " + fnsMessage.getFNS_ID() + " with CorrelationIds: "
					+ fnsMessage.getCorrelationId() + " | Latency (ms): "
					+ (Instant.now().toEpochMilli() - jmsMessage.getJMSTimestamp()));
			
			if(missedMessageTracker != null)
			{
				missedMessageTracker.put(fnsMessage.getCorrelationId(), Instant.now());
			}
			
			executor.execute(new Runnable(){
				@Override
				public void run() {
					processFnsMessage(fnsMessage);					
				}				
			});

		} catch (Exception e) {
			logger.error("Failed to processed JMS Text Message due to: " + e.getMessage());
		}
	}

	private FnsMessage parseFnsJmsMessage(Message message) throws Exception {

		FnsMessage fnsMessage;
		long correlationId = -1;
		if (message.propertyExists("us_gov_dot_faa_aim_fns_nds_CorrelationID")) {
			correlationId = Long.parseLong(message.getStringProperty("us_gov_dot_faa_aim_fns_nds_CorrelationID"));
		}

		String messageBody = "";
		if (message instanceof BytesMessage) {
			BytesMessage byteMessage = (BytesMessage) message;
			byte[] messageBytes = new byte[(int) byteMessage.getBodyLength()];
			byteMessage.readBytes(messageBytes);
			messageBody = new String(messageBytes);
		} else if (message instanceof TextMessage) {
			messageBody = ((TextMessage) message).getText();
		}

		fnsMessage = new FnsMessage(correlationId, messageBody);
		fnsMessage.setStatus(NotamStatus.valueOf(message.getStringProperty("us_gov_dot_faa_aim_fns_nds_NOTAMStatus")));

		return fnsMessage;

	}

	private void processFnsMessage(final FnsMessage fnsMessage) {
		if (!this.notamDb.isValid()) {
			logger.debug("Pending " + fnsMessage.getStatus() + " NOTAM with FNS_ID:" + fnsMessage.getFNS_ID()
					+ " and CorrelationId: " + fnsMessage.getCorrelationId() + " due to invalid database.");
			this.notamDb.pendingMessages.add(fnsMessage);
		} else {

			try {
				this.notamDb.putNotam(fnsMessage);
			} catch (SQLException e) {
				logger.warn("Failed to insert Notam into Database, setting NotamDb to inValid", e);
				this.notamDb.setInvalid();
			}
		}
	}

}