package us.dot.faa.swim.fns;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;
import javax.xml.bind.JAXBIntrospector;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import aero.aixm.event.EventTimeSlicePropertyType;
import aero.aixm.event.EventTimeSliceType;
import aero.aixm.event.EventType;
import aero.aixm.event.NOTAMType;
import aero.aixm.extension.fnse.EventExtensionType;
import aero.aixm.message.AIXMBasicMessageType;
import aero.aixm.message.BasicMessageMemberAIXMPropertyType;
import net.opengis.gml.TimePeriodType;
import us.dot.faa.swim.fns.aixm.AIXMBasicMessageFeatureCollection;
import us.dot.faa.swim.fns.aixm.AixmUtilities;

public class FnsMessage {
	private static final Logger logger = LoggerFactory.getLogger(FnsMessage.class);

	private int fns_id;
	private long correlationId;
	private Timestamp issuedTimestamp;
	private Timestamp updatedTimestamp;
	private Timestamp validFromTimestamp;
	private Timestamp validToTimestamp;
	private String classification;
	private String locationDesignator;
	private String notamAccountability;
	private String notamText;
	private String aixmNotamMessage;

	public enum NotamStatus {
		ACTIVE, CANCELLED, EXPIRED
	};

	private NotamStatus status;

	public FnsMessage(final Long correlationId, final String xmlMessage) throws FnsMessageParseException {

		StringReader reader = new StringReader(xmlMessage.trim());
		
		try {
			final Unmarshaller jaxb_FNSNOTAM_Unmarshaller = AixmUtilities.createAixmUnmarshaller();
	
			this.correlationId = correlationId;			
	
			AIXMBasicMessageType aixmBasicMessage = null;

			aixmBasicMessage = (AIXMBasicMessageType) JAXBIntrospector
					.getValue(jaxb_FNSNOTAM_Unmarshaller.unmarshal(reader));

			this.fns_id = Integer.parseInt(aixmBasicMessage.getId().split("_")[2]);

			for (Node element : aixmBasicMessage.getAny()) {
				BasicMessageMemberAIXMPropertyType basicMessageMemberAIXMPropertyType = jaxb_FNSNOTAM_Unmarshaller
						.unmarshal(element, BasicMessageMemberAIXMPropertyType.class).getValue();

				String featureTypeName = basicMessageMemberAIXMPropertyType.getAbstractAIXMFeature().getName()
						.getLocalPart();
				if (featureTypeName == "Event") {
					EventType event = (EventType) basicMessageMemberAIXMPropertyType.getAbstractAIXMFeature()
							.getValue();
					EventTimeSliceType eventTimeSlice = event.getTimeSlice().get(0).getEventTimeSlice();
					List<EventTimeSlicePropertyType> notamEvent = event.getTimeSlice().stream()
							.filter(evt -> !evt.getEventTimeSlice().getTextNOTAM().isEmpty())
							.collect(Collectors.toList());

					if (notamEvent.size() == 1) {

						TimePeriodType validTime = (TimePeriodType) eventTimeSlice.getValidTime()
								.getAbstractTimePrimitive().getValue();

						NOTAMType notam = notamEvent.get(0).getEventTimeSlice().getTextNOTAM().get(0).getNOTAM();
						EventExtensionType eventExtension = (EventExtensionType) eventTimeSlice.getExtension().get(0)
								.getAbstractEventExtension().getValue();

						DateFormat timestampFormater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
						timestampFormater.setTimeZone(TimeZone.getTimeZone("UTC"));

						this.issuedTimestamp = new Timestamp(
								timestampFormater.parse(notam.getIssued().getValue().getValue().toString()).getTime());
						if (!eventExtension.getLastUpdated().isNil()) {
							this.updatedTimestamp = new Timestamp(timestampFormater
									.parse(eventExtension.getLastUpdated().getValue().getValue().toString()).getTime());
						}

						if (validTime.getBeginPosition().getValue().size() != 0) {
							this.validFromTimestamp = new Timestamp(
									timestampFormater.parse(validTime.getBeginPosition().getValue().get(0)).getTime());
						}

						if (validTime.getEndPosition().getValue().size() != 0) {
							this.validToTimestamp = new Timestamp(
									timestampFormater.parse(validTime.getEndPosition().getValue().get(0)).getTime());
						}

						this.classification = eventExtension.getClassification().getValue();
						this.notamAccountability = eventExtension.getAccountId().getValue().getValue();
						this.locationDesignator = notam.getLocation().getValue().getValue();
						this.notamText = notam.getText().getValue().getValue();

						if ((this.classification.equals("INTL") || this.classification.equals("MIL")
								|| this.classification.equals("LMIL"))
								&& notam.getEffectiveEnd().getValue().getValue().contains("EST")) {
							this.validToTimestamp = null;
						}
					}
				}

				this.aixmNotamMessage = xmlMessage;
			}
		} catch (JAXBException | ParseException e) {
			throw new FnsMessageParseException("Failed to create FnsMessage message due to: " + e.getMessage(), e);
		} finally {
			reader.close();
		}
	}

	public FnsMessage(final int fns_id, final long correlationId, final Timestamp issuedTimestamp,
			final Timestamp updatedTimestamp, final Timestamp validFromTimestamp, final Timestamp validToTimestamp,
			final String locationDesignator, final String classification, final String notamAccountability,
			final String notamText, final String aixmNotamMessage) throws JAXBException {
		this.fns_id = fns_id;
		this.correlationId = correlationId;
		this.issuedTimestamp = issuedTimestamp;
		this.updatedTimestamp = updatedTimestamp;
		this.validFromTimestamp = validFromTimestamp;
		this.validToTimestamp = validToTimestamp;
		this.locationDesignator = locationDesignator;
		this.classification = classification;
		this.notamAccountability = notamAccountability;
		this.notamText = notamText;
		this.aixmNotamMessage = aixmNotamMessage;
	}

	public static String createAixmBasicMessageCollectionMessage(List<String> aixmNotamStringList) throws JAXBException {

		String xmlString = marshalToXml(new AIXMBasicMessageFeatureCollection(aixmNotamStringList));

		return xmlString;

	}

	private static String marshalToXml(AIXMBasicMessageFeatureCollection messageCollection) {
		StringWriter sw = new StringWriter();
		System.out.println("MessageCollection " + messageCollection.getFeatureMember().size());
		try {
			AIXMBasicMessageFeatureCollection.getMarshaller().marshal(messageCollection, sw);
			return sw.toString();
		} catch (JAXBException e) {
			logger.error("Error marshaling: " + e.getMessage());
			return null;
		}
	}

	public static AIXMBasicMessageType createFnsAixmMessage(String fnsAixmMessage) throws JAXBException {
		StringReader reader = new StringReader(fnsAixmMessage);

		AIXMBasicMessageType fnsAixmMessageType = null;
		try {
			fnsAixmMessageType = (AIXMBasicMessageType) JAXBIntrospector
					.getValue(AixmUtilities.createAixmUnmarshaller().unmarshal(reader));
		} catch (JAXBException e) {
			throw e;
		} finally {
			reader.close();
		}

		return fnsAixmMessageType;

	}

	public static AIXMBasicMessageType createFnsAixmMessage(Node fnsAixmMessage) throws JAXBException {
		return (AIXMBasicMessageType) JAXBIntrospector
				.getValue(AixmUtilities.createAixmUnmarshaller().unmarshal(fnsAixmMessage));

	}
	
	@SuppressWarnings("serial")
	public class FnsMessageParseException extends Exception
	{
		 public FnsMessageParseException(String message, Exception e)
		 {
		  super(message, e);
		 }
	}

	// getters
	public int getFNS_ID() {
		return this.fns_id;
	}

	public long getCorrelationId() {
		return this.correlationId;
	}

	public Timestamp getIssuedTimestamp() {
		return this.issuedTimestamp;
	}

	public Timestamp getUpdatedTimestamp() {
		return this.updatedTimestamp;
	}

	public Timestamp getValidFromTimestamp() {
		return this.validFromTimestamp;
	}

	public Timestamp getValidToTimestamp() {
		return this.validToTimestamp;
	}

	public String getClassification() {
		return this.classification;
	}

	public String getNotamAccountability() {
		return this.notamAccountability;
	}

	public String getNotamText() {
		return this.notamText;
	}

	public String getAixmNotamMessage() {
		return this.aixmNotamMessage;
	}

	public String getLocationDesignator() {
		return this.locationDesignator;
	}

	public NotamStatus getStatus() {
		return this.status;
	}

	// setters
	public void setFNS_ID(final int fnsId) {
		this.fns_id = fnsId;
	}

	public void setCorrelationId(final long correlationId) {
		this.correlationId = correlationId;
	}

	public void setIssuedTimestamp(final Timestamp issuedTimestamp) {
		this.issuedTimestamp = issuedTimestamp;
	}

	public void setUpdatedTimestamp(final Timestamp updatedTimestamp) {
		this.updatedTimestamp = updatedTimestamp;
	}

	public void setValidFromTimestamp(final Timestamp validFromTimestamp) {
		this.validFromTimestamp = validFromTimestamp;
	}

	public void setValidToTimestamp(final Timestamp validToTimestamp) {
		this.validToTimestamp = validToTimestamp;
	}

	public void setClassification(final String classification) {
		this.classification = classification;
	}

	public void setNotamAccountability(final String notamAccountability) {
		this.notamAccountability = notamAccountability;
	}

	public void setNotamText(final String notamText) {
		this.notamText = notamText;
	}

	public void setAixmNotamMessage(final String aixmNotamMessage) {
		this.aixmNotamMessage = aixmNotamMessage;
	}

	public void setLocationDesignator(final String locationDesignator) {
		this.locationDesignator = locationDesignator;
	}

	public void setStatus(final NotamStatus status) {
		this.status = status;
	}
}