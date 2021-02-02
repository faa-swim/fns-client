package us.dot.faa.swim.fns.aixm;

import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.JAXBIntrospector;
import javax.xml.bind.Unmarshaller;

import org.w3c.dom.Node;

import aero.aixm.message.AIXMBasicMessageType;

public class AixmUtilities {

	private static JAXBContext jaxb_FNSNOTAM_Context = null;

    static
    {
        try {
			jaxb_FNSNOTAM_Context = JAXBContext.newInstance(AIXMBasicMessageType.class);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
    }

    public static Unmarshaller createAixmUnmarshaller() throws JAXBException
    {
        return jaxb_FNSNOTAM_Context.createUnmarshaller();
    }
    
    public static AIXMBasicMessageType createFnsAixmMessage(String fnsAixmMessage) throws JAXBException {
		StringReader reader = new StringReader(fnsAixmMessage);

		AIXMBasicMessageType fnsAixmMessageType = null;
		try {
			fnsAixmMessageType = (AIXMBasicMessageType) JAXBIntrospector
					.getValue(jaxb_FNSNOTAM_Context.createUnmarshaller().unmarshal(reader));
		} catch (JAXBException e) {
			throw e;
		} finally {
			reader.close();
		}

		return fnsAixmMessageType;

	}

	public static AIXMBasicMessageType createFnsAixmMessage(Node fnsAixmMessage) throws JAXBException {
		return (AIXMBasicMessageType) JAXBIntrospector
				.getValue(jaxb_FNSNOTAM_Context.createUnmarshaller().unmarshal(fnsAixmMessage));

	}
}
