package us.dot.faa.swim.fns.aixm;

import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.opengis.gml.FeatureArrayPropertyType;
import net.opengis.gml.FeatureCollectionType;
import net.opengis.gml.ObjectFactory;
import us.dot.faa.swim.fns.FnsMessage;

@XmlRootElement(namespace="http://www.opengis.net/gml/3.2", name = "FeatureCollection")
@XmlAccessorType(XmlAccessType.FIELD)
public class AIXMBasicMessageFeatureCollection extends FeatureCollectionType {
    private static final Log logger = LogFactory.getLog(AIXMBasicMessageFeatureCollection.class);
    private static JAXBContext jaxb_FNSNOTAM_Context = null;

    static {
        try {
            jaxb_FNSNOTAM_Context = JAXBContext.newInstance(AIXMBasicMessageFeatureCollection.class);
        } catch (JAXBException e) {
             logger.error("Failed creating JAXB Context for AIXMBasicMessageTypeCollection: " + e.getMessage());            
        }
    }

    public static Marshaller getMarshaller() {
        try {
            return jaxb_FNSNOTAM_Context.createMarshaller();
        } catch (JAXBException e) {
            logger.error("Failed creating JAXB Marshallers for AIXMBasicMessageTypeCollection: " + e.getMessage());   
            return null;   
        }
    }

    public static Unmarshaller getUnmarshaller() {
        try {
            return jaxb_FNSNOTAM_Context.createUnmarshaller();
        } catch (JAXBException e) {
            logger.error("Failed creating JAXB Marshallers for AIXMBasicMessageTypeCollection: " + e.getMessage());   
            return null;   
        }
    }

    public AIXMBasicMessageFeatureCollection() {
        
    }

    public AIXMBasicMessageFeatureCollection(String xmlString) {
        
    }

    public AIXMBasicMessageFeatureCollection(List<String> aixmNotamStringList) throws JAXBException {
        final aero.aixm.message.ObjectFactory of = new aero.aixm.message.ObjectFactory();

        final ObjectFactory gmlOf = new ObjectFactory();
        FeatureCollectionType fc = gmlOf.createFeatureCollectionType();
        FeatureArrayPropertyType fa = gmlOf.createFeatureArrayPropertyType();

        for(String aixmNotam : aixmNotamStringList)
        {
        	fa.getAbstractFeature().add(of.createAIXMBasicMessage(FnsMessage.createFnsAixmMessage(aixmNotam)));  
        }        

        fc.setFeatureMembers(fa);

        this.setFeatureMembers(fa);
    }
}
