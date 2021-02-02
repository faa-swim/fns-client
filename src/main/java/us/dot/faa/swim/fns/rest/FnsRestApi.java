package us.dot.faa.swim.fns.rest;

import static spark.Spark.get;
import static spark.Spark.after;
import static spark.Spark.before;
import static spark.Spark.stop;

import java.io.StringReader;
import java.util.AbstractMap;
import java.util.concurrent.TimeUnit;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import spark.Spark;
import us.dot.faa.swim.fns.notamdb.NotamDb;

public class FnsRestApi {
	private static final Logger logger = LoggerFactory.getLogger(FnsRestApi.class);
    
	private NotamDb notamDb = null;
	
    public FnsRestApi(NotamDb notamDb, int port) {
    	
    	this.notamDb = notamDb;
    	
    	Spark.port(port);
    	
        Spark.staticFiles.location("/public");

        before((request, response) -> {
            logger.info("Recieved Request at: " + request.url() + " from:" + request.ip());
        });

        after((request, response) -> {

            response.header("Content-Encoding", "gzip");

            if (request.queryParams().contains("responseFormat")
                    && request.queryParams("responseFormat").toString().equals("json")) {
                response.type("application/json");
                JSONObject xmlJSONObj = XML.toJSONObject(response.body());
                response.body(xmlJSONObj.toString(4));
            }

            if (request.queryParams().contains("responseFormat")
                    && request.queryParams("responseFormat").toString().equals("notamTable")) {
                response.type("application/json");

                InputSource resultSource = new InputSource(new StringReader(response.body()));

                XPathFactory xpathFactory = XPathFactory.newInstance();
                XPath xpath = xpathFactory.newXPath();
                XPathExpression eventTimeSlice_XpathExpr = xpath.compile("//*[local-name()='EventTimeSlice']");
                NodeList eventTimeSliceNodes = (NodeList) eventTimeSlice_XpathExpr.evaluate(resultSource, XPathConstants.NODESET);                

                JSONArray notamTable = new JSONArray();
                for (int i = 0; i < eventTimeSliceNodes.getLength(); i++) {
                    JSONObject  notam = new JSONObject();
                    Node eventTimeSliceNode = eventTimeSliceNodes.item(i);
                    Node notamNode = null;
                    Node fnsExtensionNode = null;

                    for (int n = 0; n < eventTimeSliceNode.getChildNodes().getLength(); n++) {

                        if(eventTimeSliceNode.getChildNodes().item(n).getLocalName().equals("textNOTAM"))
                        {
                            notamNode = eventTimeSliceNode.getChildNodes().item(n).getFirstChild();
                        }
                        if(eventTimeSliceNode.getChildNodes().item(n).getLocalName().equals("extension"))
                        {
                            fnsExtensionNode = eventTimeSliceNode.getChildNodes().item(n).getFirstChild();
                        }
                    }
                    
                    if(notamNode == null)
                    {
                        continue;
                    }

                    String id = notamNode.getAttributes().getNamedItemNS("http://www.opengis.net/gml/3.2", "id").getNodeValue();
                    notam.put("id", id);
                   for (int ii = 0; ii < notamNode.getChildNodes().getLength(); ii++) {                    
                       
                        NodeList items = notamNode.getChildNodes();                        
                        
                        String name = items.item(ii).getLocalName();
                        String value = items.item(ii).getTextContent();   
                        
                        notam.put(name, value);
                    }    

                    for (int ii = 0; ii < fnsExtensionNode.getChildNodes().getLength(); ii++) {                    
                       
                        NodeList items = fnsExtensionNode.getChildNodes();                        
                        
                        String name = items.item(ii).getLocalName();
                        String value = items.item(ii).getTextContent();   
                        
                        notam.put(name, value);
                    } 

                    notamTable.put(notam);                 
                }

                System.out.println("Provided "+ notamTable.length());;
                response.body(notamTable.toString(1));
            }

        });

        get("/locationDesignator/:id", (req, res) -> {
            long startTime = System.nanoTime();

            String toReturn = this.notamDb.getByLocationDesignator(req.params(":id")).getValue();

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for Location Designator: " + req.params(":id") + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");
            return toReturn;
        });

        get("/classification/:id", (req, res) -> {
            long startTime = System.nanoTime();
            res.type("application/xml");

            int lastFnsId = 0;
            if(req.headers().contains("lastFnsId"))
            {
            	lastFnsId = Integer.parseInt(req.headers("lastFnsId"));
            }
            
            String toReturn = "";
            AbstractMap.SimpleEntry<Integer,String> results = this.notamDb.getByClassification(req.params(":id"),lastFnsId);                        
            toReturn = results.getValue();                
            lastFnsId = results.getKey();
            
            res.header("lastFnsId", Integer.toString(lastFnsId) );                       

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for Classification Designator: " + req.params(":id") + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");
            return toReturn;
        });

        get("/delta/:deltaTime", (req, res) -> {
            long startTime = System.nanoTime();
            res.type("application/xml");

            String deltaTime = req.params("deltaTime");

            int lastFnsId = 0;
            if(req.headers().contains("lastFnsId"))
            {
            	lastFnsId = Integer.parseInt(req.headers("lastFnsId"));
            }
            
            String toReturn = "";
            AbstractMap.SimpleEntry<Integer,String> results = this.notamDb.getDelta(deltaTime, lastFnsId);
            
            toReturn = results.getValue();                
            lastFnsId = results.getKey();
            
            res.header("lastFnsId", Integer.toString(lastFnsId) );               

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for Delta From: " + req.params(":deltaTime") + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            if (deltaTime != null) {
                try {
                    return toReturn;
                } catch (IllegalArgumentException e) {
                    return e.getMessage();
                }
            } else {
                return "";
            }
        });

        get("/timerange/:from/:to", (req, res) -> {
            long startTime = System.nanoTime();
            
            res.type("application/xml");

            String fromDateTime = req.params(":from");
            String toDateTime = req.params("to");    
            
            int lastFnsId = 0;
            if(req.headers().contains("lastFnsId"))
            {
            	lastFnsId = Integer.parseInt(req.headers("lastFnsId"));
            }
            
            String toReturn = "";
            AbstractMap.SimpleEntry<Integer,String> results = this.notamDb.getByTimeRange(fromDateTime, toDateTime, lastFnsId);
            
            toReturn = results.getValue();                
            lastFnsId = results.getKey();
            
            res.header("lastFnsId", Integer.toString(lastFnsId) );                       

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;

            logger.info("Processed rest request for Time Range From:" + fromDateTime +" To:"  + toDateTime +" ,took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            if (fromDateTime != null && toDateTime != null) {
                try {
                    return toReturn;
                } catch (IllegalArgumentException e) {
                    return e.getMessage();
                }
            } else {
                return "";
            }
        });

        get("/allNotams", (req, res) -> {
            long startTime = System.nanoTime();
            res.type("application/xml");
            
            int lastFnsId = 0;
            if(req.headers().contains("lastFnsId"))
            {
            	lastFnsId = Integer.parseInt(req.headers("lastFnsId"));
            }
            
            AbstractMap.SimpleEntry<Integer,String> results = this.notamDb.getAllNotams(lastFnsId);
            
            String toReturn = results.getValue();
            
            lastFnsId = results.getKey();
            
            res.header("lastFnsId", Integer.toString(lastFnsId) );

            if (toReturn != null) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for All Notams took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return toReturn;
        });

    }

    public void terminate() {
        stop();
    }
        
}