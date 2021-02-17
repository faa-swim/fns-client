package us.dot.faa.swim.fns.rest;

import static spark.Spark.get;
import static spark.Spark.after;
import static spark.Spark.before;
import static spark.Spark.stop;
import static spark.Spark.halt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import spark.Request;
import spark.Response;
import spark.Spark;
import spark.http.matching.Halt;
import spark.HaltException;
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
            if(!this.notamDb.isValid()){
                
                halt(503, "NotamDb State InValid");
            }
        });

        after((request, response) -> {

            if (request.headers("Content-Encoding") != null && request.headers("Content-Encoding").contains("gzip")) {
                response.header("Content-Encoding", "gzip");
            }
        });

        get("/notamTable/:id", (req, res) -> {

            long startTime = System.nanoTime();
            Boolean success = false;
            String locationDesignator = req.params(":id");

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                this.notamDb.getByLocationDesignator(locationDesignator, outputStream, false);
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            XPathExpression eventTimeSlice_XpathExpr = xpath.compile("//*[local-name()='EventTimeSlice']");

            DocumentBuilderFactory db = DocumentBuilderFactory.newInstance();
            db.setNamespaceAware(true);
            Document doc = db.newDocumentBuilder().parse(new ByteArrayInputStream(outputStream.toByteArray()));
            NodeList eventTimeSliceNodes = (NodeList) eventTimeSlice_XpathExpr
                    .evaluate(doc.getDocumentElement(), XPathConstants.NODESET);

            JSONArray notamTable = new JSONArray();
            for (int i = 0; i < eventTimeSliceNodes.getLength(); i++) {
                JSONObject notam = new JSONObject();
                Node eventTimeSliceNode = eventTimeSliceNodes.item(i);
                Node notamNode = null;
                Node fnsExtensionNode = null;

                int childNodeLength = eventTimeSliceNode.getChildNodes().getLength();
                for (int n = 0; n < childNodeLength; n++) {

                    NodeList childNodes = eventTimeSliceNode.getChildNodes();
                    if (childNodes.item(n).getLocalName().equals("textNOTAM")) {
                        notamNode = eventTimeSliceNode.getChildNodes().item(n).getFirstChild();
                    }
                    if (childNodes.item(n).getLocalName().equals("extension")) {
                        fnsExtensionNode = eventTimeSliceNode.getChildNodes().item(n).getFirstChild();
                    }
                }

                if (notamNode == null) {
                    continue;
                }

                String id = notamNode.getAttributes().getNamedItemNS("http://www.opengis.net/gml/3.2", "id")
                        .getNodeValue();
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

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for for LocationDesignator:" + locationDesignator + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");
            
            return notamTable.toString();

        });

        get("/locationDesignator/:id", (req, res) -> {
            long startTime = System.nanoTime();
            Boolean success = false;
            String locationDesignator = req.params(":id");

            try {
                this.notamDb.getByLocationDesignator(locationDesignator, res.raw().getOutputStream(), asJson(req, res));
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for for LocationDesignator:" + locationDesignator + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return "";
        });

        get("/classification/:id", (req, res) -> {
            long startTime = System.nanoTime();
            Boolean success = false;
            String classification = req.params(":id");

            try {
                this.notamDb.getByClassification(classification, res.raw().getOutputStream(), asJson(req, res));
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for for Classification:" + classification + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return "";
        });

        get("/delta/:deltaTime", (req, res) -> {
            long startTime = System.nanoTime();

            String deltaTime = req.params("deltaTime");

            Boolean success = false;

            try {
                this.notamDb.getDelta(deltaTime, res.raw().getOutputStream(), asJson(req, res));
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for Delta: " + deltaTime + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return "";

        });

        get("/timerange/:from/:to", (req, res) -> {
            long startTime = System.nanoTime();

            String fromDateTime = req.params(":from");
            String toDateTime = req.params("to");

            Boolean success = false;

            try {
                this.notamDb.getByTimeRange(fromDateTime, toDateTime, res.raw().getOutputStream(), asJson(req, res));
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for Time Range: " + fromDateTime + "-" + toDateTime + " took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return "";
        });

        get("/allNotams", (req, res) -> {
            long startTime = System.nanoTime();

            Boolean success = false;

            try {
                notamDb.getAllNotams(res.raw().getOutputStream(), asJson(req, res));
                success = true;
            } catch (Exception e) {
                logger.error("Failed to process rest request ", e.getMessage());
            }

            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            if (success) {
                res.status(200);
            } else {
                res.status(500);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("Processed rest request for All Notams took:"
                    + TimeUnit.MILLISECONDS.convert(totalTime, TimeUnit.NANOSECONDS) + " ms");

            return "";
        });

    }

    private boolean asJson(Request req, Response res) {
        boolean asJson = false;
        if (req.queryParams().contains("responseFormat")
                && req.queryParams("responseFormat").toString().equals("json")) {
            res.type("application/json");
            asJson = true;
        } else {
            res.type("application/xml");
        }
        return asJson;
    }

    public void terminate() {
        stop();
    }

}