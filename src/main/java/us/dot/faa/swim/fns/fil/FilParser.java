package us.dot.faa.swim.fns.fil;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import us.dot.faa.swim.utilities.xml.SaxParserErrorHandler;
import us.dot.faa.swim.utilities.xml.XmlSplitterSaxParser;

public class FilParser {

    final ThreadPoolExecutor executor;

    public FilParser(int workerThreadCount, int workQueueSize)
    {
        executor = new ThreadPoolExecutor(workerThreadCount, workerThreadCount, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(workQueueSize), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public int getPendingJobCount()
    {
        return executor.getQueue().size();
    }

    public void parseFilFile(InputStream filInputStream, FilParserWorker worker) throws Exception {        

        final XmlSplitterSaxParser parser = new XmlSplitterSaxParser(msg -> {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    worker.processesMessage(msg);
                }
            });
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
        xmlReader.parse(new InputSource(filInputStream));
        if (!parsingErrorHandeler.isValid()) {
            throw new Exception("Failed to Parse");
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

}
