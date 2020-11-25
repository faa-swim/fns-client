package us.dot.faa.swim.utilities;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class XmlSplitterSaxParser implements ContentHandler {

        private static final String XML_PROLOGUE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        private final XmlElementNotifier notifier;
        private final int splitDepth;
        private final StringBuilder sb = new StringBuilder(XML_PROLOGUE);
        private int depth = 0;
        private Map<String, String> prefixMap = new TreeMap<>();

        public XmlSplitterSaxParser(XmlElementNotifier notifier, int splitDepth) {
            this.notifier = notifier;
            this.splitDepth = splitDepth;
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            // if we're not at a level where we care about capturing text, then return
            if (depth <= splitDepth) {
                return;
            }

            // capture text
            for (int i = start; i < start + length; i++) {
                char c = ch[i];
                switch (c) {
                    case '<':
                        sb.append("&lt;");
                        break;
                    case '>':
                        sb.append("&gt;");
                        break;
                    case '&':
                        sb.append("&amp;");
                        break;
                    case '\'':
                        sb.append("&apos;");
                        break;
                    case '"':
                        sb.append("&quot;");
                        break;
                    default:
                        sb.append(c);
                        break;
                }
            }
        }

        @Override
        public void endDocument() throws SAXException {
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            // We have finished processing this element. Decrement the depth.
            int newDepth = --depth;

            // if we're at a level where we care about capturing text, then add the closing element
            if (newDepth >= splitDepth) {
                // Add the element end tag.
                sb.append("</").append(qName).append(">");
            }

            // If we have now returned to level 1, we have finished processing
            // a 2nd-level element. Send notification with the XML text and
            // erase the String Builder so that we can start
            // processing a new 2nd-level element.
            if (newDepth == splitDepth) {
                String elementTree = sb.toString();
                notifier.onXmlElementFound(elementTree);
                // Reset the StringBuilder to just the XML prolog.
                sb.setLength(XML_PROLOGUE.length());
            }
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            prefixMap.remove(prefixToNamespace(prefix));
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
        }

        @Override
        public void startDocument() throws SAXException {
        }

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes atts) throws SAXException {
            // Increment the current depth because start a new XML element.
            int newDepth = ++depth;
            // Output the element and its attributes if it is
            // not the root element.
            if (newDepth > splitDepth) {
                sb.append("<");
                sb.append(qName);

                final Set<String> attributeNames = new HashSet<>();
                int attCount = atts.getLength();
                for (int i = 0; i < attCount; i++) {
                    String attName = atts.getQName(i);
                    attributeNames.add(attName);
                    String attValue = StringEscapeUtils.escapeXml(atts.getValue(i));
                    sb.append(" ").append(attName).append("=").append("\"").append(attValue).append("\"");
                }

                // If this is the first node we're outputting write out
                // any additional namespace declarations that are required
                if (splitDepth == newDepth - 1) {
                    for (Entry<String, String> entry : prefixMap.entrySet()) {
                        // If we've already added this namespace as an attribute then continue
                        if (attributeNames.contains(entry.getKey())) {
                            continue;
                        }
                        sb.append(" ");
                        sb.append(entry.getKey());
                        sb.append("=\"");
                        sb.append(entry.getValue());
                        sb.append("\"");
                    }
                }

                sb.append(">");
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            final String ns = prefixToNamespace(prefix);
            prefixMap.put(ns, uri);
        }

        private String prefixToNamespace(String prefix) {
            final String ns;
            if (prefix.length() == 0) {
                ns = "xmlns";
            } else {
                ns="xmlns:"+prefix;
            }
            return ns;
        }

        public interface XmlElementNotifier {

            public void onXmlElementFound(String xmlTree);
        }
    }