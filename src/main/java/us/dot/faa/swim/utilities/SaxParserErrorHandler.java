package us.dot.faa.swim.utilities;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class SaxParserErrorHandler implements ErrorHandler {

    private boolean isValid = true;

    public boolean isValid() {
        return this.isValid;
    }

	@Override
	public void warning(SAXParseException exception) throws SAXException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void error(SAXParseException exception) throws SAXException {
		this.isValid = false;
		throw exception;
	}

	@Override
	public void fatalError(SAXParseException exception) throws SAXException {
		this.isValid = false;
        throw exception;
		
	}
}