

/**
 * afzaal 06-11-2008 initial version
 */
package OpenRate.parser;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * An XML parser
 *
 * @author afzaal
 */
public class XMLParser extends DefaultHandler
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: XMLParser.java,v $, $Revision: 1.11 $, $Date: 2013-05-13 18:12:12 $";

	private static SAXParserFactory factory = SAXParserFactory.newInstance();

  //	Used to store xml tag names
  private ArrayList<String> tmpQNames = new ArrayList<>(5);
  private StringBuffer tmpValue;

  //	Header Identifier to Skip calling SetAttribute of IXMLparser interface
  private String headerIdentifier;

  //	This is the client that needs attributes from xml
  private IXmlParser client;

/**
 * Constructor
 *
 * @param client The client
 */
public XMLParser(IXmlParser client)
  {
		this.client = client;
	}

/**
 * Parse XML
 *
 * @param xmlToParse The XML to parse
 * @param headerIdentifier The header identifier
 * @throws Exception
 */
public void parseXML(String xmlToParse, String headerIdentifier) throws Exception
  {
		if(headerIdentifier == null || headerIdentifier.trim().equals(""))
    {
			headerIdentifier = null;
    }

		this.headerIdentifier = headerIdentifier;
		SAXParser parser;
		parser = factory.newSAXParser();
		parser.parse(new ByteArrayInputStream(xmlToParse.getBytes()), this);
	}

/**
 * Add a start element
 *
 * @param uri
 * @param local
 * @param qname
 * @param atts
 * @throws SAXException
 */
@Override
	public void startElement(String uri, String local, String qname,
	          Attributes atts) throws SAXException
  {
    tmpValue = new StringBuffer("");

    if(headerIdentifier != null && qname.equalsIgnoreCase(headerIdentifier))
    {
      tmpQNames.add(headerIdentifier);
    }
    else if(tmpQNames.isEmpty() && (headerIdentifier== null || !(qname.equalsIgnoreCase(headerIdentifier))))
    {
      tmpQNames.add(qname);
    }
    else if(tmpQNames.size() > 0)
    {
      tmpQNames.add(tmpQNames.get(tmpQNames.size()-1)+"."+qname);
    }
  }

/**
 * Add an end element
 *
 * @param uri
 * @param local
 * @param qname
 * @throws SAXException
 */
@Override
  public void endElement(String uri, String local, String qname) throws SAXException
  {
    if(headerIdentifier == null || !(qname.equalsIgnoreCase(headerIdentifier)))
    {
      client.setAttribute(tmpQNames.get(tmpQNames.size()-1), tmpValue.toString());
      tmpQNames.remove(tmpQNames.size()-1);
      tmpValue = new StringBuffer();
    }
  }

/**
 * Add characters
 *
 * @param ch
 * @param start
 * @param length
 * @throws SAXException
 */
@Override
  public void characters(char[] ch, int start, int length) throws SAXException
  {
    if(ch != null && tmpQNames != null && tmpQNames.size() > 0)
    {
      tmpValue.append(ch, start, length);
    }
  }

/**
 * Start of document
 *
 * @throws SAXException
 */
@Override
	public void startDocument ()
		throws SAXException
  {
	}

/**
 * End of document
 *
 * @throws SAXException
 */
@Override
  public void endDocument()
  throws SAXException
  {
  }

/**
 * Start of prefix mapping
 *
 * @param prefix
 * @param uri
 * @throws SAXException
 */
@Override
  public void startPrefixMapping (String prefix, String uri)
  throws SAXException
  {
  }

/**
 * End of prefix mapping
 *
 * @param prefix
 * @throws SAXException
 */
@Override
  public void endPrefixMapping (String prefix)
  throws SAXException
  {
  }
/**
 * Ignorable whiltespace
 *
 * @param ch
 * @param start
 * @param length
 * @throws SAXException
 */
@Override
  public void ignorableWhitespace (char ch[], int start, int length)
  throws SAXException
  {
  }

/**
 * Processing instruction
 *
 * @param target
 * @param data
 * @throws SAXException
 */
@Override
  public void processingInstruction (String target, String data)
  throws SAXException
  {
  }

/**
 * Skipped entity
 *
 * @param name
 * @throws SAXException
 */
@Override
  public void skippedEntity (String name)
  throws SAXException
  {
  }

/**
 * Set document locator
 *
 * @param locator
 */
@Override
  public void setDocumentLocator (Locator locator)
  {
  }
}