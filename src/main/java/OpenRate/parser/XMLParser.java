/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * The OpenRate Project or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

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