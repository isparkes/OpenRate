/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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

package OpenRate.parser;

import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ian
 */
public class ASN1ParserTest {
  
  public ASN1ParserTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of ready method, of class ASN1Parser. Initialises bytes and reads them
   * out one at a time.
   */
  @Test
  public void testReady() {
    System.out.println("ready");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Put some data in the parser
    byte[] testData = new byte[1234];
    int idx;
    for (idx = 0 ; idx < testData.length ; idx++)
      testData[idx] = (byte) idx;
    instance.setDataToParse(testData);
    
    boolean result;
    
    // Ready before the first byte
    result = instance.ready();
    assertEquals(true, result);
    
    // Ready after the first byte
    instance.readBlock(1);
    result = instance.ready();
    assertEquals(true, result);

    // get 1232 more
    for (idx = 0 ; idx < 1232 ; idx++)
    {
      instance.readBlock(1);
    }
    
    // ready at the last byte
    result = instance.ready();
    assertEquals(true, result);
    
    // not ready any more
    instance.readBlock(1);
    result = instance.ready();
    assertEquals(false, result);
  }

  /**
   * Test of ready method, of class ASN1Parser. Initialises bytes and reads them
   * out in chunks
   */
  @Test
  public void testReadyBlock() {
    System.out.println("ready block");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Put some data in the parser
    byte[] testData = new byte[1234];
    int idx;
    for (idx = 0 ; idx < testData.length ; idx++)
      testData[idx] = (byte) idx;
    instance.setDataToParse(testData);
    
    boolean result;
    
    // Ready before the first byte
    result = instance.ready();
    assertEquals(true, result);
    
    // Ready after the first byte
    instance.readBlock(1);
    result = instance.ready();
    assertEquals(true, result);

    // ready at the last byte
    instance.readBlock(1232);
    result = instance.ready();
    assertEquals(true, result);
    
    // not ready any more
    instance.readBlock(1);
    result = instance.ready();
    assertEquals(false, result);
  }
  
  /**
   * Test of parseASN1Name method, of class ASN1Parser.
   */
  @Test
  public void testParseASN1Name() {
    System.out.println("parseASN1Name");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // check the name of a known tag
    String result = instance.parseASN1Name(0);
    assertEquals("Tag0", result);
    
    // check the name of an unknown tag
    result = instance.parseASN1Name(100);
    assertEquals("", result);    
  }

  /**
   * Test of getType method, of class ASN1Parser.
   */
  @Test
  public void testGetType() {
    System.out.println("getType");
  }

  /**
   * Test of getTypeName method, of class ASN1Parser.
   */
  @Test
  public void testGetTypeName() {
    System.out.println("getTypeName");
  }

  /**
   * Test of parseBCDString method, of class ASN1Parser.
   */
  @Test
  public void testParseBCDString() {
    System.out.println("parseBCDString");
  }

  /**
   * Test of parseInteger method, of class ASN1Parser.
   */
  @Test
  public void testParseInteger() {
    System.out.println("parseInteger");
  }

  /**
   * Test of parseIntegerAsInteger method, of class ASN1Parser.
   */
  @Test
  public void testParseIntegerAsInteger() {
    System.out.println("parseIntegerAsInteger");
  }

  /**
   * Test of parsePrintableString method, of class ASN1Parser.
   */
  @Test
  public void testParsePrintableString() {
    System.out.println("parsePrintableString");
  }

  /**
   * Test of parseIA5String method, of class ASN1Parser.
   */
  @Test
  public void testParseIA5String() {
    System.out.println("parseIA5String");
  }

  /**
   * Test of parseBytes method, of class ASN1Parser.
   */
  @Test
  public void testParseBytes_byteArr() {
    System.out.println("parseBytes");
  }

  /**
   * Test of parseBytes method, of class ASN1Parser.
   */
  @Test
  public void testParseBytes_byteArr_int() {
    System.out.println("parseBytes");
  }

  /**
   * Test of parseASN1 method, of class ASN1Parser.
   */
  @Test
  public void testParseASN1() throws Exception {
    System.out.println("parseASN1");
  }

  /**
   * Test of readBlock method, of class ASN1Parser.
   */
  @Test
  public void testReadBlock() {
    System.out.println("readBlock");
  }

  /**
   * Test of readNextElement method, of class ASN1Parser.
   */
  @Test
  public void testReadNextElement() throws Exception {
    System.out.println("readNextElement");
  }
  
  /**
   * Test of readNextElement method, of class ASN1Parser.
   */
  @Test
  public void testReadNextElementLength() throws Exception {
    System.out.println("readNextElementLength");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Test 1 byte length
    byte[] testArray1 = {-96,-127,-35};
    instance.setDataToParse(testArray1);
    Asn1Class output1 = instance.readNextElement();
    Assert.assertEquals(221, output1.getLength());
    
    // Test 2 byte length
    byte[] testArray2 = {-96,-126,1,61};
    instance.setDataToParse(testArray2);
    Asn1Class output2 = instance.readNextElement();
    Assert.assertEquals(317, output2.getLength());
    
    // Test 3 byte length
    byte[] testArray3 = {48,-125,1,-97,41};
    instance.setDataToParse(testArray3);
    Asn1Class output3 = instance.readNextElement();
    Assert.assertEquals(106281, output3.getLength());
    
    // Test 4 byte length
    byte[] testArray4 = {48,-124,1,-97,1,41};
    instance.setDataToParse(testArray4);
    Asn1Class output4 = instance.readNextElement();
    Assert.assertEquals(27197737, output4.getLength());
  }

  /**
   * Test of readNextTag method, of class ASN1Parser.
   */
  @Test
  public void testReadNextTag() throws Exception {
    System.out.println("readNextTag");
  }
}