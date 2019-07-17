

package OpenRate.parser;

import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests the ASN.1 decoding ability of the inbuilt ASN.1 parser. ASN.1 parsing
 * is a bit tricky, but with the right tools not so hard. The parser serves to
 * give the tool to more or less easily parse ASN.1 streams. 
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
   * out one at a time, until there are no bytes left to read. This is to 
   * ensure that we accurately read steams even with a mixture of single byte
   * and block reads.
   * 
   * This method concentrates on reading single bytes.
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
   * out one at a time, until there are no bytes left to read. This is to 
   * ensure that we accurately read steams even with a mixture of single byte
   * and block reads.
   * 
   * This method concentrates on reading blocks.
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
  //@Test
  public void testParseASN1Name() {
    System.out.println("parseASN1Name");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    
    // check the name of a known tag
    String result = asn1Specification.getTagName("00;83");
    assertEquals("MSISDN", result);
    
    // check the name of an unknown tag
    result = asn1Specification.getTagName("youdontknowme");
    assertEquals("", result);
  }

  /**
   * Test of getType method, of class ASN1Parser. This reads the name of a 
   * tag from the specification file.
   */
  @Test
  public void testGetType() {
    System.out.println("getType");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    
    // check the type name of a known tag
    int result = asn1Specification.getTagType("00;83");
    assertEquals(ASN1Parser.BCDString, result);
    
    // check the name of an unknown tag
    result = asn1Specification.getTagType("youdontknowme");
    assertEquals(-1, result);
  }

  /**
   * Test of parseBCDString method, of class ASN1Parser.
   */
  @Test
  public void testParseBCDString() throws Exception {
    System.out.println("parseBCDString");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Test 2 byte length
    byte[] testArray = new byte[10];
    testArray[0] = (byte) -126;
    testArray[1] = (byte) 8	;
    testArray[2] = (byte) 38	;
    testArray[3] = (byte) 2	;
    testArray[4] = (byte) 3	;
    testArray[5] = (byte) 17	;
    testArray[6] = (byte) 70	;
    testArray[7] = (byte) 9	;
    testArray[8] = (byte) 18	;
    testArray[9] = (byte) -9	;

    instance.setDataToParse(testArray);
    Asn1Class output = instance.readNextElement();
    
    // perform the conversion
    String result = instance.parseASN1(ASN1Parser.BCDString, output.getOrigValue());
    String expectedResult = "260203114609127";
    Assert.assertEquals(expectedResult, result);
  }

  /**
   * Test of parseBCDString method, of class ASN1Parser.
   * (Little endian BCD as used by Ericsson AXE).
   */
  @Test
  public void testParseBCDStringLE() throws Exception {
    System.out.println("parseBCDString");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Test 2 byte length
    byte[] testArray = new byte[10];
    testArray[0] = (byte) -126;
    testArray[1] = (byte) 8	;
    testArray[2] = (byte) 38	;
    testArray[3] = (byte) 2	;
    testArray[4] = (byte) 3	;
    testArray[5] = (byte) 17	;
    testArray[6] = (byte) 70	;
    testArray[7] = (byte) 9	;
    testArray[8] = (byte) 18	;
    testArray[9] = (byte) -9	;

    instance.setDataToParse(testArray);
    Asn1Class output = instance.readNextElement();
    
    // perform the conversion
    String result = instance.parseASN1(ASN1Parser.BCDStringLE, output.getOrigValue());
    String expectedResult = "622030116490217";
    Assert.assertEquals(expectedResult, result);
  }

  /**
   * Test the parsing of a real (captured) CDR.
   */
  @Test
  public void testParseCDR() throws Exception {
    System.out.println("parseCDR");
    
    Asn1Class output;
    StringBuilder recordContents = new StringBuilder();
    
    byte[] testCDR = new byte[162];
    
    testCDR[ 0] = (byte) -128  ;
    testCDR[ 1] = (byte) 1     ;
    testCDR[ 2] = (byte) 7     ;
    testCDR[ 3] = (byte) -127  ;
    testCDR[ 4] = (byte) 7     ;
    testCDR[ 5] = (byte) -111  ;
    testCDR[ 6] = (byte) 50    ;
    testCDR[ 7] = (byte) -107  ;
    testCDR[ 8] = (byte) -103  ;
    testCDR[ 9] = (byte) -103  ;
    testCDR[10] = (byte) 25    ;
    testCDR[11] = (byte) -16   ;
    testCDR[12] = (byte) -126  ;
    testCDR[13] = (byte) 8     ;
    testCDR[14] = (byte) 38    ;
    testCDR[15] = (byte) 2     ;
    testCDR[16] = (byte) 3     ;
    testCDR[17] = (byte) 17    ;
    testCDR[18] = (byte) 70    ;
    testCDR[19] = (byte) 9     ;
    testCDR[20] = (byte) 18    ;
    testCDR[21] = (byte) -9    ;
    testCDR[22] = (byte) -125  ;
    testCDR[23] = (byte) 8     ;
    testCDR[24] = (byte) 83    ;
    testCDR[25] = (byte) 20    ;
    testCDR[26] = (byte) 84    ;
    testCDR[27] = (byte) 64    ;
    testCDR[28] = (byte) 36    ;
    testCDR[29] = (byte) 96    ;
    testCDR[30] = (byte) 73    ;
    testCDR[31] = (byte) -16   ;
    testCDR[32] = (byte) -124  ;
    testCDR[33] = (byte) 7     ;
    testCDR[34] = (byte) -111  ;
    testCDR[35] = (byte) 50    ;
    testCDR[36] = (byte) -107  ;
    testCDR[37] = (byte) 35    ;
    testCDR[38] = (byte) -121  ;
    testCDR[39] = (byte) 82    ;
    testCDR[40] = (byte) -13   ;
    testCDR[41] = (byte) -123  ;
    testCDR[42] = (byte) 3     ;
    testCDR[43] = (byte) 51    ;
    testCDR[44] = (byte) 89    ;
    testCDR[45] = (byte) -128  ;
    testCDR[46] = (byte) -122  ;
    testCDR[47] = (byte) 7     ;
    testCDR[48] = (byte) -111  ;
    testCDR[49] = (byte) 50    ;
    testCDR[50] = (byte) -107  ;
    testCDR[51] = (byte) -103  ;
    testCDR[52] = (byte) -103  ;
    testCDR[53] = (byte) 9     ;
    testCDR[54] = (byte) -15   ;
    testCDR[55] = (byte) -89   ;
    testCDR[56] = (byte) 8     ;
    testCDR[57] = (byte) -128  ;
    testCDR[58] = (byte) 2     ;
    testCDR[59] = (byte) 36    ;
    testCDR[60] = (byte) 84    ;
    testCDR[61] = (byte) -127  ;
    testCDR[62] = (byte) 2     ;
    testCDR[63] = (byte) 6     ;
    testCDR[64] = (byte) -99   ;
    testCDR[65] = (byte) -120  ;
    testCDR[66] = (byte) 9     ;
    testCDR[67] = (byte) 18    ;
    testCDR[68] = (byte) 18    ;
    testCDR[69] = (byte) 49    ;
    testCDR[70] = (byte) 35    ;
    testCDR[71] = (byte) 67    ;
    testCDR[72] = (byte) 72    ;
    testCDR[73] = (byte) 43    ;
    testCDR[74] = (byte) 1     ;
    testCDR[75] = (byte) 0     ;
    testCDR[76] = (byte) -117  ;
    testCDR[77] = (byte) 1     ;
    testCDR[78] = (byte) 2     ;
    testCDR[79] = (byte) -65   ;
    testCDR[80] = (byte) -127  ;
    testCDR[81] = (byte) 2     ;
    testCDR[82] = (byte) 3     ;
    testCDR[83] = (byte) -125  ;
    testCDR[84] = (byte) 1     ;
    testCDR[85] = (byte) 33    ;
    testCDR[86] = (byte) -65   ;
    testCDR[87] = (byte) -127  ;
    testCDR[88] = (byte) 5     ;
    testCDR[89] = (byte) 3     ;
    testCDR[90] = (byte) -128  ;
    testCDR[91] = (byte) 1     ;
    testCDR[92] = (byte) 2     ;
    testCDR[93] = (byte) -97   ;
    testCDR[94] = (byte) -127  ;
    testCDR[95] = (byte) 13    ;
    testCDR[96] = (byte) 1     ;
    testCDR[97] = (byte) 1     ;
    testCDR[98] = (byte) -97   ;
    testCDR[99] = (byte) -127  ;
    testCDR[100] = (byte) 39   ;
    testCDR[101] = (byte) 2    ;
    testCDR[102] = (byte) 43   ;
    testCDR[103] = (byte) 9    ;
    testCDR[104] = (byte) -97  ;
    testCDR[105] = (byte) -127 ;
    testCDR[106] = (byte) 40   ;
    testCDR[107] = (byte) 2    ;
    testCDR[108] = (byte) -104 ;
    testCDR[109] = (byte) 8    ;
    testCDR[110] = (byte) -97  ;
    testCDR[111] = (byte) -127 ;
    testCDR[112] = (byte) 60   ;
    testCDR[113] = (byte) 7    ;
    testCDR[114] = (byte) 38   ;
    testCDR[115] = (byte) -14  ;
    testCDR[116] = (byte) 48   ;
    testCDR[117] = (byte) 36   ;
    testCDR[118] = (byte) 84   ;
    testCDR[119] = (byte) 6    ;
    testCDR[120] = (byte) -99  ;
    testCDR[121] = (byte) -97  ;
    testCDR[122] = (byte) -127 ;
    testCDR[123] = (byte) 62   ;
    testCDR[124] = (byte) 1    ;
    testCDR[125] = (byte) 10   ;
    testCDR[126] = (byte) -97  ;
    testCDR[127] = (byte) -127 ;
    testCDR[128] = (byte) 64   ;
    testCDR[129] = (byte) 3    ;
    testCDR[130] = (byte) 38   ;
    testCDR[131] = (byte) -14  ;
    testCDR[132] = (byte) 48   ;
    testCDR[133] = (byte) -97  ;
    testCDR[134] = (byte) -127 ;
    testCDR[135] = (byte) 67   ;
    testCDR[136] = (byte) 1    ;
    testCDR[137] = (byte) -1   ;
    testCDR[138] = (byte) -97  ;
    testCDR[139] = (byte) -127 ;
    testCDR[140] = (byte) 73   ;
    testCDR[141] = (byte) 3    ;
    testCDR[142] = (byte) -95  ;
    testCDR[143] = (byte) 65   ;
    testCDR[144] = (byte) 65   ;
    testCDR[145] = (byte) -97  ;
    testCDR[146] = (byte) -127 ;
    testCDR[147] = (byte) 74   ;
    testCDR[148] = (byte) 5    ;
    testCDR[149] = (byte) 25   ;
    testCDR[150] = (byte) 10   ;
    testCDR[151] = (byte) 4    ;
    testCDR[152] = (byte) -22  ;
    testCDR[153] = (byte) 103  ;
    testCDR[154] = (byte) -97  ;
    testCDR[155] = (byte) -127 ;
    testCDR[156] = (byte) 104  ;
    testCDR[157] = (byte) 4    ;
    testCDR[158] = (byte) 107  ;
    testCDR[159] = (byte) -1   ;
    testCDR[160] = (byte) -72  ;
    testCDR[161] = (byte) -1   ;
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    asn1Specification.initTags();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Set the data
    instance.setDataToParse(testCDR);
    
    // get the cdr type
    output = instance.readNextElement();
    //System.out.println("CDR type (" + output.getRawTagHex() + "), Length: " + output.getLength() + ", type: " + asnp.parseInteger(output.getOrigValue()));    

    // The CDR Typw is used in controlling the filtering and naming, as well as
    // the type interpretation. Because Huawei does not stick to a 1 tag, 1
    // meaning policy, each cdr must be interpreted differently, which sucks
    String cdrType = instance.parseBytes(output.getOrigValue());
    //System.out.println("CDR type " + asn1Specification.getCDRName(cdrType) + " (" + cdrType + ")");  
    recordContents.append(asn1Specification.getCDRName(cdrType)).append(";");
    
    while (instance.ready())
    {
      // get the cdr header
      output = instance.readNextElement();
      //System.out.println("Read CDR field (" + output.getRawTagHex() + "), Length: " + output.getLength());
      
      String tagIndex = cdrType + ";" + output.getRawTag();
      
      // Calculate the tag type for the lookup out of CDR type and cdr field tag
      if (output.isConstructed())
      {
        // just read over it for this test
        instance.readBlock(output.getLength());
      }
      
      // Output the information if we can
      if (asn1Specification.getTagType(tagIndex) >= 0)
      {
        //System.out.println("Mapped CDR field (" + tagIndex + "=" + asn1Specification.getTagName(tagIndex) + "), Length: " + output.getLength() + ", Value: " + asnpr.parseASN1(asn1Specification.getTagType(tagIndex), output.getOrigValue()));
        recordContents.append("{").append(asn1Specification.getTagName(tagIndex)).append("=").append(instance.parseASN1(asn1Specification.getTagType(tagIndex), output.getOrigValue())).append("};");
      }
      else
      {
        //System.out.println("-----> CDR field (" + tagIndex + ") [" +output.getTag() + "], Length: " + output.getLength() + ", Value: " + asnpr.parseBytes(output.getOrigValue()));
      }
    }

    String result = recordContents.toString();
    String expectedResult = "SMMT;{IMSI=260203114609127};{IMEI=531454402460490};{MSISDN=9132952387523};{Timestamp=1212312343482;0100};";
    
    // Check it
    Assert.assertEquals(result, expectedResult);
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
  //@Test
  public void testReadNextElementTag() throws Exception {
    System.out.println("readNextElementTag");
    
    // Defintion of the tags and so on
    HuaweiDef asn1Specification = new HuaweiDef();
    
    // Set up the parser instance
    ASN1Parser instance = new ASN1Parser(asn1Specification);
    
    // Test 1 byte tag 1
    byte[] testArray1 = {-95,-128};
    instance.setDataToParse(testArray1);
    Asn1Class output1 = instance.readNextElement();
    Assert.assertEquals(1, output1.getTag());
    
    // Test 1 byte tag 21
    byte[] testArray2 = {-107,-128};
    instance.setDataToParse(testArray2);
    Asn1Class output2 = instance.readNextElement();
    Assert.assertEquals(21, output2.getTag());
    
    // Test 2 byte tag 102
    byte[] testArray3 = {-97, 102, -128};
    instance.setDataToParse(testArray3);
    Asn1Class output3 = instance.readNextElement();
    Assert.assertEquals(102, output3.getTag());
    
    // Test 3 byte tag 130
    byte[] testArray4 = {-65, -127, 2};
    instance.setDataToParse(testArray4);
    Asn1Class output4 = instance.readNextElement();
    Assert.assertEquals(130, output4.getTag());
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
}