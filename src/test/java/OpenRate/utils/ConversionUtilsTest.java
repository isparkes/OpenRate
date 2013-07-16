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
package OpenRate.utils;

import OpenRate.exception.InitializationException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.resource.ConversionCache;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.*;

/**
 *
 * @author tgdspia1
 */
public class ConversionUtilsTest
{
  private static URL FQConfigFileName;

  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();

  // Used for logging and exception handling
  private static String message; 

  public ConversionUtilsTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    Class<?>          ResourceClass;
    IResource         Resource;

    FQConfigFileName = new URL("File:src/test/resources/TestUtils.properties.xml");
    
    // Get a properties object
    try
    {
      PropertyUtils.getPropertyUtils().loadPropertiesXML(FQConfigFileName,"FWProps");
    }
    catch (InitializationException ex)
    {
      message = "Error reading the configuration file <" + FQConfigFileName + ">";
      Assert.fail(message);
    }

    // Get a logger
    System.out.println("  Initialising Logger Resource...");
    resourceName         = "LogFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(AbstractLogFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);

    // Get a conversion cache
    System.out.println("  Initialising Conversion Cache Resource...");
    resourceName         = "ConversionCache";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(ConversionCache.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
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
   * Test of getConversionCache method, of class ConversionUtils.
   */
  @Test
  public void testGetConversionCache()
  {
    System.out.println("getConversionCache");
    ConversionCache result = ConversionUtils.getConversionCache();

    // Check that we get something back
    Assert.assertNotNull(result);
  }

  /**
   * Test of convertInputDateToUTC method, of class ConversionUtils.
   */
  @Test
  public void testConvertInputDateToUTC() throws Exception
  {
    long expResult;
    long result;
    String amorphicDate;

    System.out.println("convertInputDateToUTC");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // Test the standard date formats
    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    expResult = 1325372400;
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult,result);

    // another common one
    amorphicDate = "2012-01-01 00:00:00";
    instance.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    expResult = 1325372400;
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult,result);

    // an integer
    amorphicDate = "1325372400";
    instance.setInputDateFormat("integer");
    expResult = 1325372400;
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult,result);

    // a long
    amorphicDate = "1325372400000";
    instance.setInputDateFormat("long");
    expResult = 1325372400;
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult,result);
  }

  /**
   * Test of getDayOfWeek method, of class ConversionUtils.
   */
  @Test
  public void testGetDayOfWeek() throws Exception
  {
    long expResult;
    long result;
    long UTCDate;
    String amorphicDate;

    System.out.println("getDayOfWeek");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // Test the standard date formats
    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);
    expResult = 1325372400;
    Assert.assertEquals(expResult,expResult);

    result = instance.getDayOfWeek(UTCDate);

    // We expect 1 (Sunday)
    expResult = Calendar.SUNDAY;
    Assert.assertEquals(expResult, result);

    // now try rolling over
    amorphicDate = "20120106000000";
    UTCDate = instance.convertInputDateToUTC(amorphicDate);
    result = instance.getDayOfWeek(UTCDate);

    // We expect 6 (Friday)
    expResult = Calendar.FRIDAY;
    Assert.assertEquals(expResult, result);

    // now try rolling over
    UTCDate += 86400 * 2;
    result = instance.getDayOfWeek(UTCDate);

    // We expect 1 (Sunday)
    expResult = Calendar.SUNDAY;
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getMinuteOfDay method, of class ConversionUtils.
   */
  @Test
  public void testGetMinuteOfDay() throws ParseException
  {
    long expResult;
    long result;
    long UTCDate;
    String amorphicDate;

    System.out.println("getMinuteOfDay");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);
    expResult = 1325372400;
    Assert.assertEquals(expResult,expResult);

    // minute 0 of day
    result = instance.getMinuteOfDay(UTCDate);
    expResult = 0;
    Assert.assertEquals(expResult, result);

    // after 1 hour 1 min
    amorphicDate = "20120101010101";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);

    // minute 61 of day
    result = instance.getMinuteOfDay(UTCDate);
    expResult = 61;
    Assert.assertEquals(expResult, result);

    // after 23 hour 59 min
    amorphicDate = "20120101235959";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);

    // minute 1439 of day
    result = instance.getMinuteOfDay(UTCDate);
    expResult = 1439;
    Assert.assertEquals(expResult, result);

    // Rollover
    UTCDate += 1;

    // minute 0 of day
    result = instance.getMinuteOfDay(UTCDate);
    expResult = 0;
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of setInputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testSetInputDateFormat() throws ParseException
  {
    String amorphicDate;

    System.out.println("setInputDateFormat");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");

    try
    {
      instance.convertInputDateToUTC(amorphicDate);
    }
    catch (Exception ex)
    {
      Assert.fail("We expect no exception.");
    }
  }

  /**
   * Test of getInputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testGetInputDateFormat()
  {
    String expResult;
    String result;

    System.out.println("getInputDateFormat");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // default format
    expResult = "yyyy-MM-dd hh:mm:ss";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);

    // another
    instance.setInputDateFormat("yyyyMMddhhmmss");
    expResult = "yyyyMMddhhmmss";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    expResult = "yyyy-MM-dd hh:mm:ss";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setInputDateFormat("integer");
    expResult = "integer";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setInputDateFormat("long");
    expResult = "long";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of setOutputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testSetOutputDateFormat()
  {
    System.out.println("setOutputDateFormat");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    try
    {
      instance.setOutputDateFormat("yyyyMMddhhmmss");
    }
    catch (Exception ex)
    {
      Assert.fail("We expect no exception.");
    }
  }

  /**
   * Test of getOutputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testGetOutputDateFormat()
  {
    String expResult;
    String result;

    System.out.println("getOutputDateFormat");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // default format
    expResult = "yyyy-MM-dd hh:mm:ss";
    result = instance.getOutputDateFormat();
    Assert.assertEquals(expResult, result);

    // another
    instance.setOutputDateFormat("yyyyMMddhhmmss");
    expResult = "yyyyMMddhhmmss";
    result = instance.getOutputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setOutputDateFormat("yyyy-MM-dd hh:mm:ss");
    expResult = "yyyy-MM-dd hh:mm:ss";
    result = instance.getOutputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setOutputDateFormat("integer");
    expResult = "integer";
    result = instance.getOutputDateFormat();
    Assert.assertEquals(expResult, result);

    // and another
    instance.setOutputDateFormat("long");
    expResult = "long";
    result = instance.getOutputDateFormat();
    Assert.assertEquals(expResult, result);

    // make sure that we didn't change the input date format
    expResult = "yyyy-MM-dd hh:mm:ss";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of formatLongDate method, of class ConversionUtils.
   */
  @Test
  public void testFormatLongDate_long()
  {
    String result;
    long dateToFormat;
    String expResult;

    System.out.println("formatLongDate");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // simple case
    dateToFormat = 1325372400;
    instance.setOutputDateFormat("yyyyMMddHHmmss");
    result = instance.formatLongDate(dateToFormat);
    expResult = "20120101000000";
    Assert.assertEquals(expResult, result);

    //
    dateToFormat += 3621;
    result = instance.formatLongDate(dateToFormat);
    expResult = "20120101010021";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of formatLongDate method, of class ConversionUtils.
   */
  @Test
  public void testFormatLongDate_Date() throws ParseException
  {
    String result;
    Date   dateToFormat;
    String expResult;
    long   dateInput;

    System.out.println("formatLongDate");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // simple case
    dateInput = 1325372400;
    dateToFormat = instance.getDateFromUTC(dateInput);
    instance.setOutputDateFormat("yyyyMMddHHmmss");
    result = instance.formatLongDate(dateToFormat);
    expResult = "20120101000000";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDatefromLongFormat method, of class ConversionUtils.
   */
  @Test
  public void testGetDatefromLongFormat() throws Exception
  {
    String DateToFormat;
    Date expResult;

    System.out.println("getDatefromLongFormat");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // simple case
    expResult = instance.getDateFromUTC(1325372400);
    DateToFormat = "20120101000000";
    instance.setInputDateFormat("yyyyMMddHHmmss");
    Date result = instance.getDatefromLongFormat(DateToFormat);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getGmtDate method, of class ConversionUtils.
   */
  @Test
  public void testGetGmtDate() throws ParseException
  {
    int offSet;
    int dateInput;
    String DateToFormat;
    Date dateFormatted;
    Date expResult;
    Date result;

    System.out.println("getGmtDate");

    // No DST
    ConversionUtils instance = new ConversionUtils();
    DateToFormat = "20120101000000";
    instance.setInputDateFormat("yyyyMMddHHmmss");
    dateFormatted = instance.getDatefromLongFormat(DateToFormat);
    dateInput = (int) (dateFormatted.getTime() / 1000);
    TimeZone tz = TimeZone.getDefault();
    offSet = tz.getOffset( dateInput * 1000 );
    dateFormatted = instance.getDateFromUTC(dateInput);
    expResult = instance.getDateFromUTC(dateInput - (offSet/1000));
    result = instance.getGmtDate(dateFormatted);
    Assert.assertEquals(expResult, result);

    // DST
    DateToFormat = "20120601000000"; // winter - no DST
    instance.setInputDateFormat("yyyyMMddHHmmss");
    dateFormatted = instance.getDatefromLongFormat(DateToFormat);
    dateInput = (int) (dateFormatted.getTime() / 1000);
    offSet = tz.getOffset( dateInput * 1000 ) + tz.getDSTSavings();
    dateFormatted = instance.getDateFromUTC(dateInput);
    expResult = instance.getDateFromUTC(dateInput - (offSet/1000));
    result = instance.getGmtDate(dateFormatted);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDateInDST method, of class ConversionUtils.
   */
  @Test
  public void testGetDateInDST()
  {
    System.out.println("getDateInDST");

    Date date = new Date();
    TimeZone tz = TimeZone.getDefault();
    int offSet = tz.getOffset( new Date().getTime() );
    ConversionUtils instance = new ConversionUtils();
    int offSetDST = tz.getDSTSavings();
    boolean expResult = ( offSet != offSetDST );
    boolean result = instance.getDateInDST(date);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCMonthStart method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCMonthStart()
  {
    System.out.println("getUTCMonthStart");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372400;
    long expResult = dateInput;

    // Move the date 20+ days on
    dateInput += 2322134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCMonthStart(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCMonthEnd method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCMonthEnd()
  {
    System.out.println("getUTCMonthEnd");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372400;
    long expResult = dateInput - 1;

    // Move the date 20+ days back
    dateInput -= 2322134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCMonthEnd(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCDayStart method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDayStart()
  {
    System.out.println("getUTCDayStart");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372400;
    long expResult = dateInput;

    // Move the date 4+ hours on
    dateInput += 12134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCDayStart(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCDate method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDate()
  {
    System.out.println("getUTCDate");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372400;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long expResult = dateInput;
    long result = instance.getUTCDate(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDateFromUTC method, of class ConversionUtils.
   */
  @Test
  public void testGetDateFromUTC() throws ParseException
  {
    System.out.println("getDateFromUTC");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    long EventStartDate = 1325372400;
    DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dfm.setTimeZone(TimeZone.getTimeZone("Europe/Zurich"));
    Date expResult = dfm.parse("2012-01-01 00:00:00");
    Date result = instance.getDateFromUTC(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCDayEnd method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDayEnd_Date()
  {
    System.out.println("getUTCDayEnd");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372399;
    long expResult = dateInput;

    // Move the date 4+ hours back
    dateInput -= 12134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCDayEnd(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCDayEnd method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDayEnd_Date_int()
  {
    System.out.println("getUTCDayEnd");
    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    int dateInput = 1325372399;
    long expResult = dateInput;

    // Move the date 4+ hours back
    dateInput -= 12134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);

    // get the end of the day with offset 0, i.e. today
    long result = instance.getUTCDayEnd(EventStartDate, 0);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getRoundedValue method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValue()
  {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValue");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // already rounded - round it up
    valueToRound = 1.26;
    expResult = 1.26;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // simple case of round up
    valueToRound = 1.260001;
    expResult = 1.26;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // simple case of round up
    valueToRound = 1.265001;
    expResult = 1.27;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of getRoundedValueRoundUp method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValueRoundUp()
  {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundUp");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // already rounded - round it up
    valueToRound = 1.26;
    expResult = 1.27;
    result = instance.getRoundedValueRoundUp(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // simple case of round up
    valueToRound = 1.260001;
    expResult = 1.27;
    result = instance.getRoundedValueRoundUp(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round up on half
    valueToRound = 1.555;
    expResult = 1.56;
    result = instance.getRoundedValueRoundUp(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round up on half
    valueToRound = 1.5699999;
    expResult = 1.57;
    result = instance.getRoundedValueRoundUp(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of getRoundedValueRoundDown method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValueRoundDown()
  {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundDown");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // simple case of truncation
    valueToRound = 1.254;
    expResult = 1.25;
    result = instance.getRoundedValueRoundDown(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // simple case of round down
    valueToRound = 1.2551;
    expResult = 1.25;
    result = instance.getRoundedValueRoundDown(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round up on half
    valueToRound = 1.555;
    expResult = 1.55;
    result = instance.getRoundedValueRoundDown(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round down on half
    valueToRound = 1.565;
    expResult = 1.56;
    result = instance.getRoundedValueRoundDown(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of getRoundedValueRoundHalfEven method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValueRoundHalfEven()
  {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundHalfEven");

    // Get the utils instance
    ConversionUtils instance = new ConversionUtils();

    // simple case of truncation
    valueToRound = 1.254;
    expResult = 1.25;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // simple case of round up
    valueToRound = 1.2551;
    expResult = 1.26;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round up on half
    valueToRound = 1.555;
    expResult = 1.55;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round down on half
    valueToRound = 1.565;
    expResult = 1.56;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);
  }
}
