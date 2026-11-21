/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package OpenRate.utils;

import OpenRate.OpenRate;
import OpenRate.resource.ConversionCache;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.*;

/**
 *
 * @author tgdspia1
 */
public class ConversionUtilsTest {

  private static URL FQConfigFileName;
  private static ConversionUtils instance;
  private static TimeZone tz;
  private static Calendar cal;

  public ConversionUtilsTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Set up time zone
    tz = TimeZone.getDefault();
    cal = Calendar.getInstance();
    cal.setTimeZone(tz);

    // Get an initialise the cache
    FQConfigFileName = new URL("File:src/test/resources/TestUtils.properties.xml");

    // Set up the OpenRate internal logger - this is normally done by app startup
    OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();

    // Get the conversion cache resource
    FrameworkUtils.startupConversionCache();
  }

  @AfterClass
  public static void tearDownClass() {
    // Deallocate
    if (tz != null) {
      TimeZone.setDefault(tz);
    }
    OpenRate.getApplicationInstance().cleanup();
  }

  @Before
  public void setUp() {
    getInstance();
  }

  @After
  public void tearDown() {
    releaseInstance();
  }

  /**
   * Test of getConversionCache method, of class ConversionUtils.
   */
  @Test
  public void testGetConversionCache() {
    System.out.println("getConversionCache");
    ConversionCache result = ConversionUtils.getConversionCache();

    // Check that we get something back
    Assert.assertNotNull(result);
  }

  /**
   * Test of getConversionCache method, of class ConversionUtils.
   */
  @Test
  public void testGetConversionCacheSymbolicName() {
    System.out.println("getConversionCache");
    String result = ConversionUtils.getConversionCache().getSymbolicName();

    // Check that we get something back
    Assert.assertEquals("ConversionCache", result);
  }

  /**
   * Test of convertInputDateToUTC method, of class ConversionUtils.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testConvertInputDateToUTC() throws ParseException {
    long expResult;
    long result;
    String amorphicDate;

    System.out.println("convertInputDateToUTC");

    // Test the standard date formatsTimeZone
    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    expResult = getUTCDateExpected(2012,1,1,0,0,0);
    
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult, result);

    // another common one
    amorphicDate = "2012-01-01 00:00:00";
    instance.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult, result);

    // an integer
    amorphicDate = Long.toString(expResult);
    instance.setInputDateFormat("integer");
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult, result);

    // a long
    expResult = 1325372400;
    amorphicDate = Long.toString(expResult) + "000";
    instance.setInputDateFormat("long");
    result = instance.convertInputDateToUTC(amorphicDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDayOfWeek method, of class ConversionUtils.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testGetDayOfWeek() throws ParseException {
    long expResult;
    long result;
    long UTCDate;
    String amorphicDate;

    System.out.println("getDayOfWeek");

    // Test the standard date formats
    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);
    expResult = getUTCDateExpected(2012,1,1,0,0,0);
    Assert.assertEquals(expResult, expResult);

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
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testGetMinuteOfDay() throws ParseException {
    long expResult;
    long result;
    long UTCDate;
    String amorphicDate;

    System.out.println("getMinuteOfDay");

    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");
    UTCDate = instance.convertInputDateToUTC(amorphicDate);
    expResult = getUTCDateExpected(2012,1,1,0,0,0);
    Assert.assertEquals(expResult, expResult);

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
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testSetInputDateFormat() throws ParseException {
    String amorphicDate;

    System.out.println("setInputDateFormat");

    amorphicDate = "20120101000000";
    instance.setInputDateFormat("yyyyMMddhhmmss");

    try {
      instance.convertInputDateToUTC(amorphicDate);
    } catch (Exception ex) {
      Assert.fail("We expect no exception.");
    }
  }

  /**
   * Test of getInputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testGetInputDateFormat() {
    String expResult;
    String result;

    System.out.println("getInputDateFormat");

    // default format
    expResult = "yyyy-MM-dd HH:mm:ss";
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
  public void testSetOutputDateFormat() {
    System.out.println("setOutputDateFormat");

    try {
      instance.setOutputDateFormat("yyyyMMddhhmmss");
    } catch (Exception ex) {
      Assert.fail("We expect no exception.");
    }
  }

  /**
   * Test of getOutputDateFormat method, of class ConversionUtils.
   */
  @Test
  public void testGetOutputDateFormat() {
    String expResult;
    String result;

    System.out.println("getOutputDateFormat");

    // default format
    expResult = "yyyy-MM-dd HH:mm:ss";
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
    expResult = "yyyy-MM-dd HH:mm:ss";
    result = instance.getInputDateFormat();
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of formatLongDate method, of class ConversionUtils.
   */
  @Test
  public void testFormatLongDate_long() {
    String result;
    long dateToFormat;
    String expResult;

    System.out.println("formatLongDate");

    // simple case
    dateToFormat = getUTCDateExpected(2012,1,1,0,0,0);
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
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testFormatLongDate_Date() throws ParseException {
    String result;
    Date dateToFormat;
    String expResult;
    long dateInput;

    System.out.println("formatLongDate");

    // simple case
    dateInput = this.getUTCDateExpected(2012,1,1,0,0,0);
    dateToFormat = instance.getDateFromUTC(dateInput);
    instance.setOutputDateFormat("yyyyMMddHHmmss");
    result = instance.formatLongDate(dateToFormat);
    expResult = "20120101000000";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDatefromLongFormat method, of class ConversionUtils.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testGetDatefromLongFormat() throws ParseException {
    String DateToFormat;
    Date expResult;

    System.out.println("getDatefromLongFormat");

    // simple case
    expResult = getDateExpected(2012,1,1,0,0,0);
    DateToFormat = "20120101000000";
    instance.setInputDateFormat("yyyyMMddHHmmss");
    Date result = instance.getDatefromLongFormat(DateToFormat);
    Assert.assertTrue(expResult.compareTo(result) == 0);
  }

  /**
   * Test of getGmtDate method, of class ConversionUtils.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testGetGmtDate() throws ParseException {
    int offSet;
    int dateInput;
    String DateToFormat;
    Date dateFormatted;
    Date expResult;
    Date result;

    System.out.println("getGmtDate");

    // No DST
    DateToFormat = "20120101000000";
    instance.setInputDateFormat("yyyyMMddHHmmss");
    dateFormatted = instance.getDatefromLongFormat(DateToFormat);
    dateInput = (int) (dateFormatted.getTime() / 1000);
    offSet = tz.getOffset(dateInput * 1000);
    dateFormatted = instance.getDateFromUTC(dateInput);
    expResult = instance.getDateFromUTC(dateInput - (offSet / 1000));
    result = instance.getGmtDate(dateFormatted);
    Assert.assertEquals(expResult, result);

    // DST
    DateToFormat = "20120601000000"; // winter - no DST
    instance.setInputDateFormat("yyyyMMddHHmmss");
    dateFormatted = instance.getDatefromLongFormat(DateToFormat);
    dateInput = (int) (dateFormatted.getTime() / 1000);
    offSet = tz.getOffset(dateInput * 1000) + tz.getDSTSavings();
    dateFormatted = instance.getDateFromUTC(dateInput);
    expResult = instance.getDateFromUTC(dateInput - (offSet / 1000));
    result = instance.getGmtDate(dateFormatted);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDateInDST method, of class ConversionUtils. Not all time zones
   * have DST. We take a day a month and check it.
   */
  @Test
  public void testGetDateInDST() {
    System.out.println("getDateInDST");

    for (int month = 1 ; month < 12 ; month++) {
      Date expDate = getDateExpected(2012, month, 1, 0, 0, 0);
      boolean expResult = tz.inDaylightTime(expDate);
      boolean result = instance.getDateInDST(expDate);
      Assert.assertEquals(expResult, result);
    }
  }

  /**
   * Test of getUTCMonthStart method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCMonthStart() {
    System.out.println("getUTCMonthStart");

    long dateInput = getUTCDateExpected(2012,1,1,0,0,0);
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
  public void testGetUTCMonthEnd() {
    System.out.println("getUTCMonthEnd");

    long dateInput = getUTCDateExpected(2012,1,31,23,59,59);
    long expResult = dateInput;

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
  public void testGetUTCDayStart() {
    System.out.println("getUTCDayStart");

    long dateInput = getUTCDateExpected(2012,1,1,0,0,0);
    long expResult = dateInput;

    // Move the date 4+ hours on, but when we get the result, it should be
    // rounded back down to the start date
    dateInput += 12134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCDayStart(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of the offset form of getUTCDayStart, of class ConversionUtils.
   *
   * The offset form used to delegate to getDayEnd, so it returned the last
   * second of the offset day instead of the first. Check the whole family
   * against each other: day start plus a day, minus a second, is day end.
   */
  @Test
  public void testGetUTCDayStartWithOffset() {
    System.out.println("getUTCDayStartWithOffset");

    // Three and a bit hours into the first of January 2012
    Date eventDate = instance.getDateFromUTC(getUTCDateExpected(2012,1,1,0,0,0) + 12134);

    // No offset must agree with the no-argument form
    Assert.assertEquals(instance.getUTCDayStart(eventDate),
                        instance.getUTCDayStart(eventDate, 0));

    // The start of the day itself, and of the days either side of it
    Assert.assertEquals(getUTCDateExpected(2012,1,1,0,0,0),  instance.getUTCDayStart(eventDate, 0));
    Assert.assertEquals(getUTCDateExpected(2012,1,2,0,0,0),  instance.getUTCDayStart(eventDate, 1));
    Assert.assertEquals(getUTCDateExpected(2011,12,31,0,0,0), instance.getUTCDayStart(eventDate, -1));

    // A day start is never a day end: they are a whole day apart, less a second
    Assert.assertEquals(instance.getUTCDayEnd(eventDate, 0),
                        instance.getUTCDayStart(eventDate, 1) - 1);

    // Crossing a month boundary in a leap year, where an off by one day shows up
    Date lastOfFebruary = instance.getDateFromUTC(getUTCDateExpected(2012,2,29,13,0,0));
    Assert.assertEquals(getUTCDateExpected(2012,3,1,0,0,0), instance.getUTCDayStart(lastOfFebruary, 1));
  }

  /**
   * Test of getUTCDate method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDate() {
    System.out.println("getUTCDate");

    int dateInput = 1325372400;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long expResult = dateInput;
    long result = instance.getUTCDate(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDateFromUTC method, of class ConversionUtils.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testGetDateFromUTC() throws ParseException {
    System.out.println("getDateFromUTC");

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
  public void testGetUTCDayEnd_Date() {
    System.out.println("getUTCDayEnd");

    long dateInput = getUTCDateExpected(2012,1,1,23,59,59);
    long expResult = dateInput;

    // Move the date 4+ hours back, but when we get the result, it should have
    // been rounded up again
    dateInput -= 12134;
    Date EventStartDate = instance.getDateFromUTC(dateInput);
    long result = instance.getUTCDayEnd(EventStartDate);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getUTCDayEnd method, of class ConversionUtils.
   */
  @Test
  public void testGetUTCDayEnd_Date_int() {
    System.out.println("getUTCDayEnd");

    long dateInput = getUTCDateExpected(2012,1,1,23,59,59);
    long expResult = dateInput;

    // Move the date 4+ hours back, but when we get the result, it should have
    // been rounded up again
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
  public void testGetRoundedValue() {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValue");

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

    // The old implementation rounded at one extra decimal place first and
    // then rounded that result, so 1.2649 became 1.265 became 1.27. Rounding
    // once, at the requested place, gives 1.26.
    valueToRound = 1.2649;
    expResult = 1.26;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // The old implementation lost the cent on anything above ~167772, and
    // clamped everything above 21474836.47, because Math.round() applied to a
    // long resolves to the float overload and returns an int.
    valueToRound = 1000000.01;
    expResult = 1000000.01;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    valueToRound = 50000000.00;
    expResult = 50000000.00;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // Negative values round away from zero, the same distance as the
    // equivalent positive value. The old implementation rounded towards
    // positive infinity, so debits and credits rounded differently.
    valueToRound = -1.265;
    expResult = -1.27;
    result = instance.getRoundedValue(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // The old implementation computed 10^places into an int, which overflowed
    // for 10 or more places and returned 1.0 here.
    valueToRound = 1.5;
    expResult = 1.5;
    result = instance.getRoundedValue(valueToRound, 10);
    Assert.assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of getRoundedValueRoundUp method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValueRoundUp() {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundUp");

    // Already exact at 2 decimal places, so there is nothing to round up.
    // The old implementation returned 1.27 here, because new BigDecimal(1.26)
    // is 1.26000000000000000888 and ROUND_UP moved it. Whether that happened
    // depended on the binary representation: 2.67 is 2.66999999999999992894
    // and stayed put. See the stability check at the end of this test.
    valueToRound = 1.26;
    expResult = 1.26;
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

    // Values that are already exact at 2 decimal places must all be left
    // alone, whichever side of the decimal their binary representation falls.
    // The old implementation moved the first, third, fifth and sixth of these
    // and left the others.
    for (double exact : new double[]{1.26, 2.67, 0.10, 0.29, 3.45, 1.11, 0.07})
    {
      Assert.assertEquals(exact, instance.getRoundedValueRoundUp(exact, decimalPlaces), 0.0);
    }
  }

  /**
   * Test of getRoundedValueRoundDown method, of class ConversionUtils.
   */
  @Test
  public void testGetRoundedValueRoundDown() {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundDown");

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
  public void testGetRoundedValueRoundHalfEven() {
    double valueToRound;
    int decimalPlaces = 2;
    double expResult;
    double result;

    System.out.println("getRoundedValueRoundHalfEven");

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

    // A true tie: half-even breaks it towards the even neighbour, so 1.56.
    // The old implementation returned 1.55, because new BigDecimal(1.555) is
    // 1.55499999999999993783 and so was not a tie at all. That defeats the
    // point of half-even rounding, which is to remove the bias on ties.
    valueToRound = 1.555;
    expResult = 1.56;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);

    // round down on half
    valueToRound = 1.565;
    expResult = 1.56;
    result = instance.getRoundedValueRoundHalfEven(valueToRound, decimalPlaces);
    Assert.assertEquals(expResult, result, 0.0);
  }

  /**
   * Get the UTC date using the calendar. We expect 
   */
  private long getUTCDateExpected(int year, int month, int day, int hour, int min, int sec) {
    cal.set(year, month-1, day, hour, min, sec);
    return cal.getTimeInMillis()/1000;
  }
  
  /**
   * A ConversionUtils object is shared: the ConversionCache hands the same
   * one to everything asking for a given type, and getConversionUtilsObject()
   * is a process wide singleton that the pipeline worker threads use. The
   * date helpers must therefore be safe to call concurrently.
   *
   * Before the calendar was made per thread, this failed within a few hundred
   * iterations, because one thread's setTime() landed between another
   * thread's setTime() and getTime(). Nothing was thrown: the wrong month
   * simply came back.
   */
  @Test
  public void testConcurrentDateHelpers() throws Exception {
    System.out.println("concurrent date helpers");

    final int threadCount = 8;
    final int iterations = 5000;
    final ConversionUtils shared = instance;
    final CountDownLatch start = new CountDownLatch(1);
    final List<String> failures = Collections.synchronizedList(new ArrayList<String>());

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);

    for (int thread = 0; thread < threadCount; thread++) {
      final int month = thread + 1;

      pool.submit(() -> {
        try {
          Date input = dateFor(2015, month, 15, 12, 30, 0);
          Date expectedStart = dateFor(2015, month, 1, 0, 0, 0);

          start.await();

          for (int i = 0; i < iterations; i++) {
            Date result = shared.getMonthStart(input);

            if (result.equals(expectedStart) == false) {
              failures.add("getMonthStart for month " + month + " returned <"
                      + result + "> instead of <" + expectedStart + ">");
              return;
            }
          }
        } catch (Throwable ex) {
          failures.add("month " + month + " threw " + ex);
        }
      });
    }

    start.countDown();
    pool.shutdown();

    Assert.assertTrue("worker threads did not finish", pool.awaitTermination(60, TimeUnit.SECONDS));
    Assert.assertEquals("concurrent date helpers disagreed", Collections.emptyList(), failures);
  }

  /**
   * The same again for the input formatter. SimpleDateFormat is not thread
   * safe, and used concurrently it either throws from inside the number
   * parser or returns a date built from another thread's input.
   */
  @Test
  public void testConcurrentDateParsing() throws Exception {
    System.out.println("concurrent date parsing");

    final int threadCount = 8;
    final int iterations = 5000;
    final ConversionUtils shared = instance;
    final CountDownLatch start = new CountDownLatch(1);
    final List<String> failures = Collections.synchronizedList(new ArrayList<String>());

    shared.setInputDateFormat("yyyy-MM-dd HH:mm:ss");

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);

    for (int thread = 0; thread < threadCount; thread++) {
      final int month = thread + 1;

      pool.submit(() -> {
        try {
          String input = String.format("2015-%02d-15 12:30:00", month);
          long expected = dateFor(2015, month, 15, 12, 30, 0).getTime() / 1000;

          start.await();

          for (int i = 0; i < iterations; i++) {
            long result = shared.convertInputDateToUTC(input);

            if (result != expected) {
              failures.add("convertInputDateToUTC(" + input + ") returned <"
                      + result + "> instead of <" + expected + ">");
              return;
            }
          }
        } catch (Throwable ex) {
          failures.add("month " + month + " threw " + ex);
        }
      });
    }

    start.countDown();
    pool.shutdown();

    Assert.assertTrue("worker threads did not finish", pool.awaitTermination(60, TimeUnit.SECONDS));
    Assert.assertEquals("concurrent parsing disagreed", Collections.emptyList(), failures);
  }

  /**
   * Build a date without touching the calendar shared by the other tests in
   * this class, so that it can be called from several threads at once.
   */
  private static Date dateFor(int year, int month, int day, int hour, int min, int sec) {
    Calendar workingCalendar = Calendar.getInstance();
    workingCalendar.clear();
    workingCalendar.set(year, month - 1, day, hour, min, sec);
    workingCalendar.set(Calendar.MILLISECOND, 0);

    return workingCalendar.getTime();
  }

  /**
   * Get the UTC date using the calendar. We expect 
   */
  private Date getDateExpected(int year, int month, int day, int hour, int min, int sec) {
    cal.set(year, month-1, day, hour, min, sec);
    cal.set(Calendar.MILLISECOND,0);
    return cal.getTime();
  }
  
  /**
   * Method to get an instance of the implementation. Done this way to allow
   * tests to be executed individually.
   *
   * @throws InitializationException
   */
  private void getInstance() {
    if (instance == null) {
      // Get an initialise the cache
      instance = new ConversionUtils();
    } else {
      org.junit.Assert.fail("Instance already allocated");
    }
  }

  /**
   * Method to release an instance of the implementation.
   */
  private void releaseInstance() {
    instance = null;
  }
}
