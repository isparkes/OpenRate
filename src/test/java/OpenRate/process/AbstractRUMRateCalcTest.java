/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.ChargePacket;
import OpenRate.record.IRecord;
import OpenRate.record.TimePacket;
import OpenRate.utils.ConversionUtils;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.*;
import static org.junit.Assert.assertEquals;

/**
 * This test runs in a cut down processing environment. Just enough framework is
 * made to allow the processing module to run, and to set up the test data.
 *
 * @author TGDSPIA1
 */
public class AbstractRUMRateCalcTest {

  private static URL FQConfigFileName;
  private static AbstractRUMRateCalc instance;

  // Used for logging and exception handling
  private static String message;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRUMRating.properties.xml");

    // Set up the OpenRate internal logger - this is normally done by app startup
    OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();

    // Get the transaction manager
    FrameworkUtils.startupTransactionManager();

    // Get Data Sources
    FrameworkUtils.startupDataSources();

    // Get a connection
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RUMRateTestCache");

    try {
      JDBCChcon.prepareStatement("DROP TABLE TEST_PRICE_MODEL").execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
              (ex.getMessage().startsWith("user lacks"))) // HSQL
      {
        // It's OK
      } else {
        // Not OK, fail the case
        message = "Error dropping table TEST_PRICE_MODEL in test <AbstractRUMRateCalcTest>.";
        Assert.fail(message);
      }
    }

    try {
      JDBCChcon.prepareStatement("DROP TABLE TEST_RUM_MAP").execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
              (ex.getMessage().startsWith("user lacks"))) // HSQL
      {
        // It's OK
      } else {
        // Not OK, fail the case
        message = "Error dropping table TEST_RUM_MAP in test <AbstractRUMRateCalcTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_PRICE_MODEL (ID int,PRICE_MODEL varchar(64) NOT NULL,STEP int DEFAULT 0 NOT NULL,TIER_FROM int,TIER_TO int,BEAT int,FACTOR double,CHARGE_BASE int,VALID_FROM DATE)").execute();

    // Simplest price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel1',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Two model RUM group - one with a setup price model and one with a scaled price model
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel2a',1,0,0,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel2b',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_RUM_MAP (ID int, PRICE_GROUP varchar(24), STEP int, PRICE_MODEL varchar(24), RUM varchar(24), RESOURCE varchar(24), RESOURCE_ID int, RUM_TYPE varchar(24), CONSUME_FLAG int)").execute();

    // Simplest price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel1',1,'TestModel1','DUR','EUR',978,'TIERED',0)").execute();

    // Two model RUM group - one with a setup price model and one with a scaled price model
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel2',1,'TestModel2a','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel2',1,'TestModel2b','DUR','EUR',978,'TIERED',0)").execute();

    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    // Deallocate the resources
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
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * simple linear price model.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredNonTimeBoundNonTiered() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 0.0;
    System.out.println("testPerformRatingTieredNonTimeBoundNonTiered");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", 0);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 1.0;
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 2.0;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    expResult = 16667.0;
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", 999999);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel1", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * simple linear price model.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredTwoPriceModelInGroup() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 0.0;
    System.out.println("testPerformRatingTieredTwoPriceModelInGroup");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", 0);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 2.0;
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", seconds);
      instance.performRating(ratingRecord);
      assertEquals(2, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 3.0;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", seconds);
      instance.performRating(ratingRecord);
      assertEquals(2, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    expResult = 16668.0;
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", 999999);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecord(CDRDate, "TestModel2", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  private double getRollUp(TestRatingRecord ratingRecord) {
    double actualResult = 0;
    for (ChargePacket resCP : ratingRecord.getChargePackets()) {
      actualResult += resCP.chargedValue;
    }
    return actualResult;
  }

  public class AbstractRUMRateCalcImpl extends AbstractRUMRateCalc {

    /**
     * Override the unused event handling routines.
     *
     * @param r input record
     * @return return record
     * @throws ProcessingException
     */
    @Override
    public IRecord procValidRecord(IRecord r) throws ProcessingException {
      return r;
    }

    /**
     * Override the unused event handling routines.
     *
     * @param r input record
     * @return return record
     * @throws ProcessingException
     */
    @Override
    public IRecord procErrorRecord(IRecord r) throws ProcessingException {
      return r;
    }
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
      instance = new AbstractRUMRateCalcTest.AbstractRUMRateCalcImpl();

      try {
        // Get the instance
        instance.init("DBTestPipe", "AbstractRUMRateCalcTest");
      } catch (InitializationException ex) {
        org.junit.Assert.fail();
      }

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

  private TestRatingRecord getNewRatingRecord(long CDRDate, String newPriceGroup, double durationValue) {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    ratingRecord.UTCEventDate = CDRDate;

    ChargePacket tmpCP = new ChargePacket();
    TimePacket tmpTZ = new TimePacket();
    tmpTZ.priceGroup = newPriceGroup;
    tmpCP.addTimeZone(tmpTZ);
    ratingRecord.addChargePacket(tmpCP);
    ratingRecord.setRUMValue("DUR", durationValue);

    return ratingRecord;
  }
}
