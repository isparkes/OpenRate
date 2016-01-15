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
import TestUtils.TestRatingRecord;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the RUM based rating functions. These build on the standard rate 
 * calculation module to give a RUM based rating. There are certain functions
 * available only in the RUM model, such as automatic multiple rating against
 * several price models and intelligent time handling.
 *
 * @author TGDSPIA1
 */
public class AbstractRUMRateCalcTest {

  private static URL FQConfigFileName;
  private static AbstractRUMRateCalc instance;

  // Used for logging and exception handling
  private static String message;
  private static OpenRate appl;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRUMRating.properties.xml");

    // Set up the OpenRate internal logger - this is normally done by app startup
    appl = OpenRate.getApplicationInstance();

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

    // ******************************* PRICE MODEL *****************************
    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_PRICE_MODEL (ID int,PRICE_MODEL varchar(64) NOT NULL,STEP int DEFAULT 0 NOT NULL,TIER_FROM int,TIER_TO int,BEAT int,FACTOR double,CHARGE_BASE int,VALID_FROM DATE)").execute();

    // Simplest linear price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel1',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Two model RUM group - one with a setup price model and one with a scaled price model
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel2a',1,0,0,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel2b',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Event price model - charges 1 per event
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel3',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Threshold model, charges 1 per minute for charges under 60 seconds, otherwise 0.1 per minute
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel4',1,0,60,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel4',2,60,999999,60,0.1,60,'2000-01-01')").execute();

    // Super nasty model. This causes a non-obvious charge packet expansion when rating over a time zone
    // change. There is a RUM expansion into 2 price models for the off-peak portion, but none in the
    // peak portion.
    // Tiered model, charges 1 per minute for charges under 60 seconds, otherwise 0.1 per minute PEAK
    // charges 0.5 per minute for charges under 60 seconds, otherwise 0.05 per minute OFF-PEAK
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel5a1',1,0,60,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel5a1',2,60,999999,60,0.1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel5b1',1,0,60,60,0.5,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel5b1',2,60,999999,60,0.05,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel5b2',1,0,0,60,1,60,'2000-01-01')").execute();

    // Tiered beat rounding model. Changes beat between first and second step
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel6a',1,0,999999,60,2,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel6b',1,0,999999,30,1,60,'2000-01-01')").execute();

    // Model with a setup step
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel7a1',1,0,0,1,10,1,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel7a1',2,0,999999,60,0.35,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel7b1',1,0,0,1,10,1,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel7b1',2,0,999999,60,0.16875,60,'2000-01-01')").execute();
    
    // ********************************** RUM MAP ******************************
    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_RUM_MAP (ID int, PRICE_GROUP varchar(24), STEP int, PRICE_MODEL varchar(24), RUM varchar(24), RESOURCE varchar(24), RESOURCE_ID int, RUM_TYPE varchar(24), CONSUME_FLAG int)").execute();

    // Simplest price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel1',1,'TestModel1','DUR','EUR',978,'TIERED',0)").execute();

    // Two model RUM group - one with a setup price model and one with a scaled price model
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel2',1,'TestModel2a','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel2',1,'TestModel2b','DUR','EUR',978,'TIERED',0)").execute();

    // Event price model
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel3',1,'TestModel3','EVT','EUR',978,'EVENT',0)").execute();

    // Threshold model
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel4',1,'TestModel4','DUR','EUR',978,'THRESHOLD',0)").execute();

    // Super nasty model
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel5a',1,'TestModel5a1','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel5b',1,'TestModel5b1','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel5b',1,'TestModel5b2','DUR','EUR',978,'TIERED',0)").execute();

    // Tiered beat rounding model. Changes beat between first and second step
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel6a',1,'TestModel6a','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel6b',1,'TestModel6b','DUR','EUR',978,'TIERED',0)").execute();

    // Model with a setup step
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel7a',1,'TestModel7a1','DUR','EUR',978,'TIERED',0)").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_RUM_MAP (ID,PRICE_GROUP,STEP,PRICE_MODEL,RUM,RESOURCE,RESOURCE_ID,RUM_TYPE,CONSUME_FLAG) VALUES (1,'TestModel7b',1,'TestModel7b1','DUR','EUR',978,'TIERED',0)").execute();

    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    OpenRate.getApplicationInstance().finaliseApplication();
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
   * simple linear price model. For each non-zero rated value we expect a beat
   * rounded per minute cost of 1.
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
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 0);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 1.0;
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 2nd beat - try all integer values
    expResult = 2.0;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // maximum value (according to price model)
    expResult = 16667.0;
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 999999);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * simple linear price model, but with a RUM expansion. For each non-zero
   * rated value we expect a setup cost of 1, plus a beat rounded per minute
   * cost of 1.
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
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 0);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 2.0;
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", seconds);
      instance.performRating(ratingRecord);
      assertEquals(2, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 2nd beat - try all integer values
    expResult = 3.0;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", seconds);
      instance.performRating(ratingRecord);
      assertEquals(2, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // maximum value (according to price model)
    expResult = 16668.0;
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 999999);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * simple linear price model. For each non-zero rated value we expect a beat
   * rounded per minute cost of 1.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingEvent() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 0.0;
    System.out.println("testPerformRatingEvent");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", 0);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(seconds, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    expResult = 2.0;
    ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(seconds, getRollUp(ratingRecord), 0.00001);
    }

    // maximum value (according to price model)
    expResult = 999999.0;
    ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", 999999);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecordEVT(CDRDate, "TestModel3", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * simple linear price model. For each non-zero rated value we expect a beat
   * rounded per minute cost of 1.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingThresholdNonTimeBoundNonTiered() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 0.0;
    System.out.println("testPerformRatingThresholdNonTimeBoundNonTiered");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", 0);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    expResult = 1.0;
    for (int seconds = 1; seconds < 60; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 2nd beat - try all integer values
    expResult = 0.2;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", seconds);
      instance.performRating(ratingRecord);
      assertEquals(1, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // maximum value (according to price model)
    expResult = 1666.7;
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", 999999);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    expResult = 0.0;
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel4", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Uses a
   * more complex linear price model. There is a RUM expansion in the second
   * part of the model, but not in the first.
   *
   * The rating model here is:
   *
   * Time Packet 1 1 per minute in the first minute 0.1 per minute for other
   * minutes Time Packet 2 1 set up 0.5 per minute in the first minute 0.05 per
   * minute for other minutes
   *
   * Example: A 70 second call will be: Time packet 1 = 1 (first minute) + 0.1
   * (second minute) Time packet 2 = 1 (set up) + 0.5 (first minute) + 0.05
   * (second minute)
   *
   * --> 2.65
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredAsymmetricRUMExpansion() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 0.0;
    System.out.println("testPerformRatingTieredAsymmetricRUMExpansion");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 0);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      expResult = 2.5;
      ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", seconds);
      instance.performRating(ratingRecord);
      assertEquals(3, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // intra-beat 2, non integer value
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 2.654);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // intra-beat 2nd beat - try all integer values
    expResult = 2.65;
    for (int seconds = 61; seconds < 120; seconds++) {
      ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", seconds);
      instance.performRating(ratingRecord);
      assertEquals(3, ratingRecord.getChargePacketCount());
      assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
    }

    // maximum value (according to price model)
    expResult = 2502.4;
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 999999);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 1000000);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    // run off the end of the rating some more
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 1500000);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test the performance of the main performRating method. Uses a complex price
   * model with a RUM expansion. We expect way more than 10,000 per second.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredAsymmetricRUMExpansionPerfomance() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult;
    System.out.println("testPerformRatingTieredAsymmetricRUMExpansionPerfomance");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // Check that we get the right answer
    expResult = 2.65;
    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 78.4);
    instance.performRating(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);

    long startMs = Calendar.getInstance().getTimeInMillis();

    for (int i = 1; i < 10000; i++) {
      ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 78.4);
      instance.performRating(ratingRecord);
    }

    long duration = Calendar.getInstance().getTimeInMillis() - startMs;

    System.out.println("10000 took " + duration + "mS");
    assertTrue(duration < 1000);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Test
   * the beat rounding time splitting algorithm. This should apportion as much
   * of the RUM necessary to each packet to respect the beat rounding of that
   * model.
   *
   * For example, if a 62 second call has 1 second in off-peak, but a 60 second
   * beat, then 60 seconds should be charged in off peak, and the remaining 2
   * seconds in peak.
   *
   * Without this splitting algorithm, we would charge 60 seconds in off-peak
   * (from the 1 second in off-peak), then 120 seconds in peak (61 seconds
   * rounded up).
   *
   * With the example we have we expect the result to be:
   *
   * 1 minute at 2 per minute = 2, .5 minutes at 1 per minute = 0.5
   * 
   * --> 1 second in peak pulls in a whole beat of 60 seconds into peak = 2
   * --> remaining duration in off-peak = 62 - 60 = 2
   * --> 2 seconds in off-peak rounded up to 30 seconds because of the model = 0.5
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredBeatRounding() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 2.5;
    System.out.println("testPerformRatingTieredBeatRounding");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDURTimeSplitBeatRounding(CDRDate, "TestModel6a", "TestModel6b", 1, 61);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test of the main performRating method, of class AbstractRUMRateCalc. Test
   * the time splitting algorithm. This should blindly apportion as much
   * of the RUM necessary to each packet, ignoring the beat rounding of that
   * model.
   *
   * For example, if a 62 second call has 1 second in off-peak, but a 60 second
   * beat, then 60 seconds should be charged in off peak, and the remaining 2
   * seconds in peak.
   *
   * Without this splitting algorithm, we would charge 60 seconds in off-peak
   * (from the 1 second in off-peak), then 120 seconds in peak (61 seconds
   * rounded up).
   *
   * With the example we have we expect the result to be:
   *
   * 1 minute at 2 per minute = 2, 1.5 minutes at 1 per minute = 1.5
   * 
   * --> 1 second in peak gets rated as 60 seconds = 2
   * --> remaining duration in off-peak = 62 - 1 = 61
   * --> 61 seconds in off-peak rounded up to 90 seconds because of the model = 1.5
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredNoBeatRounding() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 3.5;
    System.out.println("testPerformRatingTieredNoBeatRounding");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDURTimeSplitNoBeatRounding(CDRDate, "TestModel6a", "TestModel6b", 1, 61);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Test the time splitting algorithm. This should blindly apportion as much
   * of the RUM necessary to each packet, ignoring the beat rounding of that
   * model.
   * 
   * In this case, we are testing that the setup step is not triggered in the
   * second packet (it should have been triggered in the first packet)
   * 
   * We expect:
   * 
   * --> a setup step = 10
   * --> 1 second rated in peak at 60 second beat = 0.35
   * --> remaining duration in off-peak = 62 - 1 = 61
   * --> 61 seconds in off-peak rounded up to 120 seconds because of the model = 0.16875*2 = 0.3375
   * 
   * --> 10.6875
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRatingTieredNoBeatRoundingSetup() throws Exception {
    TestRatingRecord ratingRecord;
    double expResult = 10.6875;
    System.out.println("testPerformRatingTieredNoBeatRoundingSetup");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDURTimeSplitNoBeatRounding(CDRDate, "TestModel7a", "TestModel7b", 1, 61);
    instance.performRating(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(expResult, getRollUp(ratingRecord), 0.00001);
  }

  /**
   * Roll up the charged values from each of the charge packets.
   *
   * @param ratingRecord The record to check
   * @return The rolled up rated amount
   */
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

  /**
   * Create a rating record initialised with the information necessary for
   * performing a rating.
   *
   * @param CDRDate Date of the CDR
   * @param newPriceGroup The price group to use
   * @param durationValue The duration value to use
   * @return The record, ready to go
   */
  private TestRatingRecord getNewRatingRecordDUR(long CDRDate, String newPriceGroup, double durationValue) {
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

  /**
   * Create a rating record initialised with the information necessary for
   * performing a rating.
   *
   * @param CDRDate Date of the CDR
   * @param newPriceGroup The price group to use
   * @param durationValue The duration value to use
   * @return The record, ready to go
   */
  private TestRatingRecord getNewRatingRecordEVT(long CDRDate, String newPriceGroup, double durationValue) {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    ratingRecord.UTCEventDate = CDRDate;

    ChargePacket tmpCP = new ChargePacket();
    TimePacket tmpTZ = new TimePacket();
    tmpTZ.priceGroup = newPriceGroup;
    tmpCP.addTimeZone(tmpTZ);
    ratingRecord.addChargePacket(tmpCP);
    ratingRecord.setRUMValue("EVT", durationValue);

    return ratingRecord;
  }

  /**
   * Create a rating record initialised with the information necessary for
   * performing a rating. This simulates a record that has undergone time
   * splitting.
   *
   * @param CDRDate Date of the CDR
   * @param newPriceGroup1 The price group to use
   * @param durationValue The duration value to use
   * @return The record, ready to go
   */
  private TestRatingRecord getNewRatingRecordDURTimeSplit(long CDRDate, String newPriceGroup1, String newPriceGroup2, double durationValue) {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    ratingRecord.UTCEventDate = CDRDate;

    ChargePacket tmpCP = new ChargePacket();
    TimePacket tmpTZ1 = new TimePacket();
    tmpTZ1.priceGroup = newPriceGroup1;
    tmpCP.addTimeZone(tmpTZ1);
    TimePacket tmpTZ2 = new TimePacket();
    tmpTZ2.priceGroup = newPriceGroup2;
    tmpCP.addTimeZone(tmpTZ2);
    ratingRecord.addChargePacket(tmpCP);
    ratingRecord.setRUMValue("DUR", durationValue);

    return ratingRecord;
  }

  /**
   * Create a rating record initialised with the information necessary for
   * performing a rating. This simulates a record that has undergone time
   * splitting.
   *
   * @param CDRDate Date of the CDR
   * @param newPriceGroup1 The price group to use
   * @param durationValue The duration value to use
   * @return The record, ready to go
   */
  private TestRatingRecord getNewRatingRecordDURTimeSplitBeatRounding(long CDRDate, String newPriceGroup1, String newPriceGroup2, int durationValue1, int durationValue2) {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    ratingRecord.UTCEventDate = CDRDate;

    ChargePacket tmpCP = new ChargePacket();
    tmpCP.timeSplitting = AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING;
    TimePacket tmpTZ1 = new TimePacket();
    tmpTZ1.priceGroup = newPriceGroup1;
    tmpTZ1.duration = durationValue1;
    tmpTZ1.totalDuration = durationValue1 + durationValue2;
    tmpCP.addTimeZone(tmpTZ1);
    TimePacket tmpTZ2 = new TimePacket();
    tmpTZ2.priceGroup = newPriceGroup2;
    tmpTZ2.duration = durationValue2;
    tmpTZ2.totalDuration = durationValue1 + durationValue2;
    tmpCP.addTimeZone(tmpTZ2);
    ratingRecord.addChargePacket(tmpCP);
    ratingRecord.setRUMValue("DUR", durationValue1 + durationValue2);

    return ratingRecord;
  }
  
  /**
   * Create a rating record initialised with the information necessary for
   * performing a rating. This simulates a record that has undergone time
   * splitting.
   *
   * @param CDRDate Date of the CDR
   * @param newPriceGroup1 The price group to use
   * @param durationValue The duration value to use
   * @return The record, ready to go
   */
  private TestRatingRecord getNewRatingRecordDURTimeSplitNoBeatRounding(long CDRDate, String newPriceGroup1, String newPriceGroup2, int durationValue1, int durationValue2) {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    ratingRecord.UTCEventDate = CDRDate;

    ChargePacket tmpCP = new ChargePacket();
    tmpCP.timeSplitting = AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING;
    TimePacket tmpTZ1 = new TimePacket();
    tmpTZ1.priceGroup = newPriceGroup1;
    tmpTZ1.duration = durationValue1;
    tmpTZ1.totalDuration = durationValue1 + durationValue2;
    tmpCP.addTimeZone(tmpTZ1);
    TimePacket tmpTZ2 = new TimePacket();
    tmpTZ2.priceGroup = newPriceGroup2;
    tmpTZ2.duration = durationValue2;
    tmpTZ2.totalDuration = durationValue1 + durationValue2;
    tmpCP.addTimeZone(tmpTZ2);
    ratingRecord.addChargePacket(tmpCP);
    ratingRecord.setRUMValue("DUR", durationValue1 + durationValue2);

    return ratingRecord;
  }
}
