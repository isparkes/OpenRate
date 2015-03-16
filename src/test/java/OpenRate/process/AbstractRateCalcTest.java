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
import OpenRate.record.IRecord;
import OpenRate.utils.ConversionUtils;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import static org.junit.Assert.assertEquals;
import org.junit.*;

/**
 * This test runs in a cut down processing environment. Just enough framework is
 * made to allow the processing module to run, and to set up the test data.
 *
 * @author TGDSPIA1
 */
public class AbstractRateCalcTest {

  private static URL FQConfigFileName;
  private static AbstractRateCalc instance;

  // Used for logging and exception handling
  private static String message;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRating.properties.xml");

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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RateTestCache");

    // Set up test data
    try {
      JDBCChcon.prepareStatement("DROP TABLE TEST_PRICE_MODEL;").execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
              (ex.getMessage().startsWith("user lacks"))) // HSQL
      {
        // It's OK
      } else {
        // Not OK, fail the case
        message = "Error dropping table TEST_PRICE_MODEL in test <AbstractRateCalcTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_PRICE_MODEL (ID int,PRICE_MODEL varchar(64) NOT NULL,STEP int DEFAULT 0 NOT NULL,TIER_FROM int,TIER_TO int,BEAT int,FACTOR double,CHARGE_BASE int,VALID_FROM DATE)").execute();

    // Simplest price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (1,'TestModel1',1,0,999999,60,1,60,'2000-01-01')").execute();

    // Simplest tiered price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating" for the first min, therefore 0.1 per min
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (2,'TestModel2',1,0,60,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (3,'TestModel2',2,60,999999,60,0.1,60,'2000-01-01')").execute();

    // Time Bound tiered price model - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating" up until Jan 1 2013, thereafter 2 (doubled)
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (4,'TestModel3',1,0,999999,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (5,'TestModel3',1,0,999999,60,2,60,'2013-01-01')").execute();

    // "Non-linear" price model - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 60 (BEAT) = "per minute rating" for the first min, thereafter 30 second rating
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (6,'TestModel4',1,0,60,60,1,60,'2000-01-01')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM) values (7,'TestModel4',2,60,999999,30,1,60,'2000-01-01')").execute();

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
   * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
   * uses a simple non-tiered model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateTieredNonTimeBoundNonTiered() throws Exception {
    System.out.println("rateCalculateTieredNonTimeBoundNonTiered");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel1";
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double expResult = 0.0;
    double result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 2.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating
    valueToRate = 1000000;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more
    valueToRate = 1500000;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
   * uses a simple tiered model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateTieredNonTimeBoundTiered() throws Exception {
    System.out.println("rateCalculateTieredNonTimeBoundTiered");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel2";
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double expResult = 0.0;
    double result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 1.1;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 1667.60;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating
    valueToRate = 1000000;
    expResult = 1667.60;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more
    valueToRate = 1500000;
    expResult = 1667.60;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
   * uses a simple tiered model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateTieredTimeBoundTiered() throws Exception {
    System.out.println("testRateCalculateTieredTimeBoundTiered");

    // Simple test using time bound non-tiered model
    String priceModel = "TestModel3";
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // ******************* Before Price change ******************* 
    // zero value to rate
    double expResult = 0.0;
    double result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 2;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating
    valueToRate = 1000000;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more
    valueToRate = 1500000;
    expResult = 16667.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
    
    // ******************* Aftrer Price change ******************* 
    CDRDate = conv.convertInputDateToUTC("2014-01-23 00:00:00");
    
    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 2.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 4;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 33334.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating
    valueToRate = 1000000;
    expResult = 33334.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more
    valueToRate = 1500000;
    expResult = 33334.0;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
   * uses a simple non-tiered model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateTieredWithOffset() throws Exception {
    System.out.println("testRateCalculateTieredWithOffset");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel4";
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double expResult = 0.0;
    double result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 90; seconds++) {
      valueToRate = seconds;
      expResult = 1.5;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2 beats - try all integer values
    for (int seconds = 91; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 2.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }
    
    // Try with an offset - pretend that we are rating the second minute
    valueOffset = 60.0;
    
    // try the first beat after the offset, should get the 30 second beat, not the 60 second one
    for (int seconds = 1; seconds < 30; seconds++) {
      valueToRate = seconds;
      expResult = 0.5;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // try the second beat after the offset, should get the 30 second beat, not the 60 second one
    for (int seconds = 31; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateTiered(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }
  }

  /**
   * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
   * uses a simple non-tiered model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateTieredTimeBoundNonTiered() throws Exception {
    System.out.println("rateCalculateTieredTimeBoundNonTiered... Not implemented");
  }

  /**
   * Test of rateCalculateThreshold method, of class AbstractRateCalc. This test
   * uses a simple non-tiered threshold model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateThresholdNonTiered() throws Exception {
    System.out.println("rateCalculateThresholdNonTiered");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel1";
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    double expResult = 0.0;
    double result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st beat - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2 beats - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 2.0;
      result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 16667.0;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating
    valueToRate = 1000000;
    expResult = 0.0;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more
    valueToRate = 1500000;
    expResult = 0.0;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateThreshold method, of class AbstractRateCalc. This test
   * uses a simple tiered threshold model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateThresholdTiered() throws Exception {
    System.out.println("rateCalculateThresholdNonTiered");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel2";
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double valueToRate = 0.0;
    double valueOffset = 0.0;
    double expResult = 0.0;
    double result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st tier - try all integer values
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 1.0;
      result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // intra-beat 2, non integer value
    valueToRate = 2.654;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 2nd tier - try all integer values
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 0.2;
      result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run some large values through
    valueToRate = 999999;
    expResult = 1666.7;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating - no tier covers this
    valueToRate = 1000000;
    expResult = 0.0;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // run off the end of the rating some more - no tier covers this
    valueToRate = 1500000;
    expResult = 0.0;
    result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateThreshold method, of class AbstractRateCalc. This test
   * uses a simple tiered threshold model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateThresholdWithOffset() throws Exception {
    System.out.println("rateCalculateThresholdNonTiered");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel2";
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double valueToRate;
    double valueOffset = 60.0;
    double expResult;
    
    for (int seconds = 1; seconds < 60; seconds++) {
      valueToRate = seconds;
      expResult = 0.1;
      double result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }
    
    for (int seconds = 61; seconds < 120; seconds++) {
      valueToRate = seconds;
      expResult = 0.2;
      double result = instance.rateCalculateThreshold(priceModel, valueToRate, valueOffset, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }
  }

  /**
   * Test of rateCalculateFlat method, of class AbstractRateCalc. This test uses
   * a simple flat threshold model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateFlat() throws Exception {
    System.out.println("rateCalculateFlat");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel1";
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    double valueToRate = 0.0;
    double expResult = 0.0;
    double result = instance.rateCalculateFlat(priceModel, valueToRate, CDRDate);
    assertEquals(expResult, result, 0.00001);

    // intra-beat 1st tier - try all integer values
    for (int seconds = 1; seconds < 600; seconds++) {
      valueToRate = seconds;
      expResult = seconds / 60.0;
      result = instance.rateCalculateFlat(priceModel, valueToRate, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run off the end of the rating some more - no tier covers this
    valueToRate = 1500000;
    expResult = 25000.0;
    result = instance.rateCalculateFlat(priceModel, valueToRate, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of rateCalculateEvent method, of class AbstractRateCalc. This test
   * uses a simple event threshold model, with no time bounding.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testRateCalculateEvent() throws Exception {
    System.out.println("rateCalculateEvent");

    // Simple test using non-time bound non-tiered model
    String priceModel = "TestModel1";
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // zero value to rate
    long valueToRate;
    double expResult;
    double result;

    // intra-beat 1st tier - try all integer values
    for (int seconds = 0; seconds < 600; seconds++) {
      valueToRate = seconds;
      expResult = seconds;
      result = instance.rateCalculateEvent(priceModel, valueToRate, CDRDate);
      assertEquals(expResult, result, 0.00001);
    }

    // run off the end of the rating - rate what we can
    valueToRate = 1500000;
    expResult = 999999.0;
    result = instance.rateCalculateEvent(priceModel, valueToRate, CDRDate);
    assertEquals(expResult, result, 0.00001);
  }

  /**
   * Test of authCalculateTiered method, of class AbstractRateCalc. Work out how
   * much RUM can be got for the current available balance.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAuthCalculateTiered() throws Exception {
    System.out.println("authCalculateTiered");

    // Non-Tiered
    String priceModel = "TestModel1";
    double availableBalance = 10.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // We expect to get 600 seconds of credit for a balance of 10 @ 1/min
    double expResult = 600.0;
    double result = instance.authCalculateTiered(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Tiered
    priceModel = "TestModel2";

    // We expect to get 5460 seconds of credit for a balance of 1 @ 1/min + 90@0.1/MIN
    expResult = 5460.0;
    result = instance.authCalculateTiered(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Very large credit - just report the maximum we know about
    availableBalance = 100000.0;
    expResult = 999999.0;
    result = instance.authCalculateTiered(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Negative available balance - report 0
    availableBalance = -10.0;
    expResult = 0.0;
    result = instance.authCalculateTiered(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of authCalculateThreshold method, of class AbstractRateCalc.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAuthCalculateThreshold() throws Exception {
    System.out.println("authCalculateThreshold");

    // Non-Tiered
    String priceModel = "TestModel1";
    double availableBalance = 10.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // We expect to get 600 seconds of credit for a balance of 10 @ 1/min
    double expResult = 600.0;
    double result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Tiered
    priceModel = "TestModel2";

      // We expect to get 600 seconds, this being the smallest number of
    // seconds that we can predictably manage. 601 seconds would fall into
    // the next tier, and then we would have a diffent result.
    expResult = 600.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Very large credit - just report the maximum we know about
    availableBalance = 10000.0;
    expResult = 600000.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Negative available balance - report 0
    availableBalance = -10.0;
    expResult = 0.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of authCalculateFlat method, of class AbstractRateCalc.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAuthCalculateFlat() throws Exception {
    System.out.println("authCalculateFlat");

    // Non-Tiered
    String priceModel = "TestModel1";
    double availableBalance = 10.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // We expect to get 600 seconds of credit for a balance of 10 @ 1/min
    double expResult = 600.0;
    double result = instance.authCalculateFlat(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Tiered
    priceModel = "TestModel2";

      // We expect to get 600 seconds, this being the smallest number of
    // seconds that we can predictably manage. 601 seconds would fall into
    // the next tier, and then we would have a diffent result.
    expResult = 600.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Very large credit - just report the maximum we know about
    availableBalance = 10000.0;
    expResult = 600000.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Negative available balance - report 0
    availableBalance = -10.0;
    expResult = 0.0;
    result = instance.authCalculateThreshold(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);
  }

  /**
   * Test of authCalculateEvent method, of class AbstractRateCalc.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAuthCalculateEvent() throws Exception {
    System.out.println("authCalculateEvent");

    // Non-Tiered
    String priceModel = "TestModel1";
    double availableBalance = 10.0;
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    // We expect to get 600 seconds of credit for a balance of 10 @ 1/min
    double expResult = 10.0;
    double result = instance.authCalculateEvent(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Tiered
    priceModel = "TestModel2";

      // We expect to get 600 seconds, this being the smallest number of
    // seconds that we can predictably manage. 601 seconds would fall into
    // the next tier, and then we would have a diffent result.
    expResult = 10.0;
    result = instance.authCalculateEvent(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Very large credit - just report the maximum we know about
    availableBalance = 10000.0;
    expResult = 10000.0;
    result = instance.authCalculateEvent(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);

    // Negative available balance - report 0
    availableBalance = -10.0;
    expResult = 0.0;
    result = instance.authCalculateEvent(priceModel, availableBalance, CDRDate);
    assertEquals(expResult, result, 0.0);
  }
  
  public class AbstractRateCalcImpl extends AbstractRateCalc {

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
      instance = new AbstractRateCalcTest.AbstractRateCalcImpl();

      try {
        // Get the instance
        instance.init("DBTestPipe", "AbstractRateCalcTest");
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
}
