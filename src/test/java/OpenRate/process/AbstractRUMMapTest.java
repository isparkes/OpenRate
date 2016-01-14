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
import org.junit.*;
import static org.junit.Assert.assertEquals;

/**
 * Test the RUM expansion functions. These take a price group and perform RUM
 * expansion on it so that it reflects all the elements of the price group.
 * 
 * This is in effect the first part of the RUM rating module.
 *
 * @author TGDSPIA1
 */
public class AbstractRUMMapTest {

  private static URL FQConfigFileName;
  private static AbstractRUMMap instance;

  // Used for logging and exception handling
  private static String message;
  private static OpenRate appl;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRUMMap.properties.xml");

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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RUMMapTestCache");

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
    // Deallocate the resources
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
   * Test the RUM expansion for a simple 1 model group. This should result
   * in a single charge packet populated according to the RUM map.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testRUMMapSingleModel() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testRUMMapSingleModel");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel1", 0);
    instance.evaluateRUMPriceGroup(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(1, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    ChargePacket tmpCP = ratingRecord.getChargePacket(0);
    TimePacket tmpTZ = ratingRecord.getChargePacket(0).getTimeZones().get(0);
    
    assertEquals("TestModel1", tmpTZ.priceGroup);
    assertEquals("TestModel1", tmpTZ.priceModel);
    assertEquals("DUR", tmpCP.rumName);
    assertEquals("EUR", tmpCP.resource);
    assertEquals(2, tmpCP.ratingType);
    assertEquals("TIERED", tmpCP.ratingTypeDesc);
  }

  /**
   * Test the RUM expansion for a 2 model group. This should result in two
   * charge packets, each populated according to the RUM map.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testRUMMapTwoModel() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testRUMMapTwoModel");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDUR(CDRDate, "TestModel2", 0);
    instance.evaluateRUMPriceGroup(ratingRecord);
    assertEquals(2, ratingRecord.getChargePacketCount());
    assertEquals(1, ratingRecord.getChargePacket(0).getTimeZones().size());
    assertEquals(1, ratingRecord.getChargePacket(1).getTimeZones().size());
    
    ChargePacket tmpCP1 = ratingRecord.getChargePacket(0);
    TimePacket tmpTZ1 = ratingRecord.getChargePacket(0).getTimeZones().get(0);

    assertEquals("TestModel2", tmpTZ1.priceGroup);
    assertEquals("TestModel2a", tmpTZ1.priceModel);
    assertEquals("DUR", tmpCP1.rumName);
    assertEquals("EUR", tmpCP1.resource);
    assertEquals(2, tmpCP1.ratingType);
    assertEquals("TIERED", tmpCP1.ratingTypeDesc);
    
    ChargePacket tmpCP2 = ratingRecord.getChargePacket(1);
    TimePacket tmpTZ2 = ratingRecord.getChargePacket(1).getTimeZones().get(0);

    assertEquals("TestModel2", tmpTZ2.priceGroup);
    assertEquals("TestModel2b", tmpTZ2.priceModel);
    assertEquals("DUR", tmpCP2.rumName);
    assertEquals("EUR", tmpCP2.resource);
    assertEquals(2, tmpCP2.ratingType);
    assertEquals("TIERED", tmpCP2.ratingTypeDesc);
  }

  /**
   * Test the RUM expansion for a record which has undergone time splitting. In
   * this case we get multiple charge packets in, and we treat each in turn.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testRUMMapTwoCPIn() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testRUMMapTwoCPIn");

    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

    ratingRecord = getNewRatingRecordDURTimeSplit(CDRDate, "TestModel5a", "TestModel5b", 0);
    instance.evaluateRUMPriceGroup(ratingRecord);
    assertEquals(3, ratingRecord.getChargePacketCount());
    assertEquals(1, ratingRecord.getChargePacket(0).getTimeZones().size());
    assertEquals(1, ratingRecord.getChargePacket(1).getTimeZones().size());
    assertEquals(1, ratingRecord.getChargePacket(2).getTimeZones().size());
    
    ChargePacket tmpCP1 = ratingRecord.getChargePacket(0);
    TimePacket tmpTZ1 = ratingRecord.getChargePacket(0).getTimeZones().get(0);
    
    assertEquals("TestModel5a", tmpTZ1.priceGroup);
    assertEquals("TestModel5a1", tmpTZ1.priceModel);
    assertEquals("DUR", tmpCP1.rumName);
    assertEquals("EUR", tmpCP1.resource);
    assertEquals(2, tmpCP1.ratingType);
    assertEquals("TIERED", tmpCP1.ratingTypeDesc);
    
    ChargePacket tmpCP2 = ratingRecord.getChargePacket(1);
    TimePacket tmpTZ2 = ratingRecord.getChargePacket(1).getTimeZones().get(0);
    
    assertEquals("TestModel5b", tmpTZ2.priceGroup);
    assertEquals("TestModel5b1", tmpTZ2.priceModel);
    assertEquals("DUR", tmpCP2.rumName);
    assertEquals("EUR", tmpCP2.resource);
    assertEquals(2, tmpCP2.ratingType);
    assertEquals("TIERED", tmpCP2.ratingTypeDesc);
    
    ChargePacket tmpCP3 = ratingRecord.getChargePacket(2);
    TimePacket tmpTZ3 = ratingRecord.getChargePacket(2).getTimeZones().get(0);
    
    assertEquals("TestModel5b", tmpTZ3.priceGroup);
    assertEquals("TestModel5b2", tmpTZ3.priceModel);
    assertEquals("DUR", tmpCP3.rumName);
    assertEquals("EUR", tmpCP3.resource);
    assertEquals(2, tmpCP3.ratingType);
    assertEquals("TIERED", tmpCP3.ratingTypeDesc);
  }

  public class AbstractRUMMapImpl extends AbstractRUMMap {

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
      instance = new AbstractRUMMapTest.AbstractRUMMapImpl();

      try {
        // Get the instance
        instance.init("DBTestPipe", "AbstractRUMMapTest");
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
    ratingRecord.utcEventDate = CDRDate;

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
    ratingRecord.utcEventDate = CDRDate;

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
    ratingRecord.utcEventDate = CDRDate;

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
    ratingRecord.utcEventDate = CDRDate;

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
    ratingRecord.utcEventDate = CDRDate;

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
