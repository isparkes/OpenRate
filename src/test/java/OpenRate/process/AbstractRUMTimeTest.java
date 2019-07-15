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
import static OpenRate.process.AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING;
import static OpenRate.process.AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING;
import static OpenRate.process.AbstractRUMTimeMatch.TIME_SPLITTING_NO_CHECK;
import OpenRate.record.ChargePacket;
import OpenRate.record.IRecord;
import OpenRate.record.TimePacket;
import OpenRate.utils.ConversionUtils;
import TestUtils.FrameworkUtils;
import TestUtils.TestRatingRecord;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Calendar;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the splitting of a time based rating according to time zones.
 *
 * @author TGDSPIA1
 */
public class AbstractRUMTimeTest {

  private static URL FQConfigFileName;
  private static AbstractRUMTimeMatch instance;

  // Used for logging and exception handling
  private static String message;
  private static OpenRate appl;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRUMTime.properties.xml");

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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RUMTimeTestCache");

    try {
      JDBCChcon.prepareStatement("DROP TABLE TEST_TIME_MODEL_MAP").execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
              (ex.getMessage().startsWith("user lacks"))) // HSQL
      {
        // It's OK
      } else {
        // Not OK, fail the case
        message = "Error dropping table TEST_TIME_MODEL_MAP in test <AbstractRUMTimeTest>.";
        Assert.fail(message);
      }
    }

    try {
      JDBCChcon.prepareStatement("DROP TABLE TEST_TIME_MODEL_INTERVAL").execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
              (ex.getMessage().startsWith("user lacks"))) // HSQL
      {
        // It's OK
      } else {
        // Not OK, fail the case
        message = "Error dropping table TEST_TIME_MODEL_INTERVAL in test <AbstractRUMTimeTest>.";
        Assert.fail(message);
      }
    }

    // ******************************* TIME MODEL *****************************
    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_TIME_MODEL_MAP (ID int, PRODUCT_NAME_IN varchar(24), TIME_MODEL_OUT varchar(24))").execute();

    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_MAP (ID,PRODUCT_NAME_IN,TIME_MODEL_OUT) VALUES (1,'Default','Standard')").execute();

    // ******************************* TIME INTERVAL ***************************
    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_TIME_MODEL_INTERVAL (ID int, TIME_MODEL_NAME_IN varchar(24), DAY_IN varchar(24), FROM_IN varchar(24), TO_IN varchar(24), RESULT_OUT varchar(24))").execute();

    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (1,'FLAT','0','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (2,'FLAT','1','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (3,'FLAT','2','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (4,'FLAT','3','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (5,'FLAT','4','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (6,'FLAT','5','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (7,'FLAT','6','00:00','23:59','FLAT')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (8,'Standard','1','00:00','07:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (9,'Standard','1','08:00','18:59','PEAK')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (10,'Standard','1','19:00','23:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (11,'Standard','2','00:00','07:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (12,'Standard','2','08:00','18:59','PEAK')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (13,'Standard','2','19:00','23:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (14,'Standard','3','00:00','07:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (15,'Standard','3','08:00','18:59','PEAK')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (16,'Standard','3','19:00','23:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (17,'Standard','4','00:00','07:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (18,'Standard','4','08:00','18:59','PEAK')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (19,'Standard','4','19:00','23:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (20,'Standard','5','00:00','07:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (21,'Standard','5','08:00','18:59','PEAK')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (22,'Standard','5','19:00','23:59','ECO')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (24,'Standard','0','00:00','23:59','WKD')").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_TIME_MODEL_INTERVAL (ID,TIME_MODEL_NAME_IN,DAY_IN,FROM_IN,TO_IN,RESULT_OUT) VALUES (23,'Standard','6','00:00','23:59','WKD')").execute();

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
   * Test the simple, non-crossing case.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTImeMatchNonCrossing() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTImeMatchNonCrossing");

    ratingRecord = getNewRatingRecord("2010-01-23 00:00:00","2010-01-23 00:00:01", TIME_SPLITTING_NO_CHECK);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(1, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    TimePacket tmpTZ = ratingRecord.getChargePackets().get(0).getTimeZones().get(0);
    assertEquals(1, tmpTZ.duration);
    assertEquals(1, tmpTZ.totalDuration);
    assertEquals("WKD", tmpTZ.timeResult);
  }

  /**
   * Test the simple, crossing case, but with splitting turned off. In this case
   * we create one charge packet with the time zone based on the start time.
   * 
   * Note that because we do no splitting, the duration and total duration
   * fields are not filled in.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTImeMatchCrossingNoSplit() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTImeMatchCrossingNoSplit");

    ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-20 08:10:01", TIME_SPLITTING_NO_CHECK);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(1, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    TimePacket tmpTZ = ratingRecord.getChargePackets().get(0).getTimeZones().get(0);
    assertEquals(1, tmpTZ.duration);
    assertEquals(1, tmpTZ.totalDuration);
    assertEquals("ECO", tmpTZ.timeResult);
  }
  
  /**
   * Test the simple, crossing case, with splitting. In this case
   * we create one charge packet with one time packet per time zone.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTImeMatchCrossingSplit() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTImeMatchCrossingSplit");

    ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-20 08:10:01", TIME_SPLITTING_CHECK_SPLITTING);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(2, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    TimePacket tmpTZ1 = ratingRecord.getChargePackets().get(0).getTimeZones().get(0);
    assertEquals(600, tmpTZ1.duration);
    assertEquals(1201, tmpTZ1.totalDuration);
    assertEquals("ECO", tmpTZ1.timeResult);
    
    TimePacket tmpTZ2 = ratingRecord.getChargePackets().get(0).getTimeZones().get(1);
    assertEquals(601, tmpTZ2.duration);
    assertEquals(1201, tmpTZ2.totalDuration);
    assertEquals("PEAK", tmpTZ2.timeResult);
  }
  
  /**
   * Test the simple, crossing case, with splitting. In this case
   * we create one charge packet with one time packet per time zone.
   * 
   * At this point the "Beat Rounding" mode gives exactly the same results
   * as the normal splitting. It however is treated differently during the
   * rating that will follow.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTImeMatchCrossingSplitBeatRounding() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTImeMatchCrossingSplitBeatRounding");

    ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-20 08:10:01", TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(2, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    TimePacket tmpTZ1 = ratingRecord.getChargePackets().get(0).getTimeZones().get(0);
    assertEquals(600, tmpTZ1.duration);
    assertEquals(1201, tmpTZ1.totalDuration);
    assertEquals("ECO", tmpTZ1.timeResult);
    
    TimePacket tmpTZ2 = ratingRecord.getChargePackets().get(0).getTimeZones().get(1);
    assertEquals(601, tmpTZ2.duration);
    assertEquals(1201, tmpTZ2.totalDuration);
    assertEquals("PEAK", tmpTZ2.timeResult);
  }
  
  /**
   * Test the multiple crossing case, with splitting. In this case
   * we create one charge packet with one time packet per time zone.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTimeMatchCrossingSplitLongCall() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTimeMatchCrossingSplitLongCall");

    ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-22 08:10:01", TIME_SPLITTING_CHECK_SPLITTING);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(8, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    int ourTotalDuration = 0;
    TimePacket tmpTZ1 = ratingRecord.getChargePackets().get(0).getTimeZones().get(0);
    assertEquals(600, tmpTZ1.duration);
    assertEquals(174001, tmpTZ1.totalDuration);
    assertEquals("ECO", tmpTZ1.timeResult);
    ourTotalDuration += tmpTZ1.duration;
    
    TimePacket tmpTZ2 = ratingRecord.getChargePackets().get(0).getTimeZones().get(1);
    assertEquals(39600, tmpTZ2.duration);
    assertEquals(174001, tmpTZ2.totalDuration);
    assertEquals("PEAK", tmpTZ2.timeResult);
    ourTotalDuration += tmpTZ2.duration;
    
    TimePacket tmpTZ3 = ratingRecord.getChargePackets().get(0).getTimeZones().get(2);
    assertEquals(18000, tmpTZ3.duration);
    assertEquals(174001, tmpTZ3.totalDuration);
    assertEquals("ECO", tmpTZ3.timeResult);
    ourTotalDuration += tmpTZ3.duration;
    
    TimePacket tmpTZ4 = ratingRecord.getChargePackets().get(0).getTimeZones().get(3);
    assertEquals(28800, tmpTZ4.duration);
    assertEquals(174001, tmpTZ4.totalDuration);
    assertEquals("ECO", tmpTZ4.timeResult);
    ourTotalDuration += tmpTZ4.duration;
    
    TimePacket tmpTZ5 = ratingRecord.getChargePackets().get(0).getTimeZones().get(4);
    assertEquals(39600, tmpTZ5.duration);
    assertEquals(174001, tmpTZ5.totalDuration);
    assertEquals("PEAK", tmpTZ5.timeResult);
    ourTotalDuration += tmpTZ5.duration;
    
    TimePacket tmpTZ6 = ratingRecord.getChargePackets().get(0).getTimeZones().get(5);
    assertEquals(18000, tmpTZ6.duration);
    assertEquals(174001, tmpTZ6.totalDuration);
    assertEquals("ECO", tmpTZ6.timeResult);
    ourTotalDuration += tmpTZ6.duration;
    
    TimePacket tmpTZ7 = ratingRecord.getChargePackets().get(0).getTimeZones().get(6);
    assertEquals(28800, tmpTZ7.duration);
    assertEquals(174001, tmpTZ7.totalDuration);
    assertEquals("ECO", tmpTZ7.timeResult);
    ourTotalDuration += tmpTZ7.duration;
    
    TimePacket tmpTZ8 = ratingRecord.getChargePackets().get(0).getTimeZones().get(7);
    assertEquals(601, tmpTZ8.duration);
    assertEquals(174001, tmpTZ8.totalDuration);
    assertEquals("PEAK", tmpTZ8.timeResult);
    ourTotalDuration += tmpTZ8.duration;
    
    assertEquals(tmpTZ1.totalDuration, ourTotalDuration);    
  }
  
  /**
   * Test the performance with a standard case. We expect way more than 10,000 
   * per second.
   * 
   * @throws java.lang.Exception
   */
  @Test
  public void testPerformRUMTimeMatchCrossingPerformance() throws Exception {
    TestRatingRecord ratingRecord;
    System.out.println("testPerformRUMTimeMatchCrossingPerformance");
    
    // Test the result is right
    ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-20 08:10:01", TIME_SPLITTING_CHECK_SPLITTING);
    instance.performRUMTimeMatch(ratingRecord);
    assertEquals(1, ratingRecord.getChargePacketCount());
    assertEquals(2, ratingRecord.getChargePacket(0).getTimeZones().size());
    
    long startMs = Calendar.getInstance().getTimeInMillis();

    for (int i = 1; i < 10000; i++) {
      ratingRecord = getNewRatingRecord("2010-01-20 07:50:00","2010-01-20 08:10:01", TIME_SPLITTING_CHECK_SPLITTING);
      instance.performRUMTimeMatch(ratingRecord);
    }

    long duration = Calendar.getInstance().getTimeInMillis() - startMs;

    // Dropped to 5000/s because of our crappy Jenkins
    // TODO revert to 10k/s when we upgrade Jenkins (memory expansion)
    System.out.println("10000 took " + duration + "mS");
    assertTrue(duration < 2000);
  }

  public class AbstractRUMTimeMatchImpl extends AbstractRUMTimeMatch {

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
      instance = new AbstractRUMTimeTest.AbstractRUMTimeMatchImpl();

      try {
        // Get the instance
        instance.init("DBTestPipe", "AbstractRUMTimeTest");
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
  private TestRatingRecord getNewRatingRecord(String CDRStartDate, String CDREndDate, int splittingType) throws ParseException {
    TestRatingRecord ratingRecord = new TestRatingRecord();
    
    ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
    conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
    long CDRDateUTC = conv.convertInputDateToUTC(CDRStartDate);
    ratingRecord.utcEventDate = CDRDateUTC;
    ratingRecord.eventStartDate = conv.getDatefromLongFormat(CDRStartDate);
    ratingRecord.eventEndDate = conv.getDatefromLongFormat(CDREndDate);
    ChargePacket tmpCP = new ChargePacket();
    tmpCP.timeModel = "Default";
    tmpCP.timeSplitting = splittingType;
    ratingRecord.addChargePacket(tmpCP);

    return ratingRecord;
  }
}
