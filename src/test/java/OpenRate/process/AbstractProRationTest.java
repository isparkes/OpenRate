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
package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.lang.ProRatingResult;
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import junit.framework.Assert;
import org.junit.*;

/**
 *
 * @author tgdspia1
 */
public class AbstractProRationTest
{
  private static URL FQConfigFileName;
  private static AbstractProRation instance;

 /**
  * Default constructor
  */
  public AbstractProRationTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestDB.properties.xml");
    
   // Set up the OpenRate internal logger - this is normally done by app startup
    OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();
   }

  @AfterClass
  public static void tearDownClass() {
    // Deallocate
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
   * Test of calculateProRatedMonth method, of class AbstractProRation.
   * @throws ParseException
   * @throws InitializationException
   */
  @Test
  public void testCalculateProRatedMonth() throws ParseException, InitializationException
  {
    ProRatingResult result;
    int expResultDays;
    int expResultMonths;
    double expResultFactor;
    Date StartDate;
    Date EndDate;
    boolean useCalendarDays;
    SimpleDateFormat sdfIn = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("calculateProRatedMonth");

    // Simple good case, with 30 day months
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120303000000");
    useCalendarDays = false;

    expResultDays = 1;
    expResultMonths = 2;
    expResultFactor = 2.0333;
    result = instance.calculateProRatedMonth(StartDate, EndDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Simple good case, with variable day months
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120303000000");
    useCalendarDays = true;

    expResultDays = 1;
    expResultMonths = 2;
    expResultFactor = 2.0323;
    result = instance.calculateProRatedMonth(StartDate, EndDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));
  }


  /**
   * Test of calculateProRationFactor method, of class AbstractProRation.
   * @throws InitializationException
   * @throws ParseException
   */
  @Test
  public void testCalculateProRationFactor() throws InitializationException, ParseException
  {
    ProRatingResult result;
    int expResultDays;
    int expResultMonths;
    double expResultFactor;
    Date StartDate;
    Date EndDate;
    Date PeriodStartDate;
    Date PeriodEndDate;
    Date EventDate;
    boolean useCalendarDays;
    SimpleDateFormat sdfIn = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("calculateProRationFactor");

    // We are using the number of days in the month
    useCalendarDays = true;

    // Date before validity period
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120303000000");
    EventDate = sdfIn.parse("20111231000000");
    expResultDays = 0;
    expResultMonths = 0;
    expResultFactor = 0;
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Date after validity period
    EventDate = sdfIn.parse("20120430000000");

    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Full month in the middle of the validity period
    EventDate = sdfIn.parse("20120220000000");
    expResultDays = 29;
    expResultMonths = 0;
    expResultFactor = 1;
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Start month portion
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120303000000");
    EventDate = sdfIn.parse("20120120000000");
    expResultDays = 29;
    expResultMonths = 0;
    expResultFactor = 0.9667;
    PeriodStartDate = sdfIn.parse("20120103000000");
    PeriodEndDate = sdfIn.parse("20120131235959");
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    useCalendarDays = false;
    expResultFactor = 0.9355;
    PeriodStartDate = sdfIn.parse("20120103000000");
    PeriodEndDate = sdfIn.parse("20120131235959");
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));
    useCalendarDays = false;

    // End month portion
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120302000010");
    EventDate = sdfIn.parse("20120301000000");
    expResultDays = 2;
    expResultMonths = 0;
    expResultFactor = 0.0645;
    PeriodStartDate = sdfIn.parse("20120301000000");
    PeriodEndDate = sdfIn.parse("20120302000010");
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Start and end month the same
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120112000000");
    EventDate = sdfIn.parse("20120106000000");
    expResultDays = 9;
    expResultMonths = 0;
    expResultFactor = 0.2903;
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    PeriodStartDate = sdfIn.parse("20120103000000");
    PeriodEndDate = sdfIn.parse("20120112000000");
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Start and end month the same, 30 days months
    useCalendarDays = true;
    StartDate = sdfIn.parse("20120103000000");
    EndDate = sdfIn.parse("20120112000000");
    EventDate = sdfIn.parse("20120106000000");
    expResultDays = 9;
    expResultMonths = 0;
    expResultFactor = 0.3;
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // real life example
    useCalendarDays = false;
    StartDate = sdfIn.parse("20120116000000");
    EndDate = sdfIn.parse("20380119041407");
    EventDate = sdfIn.parse("20120118120000");
    expResultDays = 16;
    expResultMonths = 0;
    expResultFactor = 0.5161;
    PeriodStartDate = sdfIn.parse("20120116000000");
    PeriodEndDate = sdfIn.parse("20120131235959");
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));

    // Cal days version of the above
    useCalendarDays = true;
    expResultFactor = 0.5333;
    result = instance.calculateProRationFactor(StartDate, EndDate, EventDate, useCalendarDays);
    Assert.assertEquals(PeriodStartDate, result.getPeriodStartDate());
    Assert.assertEquals(PeriodEndDate, result.getPeriodEndDate());
    Assert.assertEquals(expResultDays, result.getDaysInPeriod());
    Assert.assertEquals(expResultMonths, result.getMonthsInPeriod());
    Assert.assertEquals(Math.round(expResultFactor * 10000), Math.round(result.getProRationFactor() * 10000));
  }

  /**
   * Test of getDaysBetweenDates method, of class AbstractProRation.
   * @throws InitializationException
   * @throws ParseException
   */
  @Test
  public void testGetDaysBetweenDates() throws InitializationException, ParseException
  {
    System.out.println("getDaysBetweenDates");

    SimpleDateFormat sdfIn = new SimpleDateFormat("yyyyMMddhhmmss");

    // Simple good case
    Date StartDate = sdfIn.parse("20120103000000");
    Date EndDate = sdfIn.parse("20120303000000");

    int result = instance.getDaysBetweenDates(StartDate, EndDate);
    int expResult = 60;

    Assert.assertEquals(expResult, result);
  }

  public class AbstractProRationImpl extends AbstractProRation
  {
   /**
    * Override the unused event handling routines.
    *
    * @param r input record
    * @return return record
    * @throws ProcessingException
    */
    @Override
    public IRecord procValidRecord(IRecord r) throws ProcessingException
    {
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
    public IRecord procErrorRecord(IRecord r) throws ProcessingException
    {
      return r;
    }
  }

 /**
  * Method to get an instance of the implementation. Done this way to allow
  * tests to be executed individually.
  *
  * @throws InitializationException
  */
  private void getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractProRationTest.AbstractProRationImpl();
      
      try
      {
        // Get the instance
        instance.init("DBTestPipe", "AbstractMultipleValidityMatchTest");
      }
      catch (InitializationException ex)
      {
        org.junit.Assert.fail();
      }

    }
    else
    {
      org.junit.Assert.fail("Instance already allocated");
    }
  }
  
 /**
  * Method to release an instance of the implementation.
  */
  private void releaseInstance()
  {
    instance = null;
  }
}
