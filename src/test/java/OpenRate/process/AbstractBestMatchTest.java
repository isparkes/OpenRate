/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import org.junit.*;

/**
 * Unit test for AbstractBestMatch.
 *
 * @author tgdspia1
 */
public class AbstractBestMatchTest
{
  private static URL FQConfigFileName;

  private static AbstractBestMatch instance;

  // Used for logging and exception handling
  private static String message; 

 /**
  * This sets up a run time environment for testing modules. It is fairly
  * elaborate, because of the amount of support that a module needs in order
  * to function properly. It reports exceptions to the framework or pipeline
  * exception handler, logs to one of four logs, all of which are normally
  * found at runtime. For unit testing, we have to set these up manually.
  * 
  * @throws Exception 
  */
  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestBestMatchDB.properties.xml");
    
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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("BestMatchTestCache");

    try
    {
      JDBCChcon.prepareStatement("DROP TABLE TEST_BEST_MATCH;").execute();
    }
    catch (Exception ex)
    {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
          (ex.getMessage().startsWith("user lacks")))      // HSQL
      {
        // It's OK
      }
      else
      {
        // Not OK, fail the case
        message = "Error dropping table TEST_BEST_MATCH in test <AbstractBestMatchTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_BEST_MATCH (MAP_GROUP varchar(24),INPUT_VAL varchar(64), OUTPUT_VAL1 varchar(64), OUTPUT_VAL2 varchar(64));").execute();

    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','0044','UK','UK Any');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','00','INTL','Rest of the world');").execute();
    
    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale1','0032','WD1','Belgium');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale1','00328165','WD2','Termination to Belgium Geographical Number');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale2','0032','WD3','Belgium');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale2','00328165','WD4','Termination to Belgium Geographical Number');").execute();
    
    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }

  @AfterClass
  public static void tearDownClass()
  {
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
   * Test of getBestMatch method, of class AbstractBestMatch.
   */
  @Test
  public void testGetBestMatch()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("getBestMatch");

    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "UK";
    Assert.assertEquals(expResult, result);

    // Simple good case
    Group = "DefaultMap";
    BNumber = "004923434";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "INTL";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatch method, of class AbstractBestMatch - defect case
   */
  @Test
  public void testGetBestMatchErg()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("getBestMatchErg");

    // Simple good case
    Group = "WholeSale1";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "WD2";
    Assert.assertEquals(expResult, result);
    
    // Simple good case
    Group = "WholeSale2";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "WD4";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatch method, of class AbstractBestMatch - defect case
   */
  @Test
  public void testGetBestMatchNoResults()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("testGetBestMatchNoResults");

    // Access a map group (digit tree) we don't have at all
    Group = "WholeSale9";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
    
    // Simple good case
    Group = "WholeSale1";
    BNumber = "99999999999";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatchWithChildData method, of class AbstractBestMatch.
   */
  @Test
  public void testGetBestMatchWithChildData()
  {
    String BNumber;
    ArrayList<String> result;
    ArrayList<String> expResult;
    String Group;
    
    System.out.println("getBestMatchWithChildData");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.getBestMatchWithChildData(Group, BNumber);
    expResult = new ArrayList();
    expResult.add("UK");
    expResult.add("UK Any");
    Assert.assertEquals(expResult, result);

    // Simple good case
    Group = "DefaultMap";
    BNumber = "004923434";
    result = instance.getBestMatchWithChildData(Group, BNumber);
    expResult = new ArrayList();
    expResult.add("INTL");
    expResult.add("Rest of the world");
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidBestMatchResult method, of class AbstractBestMatch.
   */
  @Test
  public void testIsValidBestMatchResult_ArrayList()
  {
    String BNumber;
    boolean result;
    boolean expResult;
    String Group;

    System.out.println("isValidBestMatchResult");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.isValidBestMatchResult(instance.getBestMatchWithChildData(Group, BNumber));
    expResult = true;
    Assert.assertEquals(expResult, result);

    // Simple bad case
    Group = "DefaultMap";
    BNumber = "99999";
    result = instance.isValidBestMatchResult(instance.getBestMatchWithChildData(Group, BNumber));
    expResult = false;
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidBestMatchResult method, of class AbstractBestMatch.
   */
  @Test
  public void testIsValidBestMatchResult_String()
  {
    System.out.println("isValidBestMatchResult");
    
    String BNumber;
    boolean result;
    boolean expResult;
    String Group;

    System.out.println("isValidBestMatchResult");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.isValidBestMatchResult(instance.getBestMatch(Group, BNumber));
    expResult = true;
    Assert.assertEquals(expResult, result);

    // Simple bad case
    Group = "DefaultMap";
    BNumber = "99999";
    result = instance.isValidBestMatchResult(instance.getBestMatch(Group, BNumber));
    expResult = false;
    Assert.assertEquals(expResult, result);
  }

  public class AbstractBestMatchImpl extends AbstractBestMatch
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
  */
  private AbstractBestMatch getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractBestMatchTest.AbstractBestMatchImpl();
      
      // Get the instance
      try
      {
        instance.init("DBTestPipe", "AbstractBestMatchTest");
      }
      catch (InitializationException ex)
      {
        Assert.fail();
      }
    }
    else
    {
      Assert.fail("Instance already allocated");
    }
    
    return instance;
  }
  
 /**
  * Method to release an instance of the implementation.
  */
  private void releaseInstance()
  {
    instance = null;
  }
}
