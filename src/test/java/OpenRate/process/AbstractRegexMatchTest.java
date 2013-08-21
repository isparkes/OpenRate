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
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import org.junit.*;

/**
 * Unit test for AbstractRegexMatch.
 *
 * @author tgdspia1
 */
public class AbstractRegexMatchTest
{
  private static URL FQConfigFileName;
  private static AbstractRegexMatch instance;

  // Used for logging and exception handling
  private static String message; 

 /**
  * Default constructor
  */
  public AbstractRegexMatchTest()
  {
    // Not used
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestRegexDB.properties.xml");
    
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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RegexMatchTestCache");

    try
    {
      JDBCChcon.prepareStatement("DROP TABLE TEST_REGEX").execute();
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
        // Not OK, Assert.fail the case
        message = "Error dropping table TEST_REGEX in test <AbstractRegexMatchTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_REGEX (MAP_GROUP varchar(24), INPUT_VAL1 varchar(64), INPUT_VAL2 varchar(64), OUTPUT_VAL1 varchar(64), OUTPUT_VAL2 varchar(64), RANK int);").execute();

    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('DefaultMap','01.*','.*','OK1','OUT2',1);").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('DefaultMap','0.*','.*','OK2','OUT2',2);").execute();

    // Get the caches that we are using
    FrameworkUtils.startupCaches();
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
   * Test of init method, of class AbstractRegexMatch.
   */
  @Test
  public void testInit() throws Exception
  {
    System.out.println("init");

    // get the instance
    getInstance();
  }

  /**
   * Test of procHeader method, of class AbstractRegexMatch.
   */
  @Test
  public void testProcHeader()
  {
    System.out.println("procHeader");
  }

  /**
   * Test of procTrailer method, of class AbstractRegexMatch.
   */
  @Test
  public void testProcTrailer()
  {
    System.out.println("procTrailer");
  }

  /**
   * Test of getRegexMatch method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetRegexMatch()
  {
    String result;
    String expResult;
    String Group;

    System.out.println("getRegexMatch");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractRegexMatchTest>";
      Assert.fail(message);
    }

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "023456";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK2";
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);

    // Simple good case with lower rank
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK1";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getRegexMatchWithChildData method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetRegexMatchWithChildData()
  {
    ArrayList<String> result;
    ArrayList<String> expResult = new ArrayList<>();
    String Group;

    System.out.println("getRegexMatchWithChildData");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractRegexMatchTest>";
      Assert.fail(message);
    }

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "023456";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("OK2");
    expResult.add("OUT2");
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("NOMATCH");
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("NOMATCH");
    Assert.assertEquals(expResult, result);

    // Simple good case with lower rank
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("OK1");
    expResult.add("OUT2");
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getAllEntries method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetAllEntries()
  {
    System.out.println("getAllEntries");

    ArrayList<String> result;
    String expResult;
    String Group;
    int resultCount;

    System.out.println("getRegexMatch");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractRegexMatchTest>";
      Assert.fail(message);
    }

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getAllEntries(Group, searchParameters);
    resultCount = 2;
    Assert.assertEquals(resultCount, result.size());

    expResult = "OK1";
    Assert.assertEquals(expResult, result.get(0));

    expResult = "OK2";
    Assert.assertEquals(expResult, result.get(1));

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getAllEntries(Group, searchParameters);
    resultCount = 0;
    Assert.assertEquals(resultCount, result.size());
  }

  /**
   * Test of isValidRegexMatchResult method, of class AbstractRegexMatch.
   */
  @Test
  public void testIsValidRegexMatchResult_ArrayList()
  {
    ArrayList<String> resultToCheck;
    String Group;
    String expResultMatch;
    boolean expResult;
    boolean result;

    System.out.println("isValidRegexMatchResult");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractRegexMatchTest>";
      Assert.fail(message);
    }

    String[] searchParameters = new String[1];

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResultMatch = "NOMATCH";
    Assert.assertEquals(expResultMatch, resultToCheck.get(0));

    // Check that we have the result we need
    expResult = false;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);

    // Simple pass case
    Group = "DefaultMap";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResultMatch = "OK2";
    Assert.assertEquals(expResultMatch, resultToCheck.get(0));

    // Check that we have the result we need
    expResult = true;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidRegexMatchResult method, of class AbstractRegexMatch.
   */
  @Test
  public void testIsValidRegexMatchResult_String()
  {
    String resultToCheck;
    String Group;
    String expResultMatch;
    boolean expResult;
    boolean result;

    System.out.println("isValidRegexMatchResult");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractRegexMatchTest>";
      Assert.fail(message);
    }

    String[] searchParameters = new String[1];

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatch(Group, searchParameters);
    expResultMatch = "NOMATCH";
    Assert.assertEquals(expResultMatch, resultToCheck);

    // Check that we have the result we need
    expResult = false;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);

    // Simple pass case
    Group = "DefaultMap";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatch(Group, searchParameters);
    expResultMatch = "OK2";
    Assert.assertEquals(expResultMatch, resultToCheck);

    // Check that we have the result we need
    expResult = true;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);
  }

  public class AbstractRegexMatchImpl extends AbstractRegexMatch
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
  private void getInstance() throws InitializationException
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractRegexMatchTest.AbstractRegexMatchImpl();

      // Get the instance
      instance.init("DBTestPipe", "AbstractRegexMatchTest");
    }
  }
}
