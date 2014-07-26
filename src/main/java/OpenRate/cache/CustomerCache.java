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

package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.lang.ProductList;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a cache of customer information for use in
 * rating based on product instances. No product history is kept, and therefore
 * rating can only be done at the current state of the information. This means
 * that this class is of limited value for rating delayed usage, as it is not
 * guaranteed that the record will be rated using the products that were in
 * use at the time of the record creation. If you need a fully historicised
 * version, use the "CustomerCacheAudited" cache instead.
 *
 * The customer information can be recovered from either files or DataBase
 * data sources.
 *
 * There are some limitations in this cache implementation for simplicity, which
 * are not present in the CustomerCacheAudited module:
 *  - Aliases are not time bound. This means that an alias can only ever be
 *    attached to one account and no re-use can be permitted. A common way of
 *    overcoming this limitation is to lookup the external key in an instance
 *    of a "ValiditySegmentCache" to perform a translation from the external
 *    key to the internal key, and then use the internal key as the alias.
 *  - Subscriptions are not supported. That means that for instance, you cannot
 *    own more than one instance of a real-life level product, as there is
 *    no way of partitioning products to aliases. This has the effect of "mixing"
 *    all the products that an account has for all of the aliases. The only
 *    level of partitioning that is available is at Service level. This means
 *    that a product can be assigned a "Service", and for rating purposes, only
 *    products matching a particular service are recovered.
 *
 * ------------------------------ File Interface -------------------------------
 *
 * The verbs in the file are:
 *
 *     01 = Add Customer Account
 *     02 = Add Customer Product
 *     03 = Modify Customer Product
 *     04 = Change Customer Identifier
 *     05 = Add Alias
 *     06 = Add/Modify ERA
 *
 * Verb descriptions:
 *
 *     01, Add Customer Account
 *     Record format:
 *       01;CustomerIdentifier;ValidFrom;ValidTo;BalanceGroup
 *
 *     02, Add Customer Product
 *     Record format:
 *       02;CustomerIdentifier;Service;ProductIdentifier;ValidFrom;ValidTo
 *
 *     03, Modify Customer Product
 *     Record format:
 *       03;CustomerIdentifier;Service;ProductIdentifier;ValidFrom*;ValidTo*
 *       (* means that the field is optional, filling it will change the
 *          value, leaving it blank leaves the previous value)
 *
 *     04, Change Customer Identifier
 *     Record format:
 *       04;CustomerIdentifierOld;CustomerIdentifierNew
 *
 *     05, Add Alias
 *     Record format:
 *       05;Alias;CustomerIdentifier
 *
 *     06, Add/Modify ERA
 *     Record format:
 *       06;CustomerIdentifier;ERA_ID;Value
 *
 * ------------------------------- DB Interface --------------------------------
 *
 * If the loading is done from a DB, the queries that are executed are:
 *
 * AliasSelectQuery: This query should return a list of the aliases that are
 * to be associated with each account. The query should return:
 *
 * 1) ALIAS
 * 2) CUSTOMER_IDENTIFIER
 *
 * CustomerSelectStatement: This query returns a list of all of the customer
 * accounts that the system should handle. The query should return:
 *
 * 1) CUSTOMER_IDENTIFIER
 * 2) VALID_FROM (YYYYMMDDHHMMSS)
 * 3) VALID_TO (YYYYMMDDHHMMSS)
 * 4) BALANCE_GROUP
 *
 * ProductSelectQuery: This query returns a list of all the products that are
 * associated with a customer account. The query should return:
 *
 * 1) CUSTOMER_IDENTIFIER
 * 2) SERVICE
 * 3) PRODUCT_NAME
 * 4) VALID_FROM (YYYYMMDDHHMMSS)
 * 5) VALID_TO (YYYYMMDDHHMMSS)
 *
 * ERASelectQuery: This query returns a list of the ERAs (Extended Rating
 * Attributes) associated with the account. The query should return:
 *
 * 1) CUSTOMER_IDENTIFIER
 * 2) ERA_NAME
 * 3) ERA_VALUE
 *
 * @author i.sparkes
 */
public class CustomerCache
    extends AbstractSyncLoaderCache
{
  // Used to allow alias maps - takes a alias and maps to a poid.
  private ConcurrentHashMap<String, String> aliasCache;

  // The CustIDCache holds the aliases for the account
  private ConcurrentHashMap<String, CustInfo> CustIDCache;

  /**
   * The alias data select query is used to recover alias information from the
   * database. Aliases are the keys used to locate the customer account to use
   * for rating the traffic
   */
  protected static String aliasSelectQuery;

  /**
   * prepared statement for the alias query
   */
  protected static PreparedStatement stmtAliasSelectQuery;

  /**
   * The customer data select query is used to recover customer information
   * from the database
   */
  protected static String customerSelectQuery;

  /**
   * prepared statement for the customer data query
   */
  protected static PreparedStatement stmtCustomerSelectQuery;

 /**
  * The product data select query is used to recover the product infromation
  * from the database and associate it to the account
  */
  protected static String productSelectQuery;

  /**
   * prepared statement for the product query
   */
  protected static PreparedStatement stmtProductSelectQuery;

 /**
  * The ERA data select query is used to recover the "Extended Rating
  * Attribute" information from the database and associate it to the account
  */
  protected static String eraSelectQuery;

  /**
   * prepared statement for the ERA query
   */
  protected static PreparedStatement stmtERASelectQuery;

 /**
  * The internal date format is the format that by default will be used when
  * interpreting dates that come in the queries. The value here is the default
  * value, but this can be changed.
  */
  protected String internalDateFormat = "yyyyMMddHHmmss";

  /**
   * The CustInfo structure holds the information about the customer account,
   * including the validity dates, the product list and the balance group
   * reference. Note that we are using the dates as long integers to reduce
   * the total amount of storage that is required.
   */
  private class CustInfo
  {
    private     long UTCValidFrom;
    private     long UTCValidTo;
    private     ArrayList<CustProductInfo> CPI = null;
    private     int ProductCount = 0;
    private     int BalanceGroup = 0;
    private     ConcurrentHashMap<String, String> ERAList = null;
  }

  /**
   * The CustProductInfo structure holds the information about the products the,
   * customer has, including the validity dates. Note that we are using long integers
   * for the dates to reduce storage requirements.
   */
  private class CustProductInfo
  {
    private String ProductID=null;
    private String Service=null;
    private   long UTCValidFrom;
    private   long UTCValidTo;
  }

   /** Constructor
    * Creates a new instance of the Customer Cache. The Cache
    * contains all of the Customer IDs that have been cached.
    */
  public CustomerCache()
  {
    super();

    CustIDCache = new ConcurrentHashMap<>(5000);
    aliasCache = new ConcurrentHashMap<>(5000);
  }

 /**
  * Add an alias to the customer cache. An alias is a representation of any
  * identifier that can be used to locate the account
  *
  * @param alias The identifier that should be used to locate the account
  * @param CustId The account that should be located
  */
  public void addAlias(String alias, String CustId)
  {
    // Update the alias list
    if (!aliasCache.containsKey(alias))
    {
      aliasCache.put(alias,CustId);
    }
    else
    {
        // Otherwise write an error and ignore it
        OpenRate.getOpenRateFrameworkLog().error("Alias ID <" + alias + "> already exists.");
    }
  }

 /**
  * Add a Customer object into the CustomerCache.
  *
  * @param CustId The customer identifier
  * @param ValidFrom Valid from date of the customer relationship
  * @param ValidTo Valid to date of the customer relationship
  * @param BalanceGroup The ID of the counter balance group
  */
  public void addCustId(String CustId,long ValidFrom,long ValidTo,int BalanceGroup)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (!CustIDCache.containsKey(CustId))
    {
      // Check validity dates
      if (ValidTo <= ValidFrom)
      {
        // Otherwise write an error and ignore it
        OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> valid from <" + ValidFrom + "> is after valid to <" + ValidTo + ">. Add failed.");
        return;
      }

      // Create the new entry for the customer ID
      tmpCustInfo = new CustInfo();
      tmpCustInfo.CPI = new ArrayList<>();
      tmpCustInfo.ERAList = new ConcurrentHashMap<>(10);
      tmpCustInfo.UTCValidFrom = ValidFrom;
      tmpCustInfo.UTCValidTo   = ValidTo;
      tmpCustInfo.BalanceGroup = BalanceGroup;
      CustIDCache.put(CustId,tmpCustInfo);
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> already exists. Add failed.");
    }
  }

 /**
  * Set the date format we are using
  *
  * @param newDateFormat The new format of the date
  */
  public void setDateFormat(String newDateFormat)
  {
    internalDateFormat = newDateFormat;
  }

 /**
  * Add a CPI (CustomerProductInstance) value into the CustomerCache
  *
  * @param CustId The customer ID to add the product to
  * @param Service The service of the product
  * @param ProdID The product identifier
  * @param ValidFrom The start of the product validity
  * @param ValidTo The end of the product validity
  */
  public void addCPI(String CustId, String Service, String ProdID, long ValidFrom, long ValidTo)
  {
    CustInfo tmpCustInfo;
    CustProductInfo tmpCPI;

    // See if we already have ID for this customer
    if (CustIDCache.containsKey(CustId))
    {
      // Check validity dates
      if (ValidTo <= ValidFrom)
      {
        // Otherwise write an error and ignore it
        OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> product <" + ProdID + "> valid from <" + ValidFrom + "> is after valid to <" + ValidTo + ">. Add failed.");
        return;
      }

      // Create the new entry for the customer ID
      tmpCustInfo = CustIDCache.get(CustId);
      tmpCPI = new CustProductInfo();
      tmpCPI.Service = Service;
      tmpCPI.ProductID = ProdID;
      tmpCPI.UTCValidFrom = ValidFrom;
      tmpCPI.UTCValidTo = ValidTo;
      tmpCustInfo.CPI.add(tmpCustInfo.ProductCount,tmpCPI);
      tmpCustInfo.ProductCount++;
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> not found. Add CPI failed.");
    }
  }

 /**
  * Add an ERA (Extended Rating Attribute) object to the account. ERAs are
  * used to control rating, for example Closed User Groups are modelled using
  * these.
  *
  * @param CustId The customer to add the ERA to
  * @param ERA_ID The key of the ERA
  * @param Value The value of the ERA
  */
  public void addERA(String CustId, String ERA_ID, String Value)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (CustIDCache.containsKey(CustId))
    {
      // Create the new entry for the customer ID
      tmpCustInfo = CustIDCache.get(CustId);
      tmpCustInfo.ERAList.put(ERA_ID,Value);
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> not found. Add/modify ERA failed.");
    }
  }

 /**
  * Recover a customer ID from the cache using the alias.
  *
  * @param alias The alias to lookup
  * @return The internal customer ID
  */
  public String getCustId(String alias)
  {
    String CustPoid;

    // See if we already have ID for this customer
    if (aliasCache.containsKey(alias))
    {
      // Get the poid from the alias
      CustPoid = aliasCache.get(alias);
      return CustPoid;
    }
    else
    {
      return null;
    }
  }

 /**
  * Get the products that are attached to the customer account, using the
  * alias to locate the account
  *
  * @param alias The alias to the customer account
  * @param Service The service
  * @param CDRDate The date to retrieve the products for
  * @return The product list
  */
  public ProductList getProducts(String alias, String Service, long CDRDate)
  {
    ProductList tmpProductList;
    String CustPoid;
    CustInfo tmpCustInfo;
    CustProductInfo tmpCPI;
    boolean FirstProduct = true;

    // Prepare the result
    tmpProductList = new ProductList();

    // See if we already have ID for this customer
    if (aliasCache.containsKey(alias))
    {
      // Get the poid from the alias
      CustPoid = aliasCache.get(alias);

      // Get the product information
      tmpCustInfo = CustIDCache.get(CustPoid);

      // See if the CDR is within the period of validitysetRawProductList
      if ( tmpCustInfo.UTCValidFrom <= CDRDate )
      {
        if (tmpCustInfo.UTCValidTo > CDRDate)
        {
          // We have validity, get back the product list
          for ( int i = 0 ; i < tmpCustInfo.ProductCount ; i ++ )
          {
            tmpCPI = tmpCustInfo.CPI.get(i);
            if (tmpCPI.Service.equals(Service))
            {
              if ( tmpCPI.UTCValidFrom <= CDRDate )
              {
                if ( tmpCPI.UTCValidTo > CDRDate )
                {
                  if (FirstProduct)
                  {
                    tmpProductList.addProduct(0,tmpCPI.ProductID,null,tmpCPI.Service,tmpCPI.UTCValidFrom,tmpCPI.UTCValidTo,1);
                    FirstProduct = false;
                  }
                  else
                  {
                    tmpProductList.addProduct(0,tmpCPI.ProductID,null,tmpCPI.Service,tmpCPI.UTCValidFrom,tmpCPI.UTCValidTo,1);
                  }
                }
              }
            }
          }
          tmpProductList.setBalanceGroup(tmpCustInfo.BalanceGroup);
          return tmpProductList;
        }
      }

      return null;
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Alias <" + alias + "> not found. Lookup failed.");
    }

    return null;
  }

 /**
  * Return the value of the balance group so that we are able to update
  * it during the logic processing. This is to make sure that we have all
  * the information necessary when we start the calculation.
  *
  * @param CustId The customer ID
  * @return The balance group
  */
  public int getBalanceGroup(String CustId)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (CustIDCache.containsKey(CustId))
    {
      // Get the product information
      tmpCustInfo = CustIDCache.get(CustId);

      return tmpCustInfo.BalanceGroup;
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> not found. Lookup failed.");
    }

    return 0;
  }

 /**
  * Recover an ERA value from the customer cache.
  *
  * @param CustId The customer ID to recover the ERA for
  * @param ERA_ID The key of the ERA to recover
  * @return The ERA value
  */
  public String getERA(String CustId, String ERA_ID)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (CustIDCache.containsKey(CustId))
    {
      // Create the new entry for the customer ID
      tmpCustInfo = CustIDCache.get(CustId);
      return tmpCustInfo.ERAList.get(ERA_ID);
    }
    else
    {
      // Otherwise we don't know the customer ID, return null
      return null;
    }
  }

 /**
  * Recover an ERA value from the customer cache.
  *
  * @param CustId The customer ID to recover the ERA for
  * @return The ERA value
  */
  public List<String> getERAKeys(String CustId)
  {
    CustInfo tmpCustInfo;
    ArrayList<String> keyList = new ArrayList<>();

    // See if we already have ID for this customer
    if (CustIDCache.containsKey(CustId))
    {
      // Create the new entry for the customer ID
      tmpCustInfo = CustIDCache.get(CustId);
      keyList.addAll(tmpCustInfo.ERAList.keySet());

      return keyList;
    }
    else
    {
      // Otherwise we don't know the customer ID, return null
      return null;
    }
  }

 /**
  * load the data from a file
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromFile()
                        throws InitializationException
  {
    // Variable declarations
    int            custLoaded = 0;
    int            CPILoaded = 0;
    int            aliasLoaded = 0;
    int            ERALoaded = 0;
    int            lineCounter = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       recordFields ;
    long           tmpFromDate = 0;
    long           tmpToDate = 0;
    SimpleDateFormat sdfInput = new SimpleDateFormat (internalDateFormat);

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Customer Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      message = "Application is not able to read file <" + cacheDataFile + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,
                                        exFileNotFound,
                                        getSymbolicName());
    }

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();
        lineCounter++;

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, ignore
        }
        else
        {
            recordFields = tmpFileRecord.split(";");

            // Work on the different types of records in the file
            if ( recordFields[0].equals("01") )
            {
              // Customer data - prepare the fields
              try
              {
                tmpFromDate = sdfInput.parse(recordFields[2]).getTime()/1000;
                tmpToDate = sdfInput.parse(recordFields[3]).getTime()/1000;
              }
              catch (ParseException ex)
              {
                OpenRate.getOpenRateFrameworkLog().error("Date formats for record <" + tmpFileRecord + "> on line <" + lineCounter + "> are not correct. Data discarded." );
              }

              addCustId(recordFields[1],tmpFromDate,tmpToDate,Integer.parseInt(recordFields[4]));
              custLoaded++;

              // Update status for long operations
              if ( (custLoaded % loadingLogNotificationStep) == 0)
              {
                OpenRate.getOpenRateFrameworkLog().info("Customer Map Data Loaded " + custLoaded + " Customer Records");
              }
            }

            if (recordFields[0].equals("02"))
            {
              // Customer product - prepare the fields
              try
              {
                tmpFromDate = sdfInput.parse(recordFields[4]).getTime()/1000;
                tmpToDate = sdfInput.parse(recordFields[5]).getTime()/1000;
              }
              catch (ParseException ex)
              {
                OpenRate.getOpenRateFrameworkLog().error("Date formats for record <" + tmpFileRecord + "> are not correct. Data discarded." );
              }

              addCPI(recordFields[1],recordFields[2],recordFields[3],tmpFromDate,tmpToDate);
              CPILoaded++;

              // Update status for long operations
              if ( (CPILoaded % loadingLogNotificationStep) == 0)
              {
                OpenRate.getOpenRateFrameworkLog().info("Customer Map Data Loaded " + CPILoaded + " Product Records");
              }
            }

            if ( recordFields[0].equals("05") )
            {
              addAlias(recordFields[1],recordFields[2]);
              aliasLoaded++;
            }

            if ( recordFields[0].equals("06") )
            {
              addERA(recordFields[1],recordFields[2],recordFields[3]);
              ERALoaded++;
            }
            // Other types of record
            // ...
        }
      }
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + lineCounter + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + lineCounter + ">. Malformed Record.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + cacheDataFile +
                  ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Customer Cache Data Loading completed. " + lineCounter +
          " configuration lines loaded from <" + cacheDataFile +
          ">");
    OpenRate.getOpenRateFrameworkLog().info("Alias Loaded:    " + aliasLoaded);
    OpenRate.getOpenRateFrameworkLog().info("Customers Loaded: " + custLoaded);
    OpenRate.getOpenRateFrameworkLog().info("Products Loaded:  " + CPILoaded);
    OpenRate.getOpenRateFrameworkLog().info("ERAs Loaded:      " + ERALoaded);
  }

 /**
  * Load the data from the defined Data Source DB
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    String         ERAValue;
    String         ERAName;
    String         prodName;
    String         service;
    long           validFrom = 0;
    long           validTo = 0;
    int            balGrp;
    String         tmpBalGrp;
    String         tmpValidTo;
    String         tmpValidFrom;
    int            custLoaded = 0;
    int            CPILoaded = 0;
    int            aliasLoaded = 0;
    int            ERALoaded = 0;
    String         custId;
    String         alias;
    SimpleDateFormat sdfInput = new SimpleDateFormat (internalDateFormat);

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Customer Cache Loading from DB");

    // The datasource property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    if(DBUtil.initDataSource(cacheDataSourceName) == null)
    {
      message = "Could not initialise DB connection <" + cacheDataSourceName + "> to in module <" + getSymbolicName() + ">.";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try
    {
      mrs = stmtAliasSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Alias data. message: <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer alias cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        aliasLoaded++;
        alias  = mrs.getString(1);
        custId = mrs.getString(2);

        // Add the map
        addAlias(alias,custId);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Alias Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Result Set for Alias information from <" +
            cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Execute the query
    try
    {
      mrs = stmtCustomerSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Customer data. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer alias cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        custLoaded++;
        custId = mrs.getString(1);
        tmpValidFrom = mrs.getString(2);
        tmpValidTo = mrs.getString(3);
        tmpBalGrp = mrs.getString(4);

        // Customer data - prepare the fields
        try
        {
          validFrom = sdfInput.parse(tmpValidFrom).getTime()/1000;
          validTo   = sdfInput.parse(tmpValidTo).getTime()/1000;
        }
        catch (ParseException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Date formats for record <" + custLoaded + "> are not correct. Data discarded." );
        }

        balGrp = Integer.parseInt(tmpBalGrp);

        // Add the map
        addCustId(custId,validFrom,validTo,balGrp);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Customer Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Result Set for Customer information from <" +
            cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

     // Execute the query
    try
    {
      mrs = stmtProductSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Product data. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer alias cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        CPILoaded++;
        custId = mrs.getString(1);
        service = mrs.getString(2);
        prodName = mrs.getString(3);
        tmpValidFrom = mrs.getString(4);
        tmpValidTo = mrs.getString(5);

        // Customer data - prepare the fields
        try
        {
          validFrom = sdfInput.parse(tmpValidFrom).getTime()/1000;
          validTo   = sdfInput.parse(tmpValidTo).getTime()/1000;
        }
        catch (ParseException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Date formats for record <" + custLoaded + "> are not correct. Data discarded." );
        }

        // Add the map
        addCPI(custId,service,prodName,validFrom,validTo);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Product Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Result Set for Product information from <" +
            cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Execute the query
    try
    {
      mrs = stmtERASelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving ERA data";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer alias cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        CPILoaded++;
        custId = mrs.getString(1);
        ERAName = mrs.getString(2);
        ERAValue = mrs.getString(3);

        // Add the map
        addERA(custId,ERAName,ERAValue);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Product Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      stmtAliasSelectQuery.close();
      stmtCustomerSelectQuery.close();
      stmtProductSelectQuery.close();
      stmtERASelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Search Map Data connection for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Customer Cache Data Loading completed from <" + cacheDataSourceName +
          ">");
    OpenRate.getOpenRateFrameworkLog().info("Alias Loaded:     " + aliasLoaded);
    OpenRate.getOpenRateFrameworkLog().info("Customers Loaded: " + custLoaded);
    OpenRate.getOpenRateFrameworkLog().info("Products Loaded:  " + CPILoaded);
    OpenRate.getOpenRateFrameworkLog().info("ERAs Loaded:      " + ERALoaded);
  }

 /**
  * Load the data from the defined Data Source Method
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromMethod()
                      throws InitializationException
  {
    throw new InitializationException("Not implemented yet",getSymbolicName());
  }

 /**
  * Reset the cache
  */
  @Override
  public void clearCacheObjects()
  {
    CustIDCache.clear();
    aliasCache.clear();
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * get the select statement(s). Implemented as a separate function so that it can
  * be overwritten in implementation classes. By default the cache picks up the
  * statement with the name "SelectStatement".
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return True if the statements were found, otherwise false
  * @throws InitializationException
  */
  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName) throws InitializationException
  {
    // Get the Select statement
    aliasSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "AliasSelectStatement",
                                                                     "None");

    if (aliasSelectQuery.equalsIgnoreCase("None"))
    {
      message = "<AliasSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(message,getSymbolicName());
    }

    customerSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                        CacheName,
                                                                        "CustomerSelectStatement",
                                                                        "None");

    if (customerSelectQuery.equalsIgnoreCase("None"))
    {
      message = "<CustomerSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(message,getSymbolicName());
    }

    productSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                       CacheName,
                                                                       "ProductSelectStatement",
                                                                       "None");

    if (productSelectQuery.equalsIgnoreCase("None"))
    {
      message = "<ProductSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(message,getSymbolicName());
    }

    eraSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                   CacheName,
                                                                   "ERASelectStatement",
                                                                   "None");

    if (eraSelectQuery.equalsIgnoreCase("None"))
    {
      message = "<ERASelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(message,getSymbolicName());
    }

    // Normally we should not get here - we should have thrown an exception already
    // if anything was missing
    if ((aliasSelectQuery.equals("None")) |
        (customerSelectQuery.equals("None")) |
        (productSelectQuery.equals("None")) |
        (eraSelectQuery.equals("None")))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
   * @throws InitializationException
   */
  @Override
  protected void prepareStatements()
                          throws InitializationException
  {
    try
    {
      // prepare the SQL for the TestStatement
      stmtAliasSelectQuery = JDBCcon.prepareStatement(aliasSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + aliasSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    try
    {
      // prepare the SQL for the TestStatement
      stmtCustomerSelectQuery = JDBCcon.prepareStatement(customerSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + customerSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    try
    {
      // prepare the SQL for the TestStatement
      stmtProductSelectQuery = JDBCcon.prepareStatement(productSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + productSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    try
    {
      // prepare the SQL for the TestStatement
      stmtERASelectQuery = JDBCcon.prepareStatement(eraSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + eraSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }
  }
}
