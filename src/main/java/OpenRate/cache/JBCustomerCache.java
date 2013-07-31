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
package OpenRate.cache;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.lang.ProductList;
import OpenRate.logging.LogUtil;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a cache of customer information for use in simple
 * JBilling rating cases. It uses the Item Description field which is
 * parsed to extract the information out of the jbilling database.
 *
 * The data should be recovered from the database like this:
 *  - OrderId     (integer)
 *  - CustId      (integer)
 *  - OrderLineId (integer)
 *  - Description (string)
 *  - StartDate   (yyyy-MM-dd HH:mm:ss.S)
 *  - EndDate     (yyyy-MM-dd HH:mm:ss.S)
 *  - Quantity    (integer)
 *
 * The "invalidate on duplicate" configuration will remove a whole alias key
 * from the cache in the case that a duplicate is found. This will mean that all
 * rating for the alias key will be rejected until the contention is removed.
 */
public abstract class JBCustomerCache
    extends AbstractSyncLoaderCache
{
  // Used to allow alias maps - takes an alias and maps to a poid.
  private ConcurrentHashMap<String, validityNode> aliasCache;

  // The CustIDCache holds the aliases for the account
  private ConcurrentHashMap<Integer, CustInfo> custIDCache;

  // Conversion cache
  private ConversionUtils conv = new ConversionUtils();
  /**
   * these are the statements that we have to prepare to be able to get records
   * once and only once
   */
  protected String CustomerDataSelectQuery;

  /**
   * these are the prepared statements
   */
  protected PreparedStatement StmtCustomerDataSelectQuery;

  /**
   * The internal date format is used to translate the dates from the
   * database into internal readable date formats
   */
  protected String internalDateFormat = "yyyy-MM-dd HH:mm:ss.S";

  // Setting if we invalidate existing value if a duplicate is found
  private boolean invalidateOnDuplicate = false;

  // List of Services that this Client supports
  private final static String SERVICE_INVALIDATE_DUPLICATE = "InvalidateDuplicates";
  private final static String SERVICE_DATE_FORMAT = "DateFormat";

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
    private     int productCount = 0;
    private     int balanceGroup = 0;
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
    private String Subscription=null;
    private   long UTCValidFrom;
    private   long UTCValidTo;
    private    int OrderId;
    private    int OrderLineId;
    private    int Quantity;
  }

  /**
   * A ValidityNode is a segment of validity of a resource. These are chained
   * together in a sorted linked list. The sorting is done at insertion time
   * into the list, meaning that lookups at run time can be optimised.
   */
  private class validityNode
  {
    long         ID;         // Required for managing updates
    long         TimeFrom;
    long         TimeTo;
    Integer      Result = 0;
    String       SubId = "";
    validityNode child = null;
  }

 /** Constructor
  * Creates a new instance of the Customer Cache. The Cache
  * contains all of the Customer IDs that have been cached.
  */
  public JBCustomerCache()
  {
    super();

    custIDCache = new ConcurrentHashMap<>(5000);
    aliasCache = new ConcurrentHashMap<>(5000);
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Recover a customer ID from the cache using the alias.
  *
  * @param custID The custID to lookup
  * @return True if we know the customer ID
  */
  public boolean getCustIdExists(int custID)
  {
    return custIDCache.containsKey(custID);
  }

 /**
  * Recover a customer ID from the cache using the alias.
  *
  * @param alias The alias to lookup
  * @param CDRDate The date to get the alias for
  * @return The internal customer ID
  */
  public Integer getCustId(String alias, long CDRDate)
  {
    Integer custID = null;
    validityNode tmpValidityNode;

    // See if we already have AuditSegID for this customer
    if (aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.TimeFrom <= CDRDate) &
            (tmpValidityNode.TimeTo > CDRDate))
        {
          custID = tmpValidityNode.Result;
          break;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    // return the id
    return custID;
  }

 /**
  * Get the products that are attached to the customer account, using the
  * alias to locate the account
  *
  * @param alias The alias to the customer account
  * @param service The service
  * @param subscription The subscription to get products for
  * @param CDRDate The date to retrieve the products for
  * @return The product list
  */
  public ProductList getProducts(String alias, String service, String subscription,long CDRDate)
  {
    int custID;

    // See if we already have ID for this customer
    if (aliasCache.containsKey(alias))
    {
      // Get the poid from the Alias
      custID = getCustId(alias, CDRDate);

      // Get the products
      return getProducts(custID, service, subscription, CDRDate);
    }
    else
    {
      // We could find no alias
      OpenRate.getOpenRateFrameworkLog().error("Alias <" + alias + "> not found. Lookup failed.");

      // Just return null
      return null;
    }
  }

 /**
  * Get the products that are attached to the customer account, using the
  * alias to locate the account
  *
  * @param custID The customer ID we want to retireve for
  * @param service The service
  * @param subscription The subscription to get products for
  * @param CDRDate The date to retrieve the products for
  * @return The product list
  */
  public ProductList getProducts(int custID, String service, String subscription,long CDRDate)
  {
    ProductList tmpProductList;
    CustInfo tmpCustInfo;
    CustProductInfo tmpCPI;
    boolean firstProduct = true;

    // Prepare the result
    tmpProductList = new ProductList();

    // Get the product information
    tmpCustInfo = custIDCache.get(custID);

    // See if the CDR is within the period of validity
    if (tmpCustInfo != null)
    {
      if ( tmpCustInfo.UTCValidFrom <= CDRDate )
      {
        if (tmpCustInfo.UTCValidTo > CDRDate)
        {
          // We have validity, get back the product list

          for ( int i = 0 ; i < tmpCustInfo.productCount ; i ++ )
          {
            tmpCPI = tmpCustInfo.CPI.get(i);
            if ((tmpCPI.Service.equals(service)) & (tmpCPI.Subscription.equals(subscription)))
            {
              if ( tmpCPI.UTCValidFrom <= CDRDate )
              {
                if ( tmpCPI.UTCValidTo > CDRDate )
                {
                  if (firstProduct)
                  {
                    tmpProductList.addProduct(0,tmpCPI.ProductID,null,tmpCPI.Service,tmpCPI.UTCValidFrom,tmpCPI.UTCValidTo,tmpCPI.Quantity);
                    firstProduct = false;
                  }
                  else
                  {
                    tmpProductList.addProduct(0,tmpCPI.ProductID,null,tmpCPI.Service,tmpCPI.UTCValidFrom,tmpCPI.UTCValidTo,tmpCPI.Quantity);
                  }
                }
              }
            }
          }
        }
        tmpProductList.setBalanceGroup(tmpCustInfo.balanceGroup);
        return tmpProductList;
      }
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
  public int getBalanceGroup(int CustId)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (custIDCache.containsKey(CustId))
    {
      // Get the product information
      tmpCustInfo = custIDCache.get(CustId);

      return tmpCustInfo.balanceGroup;
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + CustId + "> not found. Lookup failed.");
    }

    return 0;
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of data loading functions ------------------------
  // -----------------------------------------------------------------------------

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    String tmpHelper;

    // load the value of the invalidate duplicates setting
    tmpHelper = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                     CacheName,
                                                     SERVICE_INVALIDATE_DUPLICATE,
                                                     "False");

    // process it
    processControlEvent(SERVICE_INVALIDATE_DUPLICATE,true,tmpHelper);


    // load the value of the invalidate duplicates setting
    tmpHelper = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                     CacheName,
                                                     SERVICE_DATE_FORMAT,
                                                     internalDateFormat);

    // Initialise the conversion object
    conv.setInputDateFormat(tmpHelper);

    // Do the parent processing
    super.loadCache(ResourceName, CacheName);
  }

 /**
  * load the data from a file - not supported for this cache.
  */
  @Override
  public void loadDataFromFile()
                        throws InitializationException
  {
    throw new InitializationException("File loading not supported",getSymbolicName());
  }

 /**
  * Load the data from the defined Data Source DB
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int            custId;
    String         orderId;
    String         orderLineId;
    String         alias;
    String         description;
    String         prodName;
    String         service;
    String         subscription;
    int            custLoaded = 0;
    String         tmpStartDate;
    String         tmpEndDate;
    long           startDate = 0;
    long           endDate = 0;
    String         tmpQuantity;
    int            quantity;

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Customer Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try
    {
      mrs = StmtCustomerDataSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Customer data. message <" + ex.getMessage() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer alias cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        custLoaded++;
        orderId = mrs.getString(1);
        custId = mrs.getInt(2);
        orderLineId = mrs.getString(3);
        description = mrs.getString(4);
        tmpStartDate = mrs.getString(5);
        tmpEndDate = mrs.getString(6);
        tmpQuantity = mrs.getString(7);

        // parse the description
        prodName = getProduct(description);
        alias = getAlias(description);
        service = getService(description);
        subscription = getSubscription(description);

        if ((prodName == null) | (alias == null) | (service == null) | (subscription == null))
        {
          OpenRate.getOpenRateFrameworkLog().warning("Record <" + description + "> skipped for customer <" + custId + "> order <" + orderId + ">." );
          continue;
        }

        // parse the start date
        try
        {
          if (tmpStartDate == null)
          {
            startDate = CommonConfig.LOW_DATE;
          }
          else
          {
            startDate = conv.convertInputDateToUTC(tmpStartDate);
          }
        }
        catch (ParseException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Start Date format for record <" + custLoaded + "> are not correct. Date <" + tmpStartDate + ">, format <" + conv.getInputDateFormat() + "> order <" + orderId + ">. Data discarded." );
        }

        // parse the end date
        try
        {
          if (tmpEndDate == null)
          {
            endDate = CommonConfig.HIGH_DATE;
          }
          else
          {
            endDate  = conv.convertInputDateToUTC(tmpEndDate);
          }
        }
        catch (ParseException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("End Date format for record <" + custLoaded + "> are not correct. Date <" + tmpEndDate + ">, format <" + conv.getInputDateFormat() + "> order <" + orderId + ">. Data discarded." );
        }

        // parse the Quantity
        quantity = Integer.parseInt(tmpQuantity);

        // print the information to the log
        OpenRate.getOpenRateFrameworkLog().info("Adding service ID <" + alias + "> to account <" + custId + "> with product <" + prodName + "> validity <" + tmpStartDate + " (" + startDate + ") - " + tmpEndDate + " (" + endDate + ")>, Qty: <" + quantity + "> order <" + orderId + ">.");

        // Add the map
        addAlias(alias,custId,startDate,endDate);
        addCustId(custId,CommonConfig.LOW_DATE,CommonConfig.HIGH_DATE,0);
        addCPI(custId, service, subscription, prodName, startDate, endDate, Integer.parseInt(orderId), Integer.parseInt(orderLineId),quantity);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Customer Data for <" + cacheDataSourceName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtCustomerDataSelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Result Set for Customer information from <" +
            cacheDataSourceName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Customer Cache Data Loading completed from <" + cacheDataSourceName +
          ">");

    OpenRate.getOpenRateFrameworkLog().info("Products Loaded:  " + custLoaded);
  }

 /**
  * Load the data from the defined Data Source Method
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
    custIDCache.clear();
    aliasCache.clear();
  }

 /**
  * Add an alias to the customer cache. An alias is a representation of any
  * identifier that can be used to locate the account. Note that we exclude
  * the last second of the validity period to avoid issues with overlapping
  * periods that end and start on the same second (e.g. old validity ends on
  * 1st Sept @ 00:00 and the new one starts on 1st Sept @ 00:00)
  *
  * @param alias The identifier that should be used to locate the account
  * @param custId The account that should be located
  * @param validFrom The start date of the validity
  * @param validTo The end date of the validity
  */
  public void addAlias(String alias, int custId,long validFrom,long validTo)
  {
    validityNode NewNode;
    validityNode tmpValidityNode;
    validityNode tmpValidityNextNode;
    boolean      insertedEntry = false;

    // Check that the valid to is after the valid from
    if (validFrom > validTo)
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Alias ID <" + alias + "> validity period from <" + validFrom + "> is after validity period to <" + validTo + ">. Ignoring.");

      return;
    }

    // Now add the validity segment into the ArrayList
    if (!aliasCache.containsKey(alias))
    {
      // We do not know this alias - Create the new ArrayList
      tmpValidityNode = new validityNode();
      //tmpValidityNode.ID = ID;
      tmpValidityNode.TimeFrom = validFrom;
      tmpValidityNode.TimeTo = validTo - 1; // Exlude the last second
      tmpValidityNode.Result = custId;
      tmpValidityNode.child = null;
      //NewNode.SubId = subID;

      // Add in the new node
      aliasCache.put(alias, tmpValidityNode);

      // mark that we have done the work
      insertedEntry = true;
    }
    else
    {
      // Recover the validity map that there is
      tmpValidityNode = aliasCache.get(alias);

      // now run down the validity periods until we find the right position
      while (tmpValidityNode != null)
      {
        tmpValidityNextNode = tmpValidityNode.child;

        // Search for the place that will accommodate the start of the segment
        if (validFrom > tmpValidityNode.TimeTo)
        {
          if (tmpValidityNextNode == null)
          {
            // Insert at the end of the list
            NewNode = new validityNode();
            //tmpValidityNode.ID = ID;
            tmpValidityNode.child = NewNode;
            NewNode.TimeFrom = validFrom;
            NewNode.TimeTo = validTo - 1; // Exclude the last second
            NewNode.Result = custId;
            //NewNode.SubId = subID;

            // mark that we have done the work
            insertedEntry = true;
          }
          else
          {
            if (validTo < tmpValidityNextNode.TimeFrom)
            {
              // insert into the middle of the list
              NewNode = new validityNode();
              NewNode.child = tmpValidityNode.child;
              tmpValidityNode.child = NewNode;
              //tmpValidityNode.ID = ID;
              NewNode.TimeFrom = validFrom;
              NewNode.TimeTo = validTo;
              NewNode.Result = custId;
              //NewNode.SubId = subID;

              // mark that we have done the work
              insertedEntry = true;
            }
          }
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    // see if we inserted correctly
    if (!insertedEntry)
    {
      if (invalidateOnDuplicate)
      {
        // remove the whole key that we couldn't add.
        aliasCache.remove(alias);
        OpenRate.getOpenRateFrameworkLog().error("Alias ID <" + alias + "> already exists for time <" + validFrom + "-" + validTo + ">. Removed key.");
      }
      else
      {
        // Otherwise write an error and ignore it
        OpenRate.getOpenRateFrameworkLog().error("Alias ID <" + alias + "> already exists for time <" + validFrom + "-" + validTo + ">");
      }
    }
  }

 /**
  * Add a Customer object into the CustomerCache. This is allowed to fail in the
  * case that we find a duplicate. It is important that we have the customer,
  * but it does not matter that we have it twice.
  *
  * @param custId The customer identifier
  * @param validFrom Valid from date of the customer relationship
  * @param validTo Valid to date of the customer relationship
  * @param balanceGroup The ID of the counter balance group
  */
  public void addCustId(int custId,long validFrom,long validTo,int balanceGroup)
  {
    CustInfo tmpCustInfo;

    // See if we already have ID for this customer
    if (!custIDCache.containsKey(custId))
    {
      // Create the new entry for the customer ID
      tmpCustInfo = new CustInfo();
      tmpCustInfo.CPI = new ArrayList<>();
      tmpCustInfo.UTCValidFrom = validFrom;
      tmpCustInfo.UTCValidTo   = validTo;
      tmpCustInfo.balanceGroup = balanceGroup;
      custIDCache.put(custId,tmpCustInfo);
    }
  }

 /**
  * Add a CPI (CustomerProductInstance) value into the CustomerCache
  *
  * @param custId The customer ID to add the product to
  * @param service The service of the product
  * @param subscription The subscription ID
  * @param prodID The product identifier
  * @param validFrom The start of the product validity
  * @param validTo The end of the product validity
  * @param orderId The JBilling order ID
  * @param orderLineId The JBilling order line ID
  * @param quantity The order line quantity
  */
  public void addCPI(int custId, String service, String subscription, String prodID, long validFrom, long validTo, int orderId, int orderLineId, int quantity)
  {
    CustInfo tmpCustInfo;
    CustProductInfo tmpCPI;

    // See if we already have ID for this customer
    if (custIDCache.containsKey(custId))
    {
      // Create the new entry for the customer ID
      tmpCustInfo = custIDCache.get(custId);
      tmpCPI = new CustProductInfo();
      tmpCPI.Service = service;
      tmpCPI.Subscription = subscription;
      tmpCPI.ProductID = prodID;
      tmpCPI.UTCValidFrom = validFrom;
      tmpCPI.UTCValidTo = validTo - 1; // exclude the last second
      tmpCPI.OrderId = orderId;
      tmpCPI.OrderLineId = orderLineId;
      tmpCPI.Quantity = quantity;
      tmpCustInfo.CPI.add(tmpCustInfo.productCount,tmpCPI);
      tmpCustInfo.productCount++;
    }
    else
    {
      // Otherwise write an error and ignore it
      OpenRate.getOpenRateFrameworkLog().error("Customer ID <" + custId + "> not found. Add CPI failed.");
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of abstract implementation functions -----------------
  // -----------------------------------------------------------------------------

 /**
  * Get the Product Name from the descriptor string
  *
  * @param descriptionString The string to parse
  * @return The product name
  * @throws InitializationException
  */
  public abstract String getProduct(String descriptionString) throws InitializationException;

 /**
  * Get the Alias from the descriptor string
  *
  * @param descriptionString
  * @return The alias
  * @throws InitializationException
  */
  public abstract String getAlias(String descriptionString) throws InitializationException;

 /**
  * Get the Service from the descriptor string
  *
  * @param descriptionString
  * @return The Service
  * @throws InitializationException
  */
  public abstract String getService(String descriptionString) throws InitializationException;

 /**
  * Get the Subscription from the descriptor string
  *
  * @param descriptionString
  * @return The Subscription
  * @throws InitializationException
  */
  public abstract String getSubscription(String descriptionString) throws InitializationException;

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName) throws InitializationException
  {
    CustomerDataSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                        CacheName,
                                                                        "CustomerSelectStatement",
                                                                        "None");
    if (CustomerDataSelectQuery.equals("None"))
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
  */
  @Override
  protected void prepareStatements()
                          throws InitializationException
  {
    try
    {
      // prepare the SQL for the TestStatement
      StmtCustomerDataSelectQuery = JDBCcon.prepareStatement(CustomerDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement <" + CustomerDataSelectQuery + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_INVALIDATE_DUPLICATE, ClientManager.PARAM_NONE);
  }

 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int         ResultCode = -1;

    // Set the status of the invalidate duplicates
    if (Command.equalsIgnoreCase(SERVICE_INVALIDATE_DUPLICATE))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // Set the value
        invalidateOnDuplicate = true;

        // done
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        // Set the value
        invalidateOnDuplicate = false;

        // done
        ResultCode = 0;
      }
      else if (Parameter.equals(""))
      {
        // return the current state
        if (invalidateOnDuplicate)
        {
          return "true";
        }
        else
        {
          return "false";
        }
      }
    }

    if (ResultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command,Init,Parameter);
    }
  }
}
