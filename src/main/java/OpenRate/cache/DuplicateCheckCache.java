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

package OpenRate.cache;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * This is a cache for performing duplicate checks on CDRs, using a persistent
 * in-memory hash table, which must be saved on shutdown or periodically.
 *
 * The duplicate check itself is very simple: We check to see if a record with
 * the identifier already exists. If not, we add it, if so, we mark it as a duplicate
 */
public class DuplicateCheckCache
     extends AbstractCache
  implements ICacheLoader,
             IEventInterface
{
	
  // Regular expression pattern for duplicate check 
  private static final Pattern duplicateCheckPattern = Pattern.compile("(?s).*uplicate.*");  	
	
  // the only supported one is Database
  private String DataSourceType = null;

  // name of the DB connection that we will use
  private String cacheDataSourceName = null;

 /**
  * This stores all the Record IDs for CDRs which have been processed so far
  */
  protected ConcurrentHashMap<String, Long> recordList;

  /**
   * This stores all the Record IDs for CDRs which have been processed so far in
   * the current transaction
   */
  protected ConcurrentHashMap<Integer,HashMap<String, Long>> TransRecordList;

 /**
  * This stores the DB insert connection per transaction for inserts/speculative inserts
  */
  protected ConcurrentHashMap<Integer, Connection> insertConnection;

  // Purge the internal memory
  private final static String SERVICE_PURGE   = "Purge";

  // Count the objects in the memory
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

  // The buffer limit is the date of the oldest CDR we will store in memory
  private final static String SERVICE_BUFFER  = "BufferLimit";

  // The store limit is the date of the oldest CDR we will try to look for in the DB
  private final static String SERVICE_STORE   = "StoreLimit";

  // Log every n records loaded
  private final static String SERVICE_LOAD_LOG_STEP = "LoadLogStep";

  // Active service 
  private final static String SERVICE_ACTIVE  = CommonConfig.ACTIVE;
  
  // default values for BufferLimit and StoreLimit
  private static final int    DEFAULT_BUFFER_LIMIT_DAYS = 90;
  private static final int    DEFAULT_STORE_LIMIT_DAYS = 180;

  // this is used to age old duplicate data in memory
  private long bufferLimit;
  private long storeLimit;

  // Whether the check is active or not
  private boolean Active = true;
  
  /**
   * This is our connection object for changes to the DB via purge or
   */
  protected Connection JDBCcon;

  /**
   * The query that is used to insert records
   */
  protected String InsertQuery = null;

  /**
   * The query that purges old records
   */
  protected String PurgeQuery = null;

  /**
   * The query that selects existing records from the table
   */
  protected String SelectQuery = null;

  /**
   * the statement that will be used to try to purge from the DB
   */
  protected static PreparedStatement StmtPurgeQuery;

  /**
   * the statement that will be used to try to see if a record exists in the DB
   */
  protected static PreparedStatement StmtSelectQuery;

 /**
  * The number of days we buffer
  */
  protected int bufferLimitDays;

 /**
  * The number of days we store
  */
  protected int storeLimitDays;

 /**
  * the frequency with which we update the log progress messages on loading
  */
  protected long loadingLogNotificationStep = 10000;

  /**
   * The duplicate check cache is used to detect and identify duplicate records
   * based on a unique record key
  */
  public DuplicateCheckCache()
  {
    // This is the in-memory duplicate table
    recordList = new ConcurrentHashMap<>(50000);

    // This is the in-memory duplicate table for the current transaction
    TransRecordList = new ConcurrentHashMap<>(100);

    // initialise the inser connection array
    insertConnection = new ConcurrentHashMap<>(10);
  }
// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    // Variable declarations
    String            strBufferLimit,strStoreLimit;

    OpenRate.getOpenRateFrameworkLog().info("Starting Duplicate Check Cache Loading");

    setSymbolicName(CacheName);

    // Get the source of the data to load
    DataSourceType = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                           CacheName,
                                                                           "DataSourceType",
                                                                           "None");

    if (DataSourceType.equals("None"))
    {
      message = "Data source type not found for cache <" + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      OpenRate.getOpenRateFrameworkLog().debug(
            "Found Duplicate Check Data Source Type Configuration:" +
            DataSourceType);
    }

    if (DataSourceType.equalsIgnoreCase("File"))
    {
      message = "Data source type (File) not supported for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    else if(DataSourceType.equalsIgnoreCase("DB"))
    {
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                             CacheName,
                                                                             "DataSource",
                                                                             "None");

      if (cacheDataSourceName.equals("None"))
      {
        message = "Data source not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Duplicate Check Data Source Configuration:" +
              cacheDataSourceName);
      }

      SelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                             CacheName,
                                                                             "SelectStatement",
                                                                             "None");

      if (SelectQuery.equals("None"))
      {
        message = "Select statement not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Duplicate Check Select statement Configuration:" +
              InsertQuery);
      }

      InsertQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                             CacheName,
                                                                             "InsertStatement",
                                                                             "None");

      if (InsertQuery.equals("None"))
      {
        message = "Insert statement not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Duplicate Check Insert statement Configuration:" +
              InsertQuery);
      }

      PurgeQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                             CacheName,
                                                                             "PurgeStatement",
                                                                             "None");

      if (PurgeQuery.equals("None"))
      {
        message = "Purge statement not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug(
              "Purge Duplicate Check Insert statement Configuration:" +
              PurgeQuery);
      }
    }
    else
    {
      message = "Data source type not valid for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }


    // **************************** Buffer Limit *******************************
    strBufferLimit = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                               CacheName,
                                                               SERVICE_BUFFER,
                                                               "None");

    if (strBufferLimit.equalsIgnoreCase("None"))
    {
      // use default limit
      bufferLimitDays = DEFAULT_BUFFER_LIMIT_DAYS;
      OpenRate.getOpenRateFrameworkLog().info("Set default value for <" + SERVICE_BUFFER + "> to <" + new Date(bufferLimit*1000) + ">");
    }
    else
    {
      try
      {
        bufferLimitDays = Integer.parseInt(strBufferLimit);

        // calculate the buffer limit cutoff date
        bufferLimit = Calendar.getInstance().getTimeInMillis()/1000 - bufferLimitDays * 86400;

        OpenRate.getOpenRateFrameworkLog().info("Set value for <" + SERVICE_BUFFER + "> to <" + new Date(bufferLimit*1000) + ">");
      }
      catch (NumberFormatException nfe)
      {
        message = "Value given for <" + SERVICE_BUFFER + "> was not numeric for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }

    // **************************** Store Limit ********************************
    strStoreLimit = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                               CacheName,
                                                               SERVICE_STORE,
                                                               "None");

    if (strStoreLimit.equalsIgnoreCase("None"))
    {
      // use default limit
      storeLimitDays = DEFAULT_STORE_LIMIT_DAYS;
      OpenRate.getOpenRateFrameworkLog().info("Set default value for <" + SERVICE_STORE + "> to <" + new Date(storeLimit*1000) + ">");
    }
    else
    {
      try
      {
        storeLimitDays = Integer.parseInt(strStoreLimit);

        // calculate the store limit cutoff date
        storeLimit = Calendar.getInstance().getTimeInMillis()/1000 - storeLimitDays * 86400;

        OpenRate.getOpenRateFrameworkLog().info("Set value for <" + SERVICE_STORE + "> to <" + new Date(storeLimit*1000) + ">");
      }
      catch (NumberFormatException nfe)
      {
        message = "Value given for <" + SERVICE_STORE + "> was not numeric for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }

    // perform some plausibility
    if (bufferLimitDays <= 1)
    {
      message = "Value given for <" + SERVICE_BUFFER + "> was less than <1> for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // perform some plausibility
    if (storeLimitDays <= 1)
    {
      message = "Value given for <" + SERVICE_STORE + "> was less than <1> for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Get the loading step, if one is defined
    loadingLogNotificationStep = initGetLoadingStep(ResourceName, CacheName);

    // The data source property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    if(DBUtil.initDataSource(cacheDataSourceName) == null)
    {
      message = "Could not initialise DB connection <" + cacheDataSourceName + "> to in module <" + getSymbolicName() + ">.";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // load in the old data from the database
    retrieveDupChkDataFromDB();
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Check for a duplicate, and if not found add to the transaction object cache.
  * The check is done first in the main cache, and then in the transaction
  * cache.
  *
  * @param RecordKey
  * @param TimeStamp
  * @param TransactionNumber
   * @return True if the record is a duplicate, otherwise false
   * @throws ProcessingException
  */
  public boolean DuplicateCheck(String RecordKey, long TimeStamp, int TransactionNumber) throws ProcessingException
  {
    Connection tmpInsertConnection;
    PreparedStatement tmpInsertStatement = null;

    if (Active)
    {
	    if (TimeStamp > bufferLimit)
	    {
	      // look only in the HashMap
	      if (recordList.containsKey(RecordKey))
	      {
	        // found in the main cache
	        return true;
	      }
	      else
	      {
	        // Check in the current transaction cache
	        if  (TransRecordList.get(TransactionNumber).containsKey(RecordKey))
	        {
	          // found in the transaction cache
	          return true;
	        }
	        else
	        {
	          // Add the record to the transaction list
	          TransRecordList.get(TransactionNumber).put(RecordKey, TimeStamp);
	          return false;
	        }
	      }
	    }
	    else if (TimeStamp > storeLimit)
	    {
	      // the key won't be in the HashMap, we need to check directly in the database
	
	      try
	      {
	        // Get the connection
	        tmpInsertConnection = getTransactionInsertConnection(TransactionNumber);
	        tmpInsertStatement = getInsertStatement(tmpInsertConnection);
	
	        // We should not normally have to use the in-transaction insert, so we
	        // will spend the time to open the connection and close it afterwards
	        // which makes the connection management and transaction management much easier
	        try
	        {
	          tmpInsertStatement.setString(1, RecordKey);
	          Timestamp date = new Timestamp(TimeStamp*1000);
	          tmpInsertStatement.setTimestamp(2, date);
	          tmpInsertStatement.execute();
	        }
	        catch (SQLException ex)
	        {
	          // check which type of exception we got
	          message=ex.getMessage();
	          if (duplicateCheckPattern.matcher(message).matches())
	          {
	            // the unique constraint of the DB has been violated, that means the key is already there
	            return true;
	          }
	          else
	          {
	            // other SQL exception
	            message = "Error inserting into <" + cacheDataSourceName + "> for the duplicate "
	                + "check data on direct DB insert. message=<" + ex.getMessage()+">";
	            OpenRate.getOpenRateFrameworkLog().error(message);
	            throw new ProcessingException(message,ex,getSymbolicName());
	          }
	        }
	      }
	      finally
	      {
	        DBUtil.close(tmpInsertStatement);
	      }
	    }
    }

    // CDR is older than the storeLimit, don't even bother to check and treat it as non-duplicate
    return false;
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of transaction layer functions ---------------------
  // -----------------------------------------------------------------------------

  /**
  * Opens a new transaction object for storing temporary results in. Once the
  * transaction is ended it will either be merged into the main cache (commit)
  * or simply discarded (rollback)
  *
  * @param TransactionNumber The transaction number to create
  */
  public void CreateTransaction(int TransactionNumber)
  {
    TransRecordList.put(TransactionNumber, new HashMap<String,Long>(5000));
  }

 /**
  * Moves the transaction cache contents over to the main cache and
  * deletes the transaction object. We also update the DB at this point.
  *
  * @param TransactionNumber
  */
  public void CommitTransaction(int TransactionNumber)
  {
	if (Active)  
	{ 
	    // insert into the DB the items in TransRecordList as well
	    HashMap<String, Long> ThisTrxRecordList = TransRecordList.get(TransactionNumber);
	
	    if (ThisTrxRecordList == null)
	    {
	      // Something wrong, we don't expect this
	      message = "No record elements found for transaction <" + TransactionNumber + "> in module <" + getSymbolicName() + ">";
	      OpenRate.getOpenRateFrameworkLog().error(message);
	    }
	    else
	    {
	      int recordCount = ThisTrxRecordList.size();
	      int recordsInserted = 0;
	
	      message = "Inserting <" + recordCount + "> records into duplicate check table" +
	                        " in module <" + getSymbolicName() + "> for transaction <" + TransactionNumber + ">";
	      
	      if (recordCount > 0)
	      {
	        // we are going to insert something, get the connection and statement
	        Connection tmpInsertConnection = getTransactionInsertConnection(TransactionNumber);
	        PreparedStatement tmpInsertStatement = getInsertStatement(tmpInsertConnection);
	
	        try
	        {
	          // Get the keys to insert
	          Set<String> keys = ThisTrxRecordList.keySet();
	          for (String key : keys)
	          {
	            try
	            {
	              tmpInsertStatement.setString(1, key);
	              Timestamp date = new Timestamp(ThisTrxRecordList.get(key)*1000);
	              tmpInsertStatement.setTimestamp(2, date);
	              tmpInsertStatement.execute();
	              
	              // Update the count of what we have inserted
	              recordsInserted++;
	            }
	            catch (SQLException ex)
	            {
	              message=ex.getMessage();
	              if (duplicateCheckPattern.matcher(message).matches())
	              {
	                // other SQL exception
	                message = "Duplicate Error inserting into <" + cacheDataSourceName + "> for the duplicate "
	                    + "check data on transaction commit for key <" + key+"> in transaction <" + TransactionNumber + ">";
	                OpenRate.getOpenRateFrameworkLog().warning(message);
	              }
	              else
	              {
	                // other SQL exception
	                message = "Error inserting into <" + cacheDataSourceName + "> for the duplicate "
	                    + "check data on transaction commit. message=<" + ex.getMessage()+"> in transaction <" + TransactionNumber + ">";
	                OpenRate.getOpenRateFrameworkLog().error(message);
	              }
	            }
	          }
	        }
	        finally
	        {
	          // Close the statement
	          DBUtil.close(tmpInsertStatement);
	        }
	
	        recordList.putAll(TransRecordList.get(TransactionNumber));
	      }
	
	
	      // and close the connection now that we have finished with it
	      closeTransactionInsertConnection(TransactionNumber);
	
	      // remove the transaction
	      TransRecordList.remove(TransactionNumber);
	
	      // Log what we did
	      message = "Inserted <" + recordsInserted + "> records into duplicate check table" +
	                        " in module <" + getSymbolicName() + "> for transaction <" + TransactionNumber + ">";
	      OpenRate.getOpenRateFrameworkLog().info(message);
	    }
	} else
    {
	   String message = "Duplicate check is disabled. No records were put into duplicate check table" + 
       " in module <" + getSymbolicName() + "> for transaction <" + TransactionNumber + ">";
	   OpenRate.getOpenRateFrameworkLog().info(message);
	}
  }

 /**
  * Deletes the transaction object without storing the data
  *
  * @param TransactionNumber
  */
  public void RollbackTransaction(int TransactionNumber)
  {
    // We just discard the keys from the transaction
    TransRecordList.remove(TransactionNumber);
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
    //Register this Client
    ClientManager.getClientManager().registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PURGE, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BUFFER, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STORE, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ACTIVE, ClientManager.PARAM_DYNAMIC_SYNC);
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
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_PURGE))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // perform the task
        purgeCache();

        ResultCode = 0;
      }
    }
    else if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      // Return the number of objects in the duplicate cache
      return Integer.toString(recordList.size());
    }
    else if (Command.equalsIgnoreCase(SERVICE_BUFFER))
    {
      // no input, return the current parameter
      return new Date(bufferLimit*1000).toString();
    }
    else if (Command.equalsIgnoreCase(SERVICE_STORE))
    {
      // no input, return the current parameter
      return new Date(storeLimit*1000).toString();
    } 
    else if (Command.equalsIgnoreCase(SERVICE_ACTIVE))
    {
	    if (Parameter.equals(""))
	    {
	      return Boolean.toString(Active);
	    }
	    else
	    {
	      if (Parameter.equalsIgnoreCase("true"))
	      {
	        Active = true;
	        ResultCode = 0;
	      }
	      else if (Parameter.equalsIgnoreCase("false"))
	      {
	        Active = false;
	        ResultCode = 0;
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
      return "Command Not Understood \n";
    }
  }

  /**
  * Purge the data in the duplicate list (no actual saving is performed, just
  * working on the in-memory data.
  */
  public void purgeCache()
  {
    purgeDupChkData();
  }


 /**
  * Recover the duplicate check data from database storage. This will filter the
  * data to remove any items that are older than the buffer date. This is run
  * on framework startup
  *
  * @throws InitializationException
  */
  public void retrieveDupChkDataFromDB() throws InitializationException
  {
    int               ColumnCount;
    int               recordsLoaded = 0;
    int               recordsDiscarded = 0;
    int               RecordsProcessed = 0;
    ResultSetMetaData Rsmd;
    ResultSet         mrs;
    long              CDRDate;
    String            CDRKey;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Duplicate Check Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS for lookup
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareSelectStatement();

    // get our cutoff date
    OpenRate.getOpenRateFrameworkLog().info("Duplicate check retrieve cutoff date is <" + new Date(bufferLimit) + ">");

    // Execute the query
    Timestamp storeLimitTimestamp = new Timestamp(storeLimit*1000);
    try
    {
      StmtSelectQuery.setTimestamp(1, storeLimitTimestamp);
      mrs = StmtSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Duplicate Check data in module <"+getSymbolicName()+">. message <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the duplicate check cache
    try
    {
      Rsmd = mrs.getMetaData();
      ColumnCount = Rsmd.getColumnCount();

      if (ColumnCount != 2)
      {
        // we're not going to be able to use this
        message = "You must define 2 entries in the record, you have defined  <" +
              ColumnCount + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // Start the loading
      mrs.beforeFirst();
      while (mrs.next())
      {
        CDRKey = mrs.getString(1);
        CDRDate = mrs.getDate(2).getTime() / 1000;

        // Overall counter for logging
        RecordsProcessed++;

        if (CDRDate > bufferLimit)
        {
          recordList.put(CDRKey, CDRDate);
          recordsLoaded++;
        }
        else
        {
          recordsDiscarded++;
        }

        // Update to the log file
        if ((RecordsProcessed % loadingLogNotificationStep) == 0)
        {
          message = "Duplicate Check Data Loading: <" + recordsLoaded +
                "> records buffered and <" + recordsDiscarded + "> records in duplicate data table for <" +
                getSymbolicName() + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Data for <" + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    DBUtil.close(mrs);
    DBUtil.close(StmtSelectQuery);
    DBUtil.close(JDBCcon);

    message = "Duplicate Check Data Loading completed. <" + recordsLoaded +
          "> records buffered and <" + recordsDiscarded +
          "> records in duplicate data table for <" + getSymbolicName() + ">";
    OpenRate.getOpenRateFrameworkLog().info(message);

  }

 /**
  * Purge the duplicate check data removing records that are older than the
  * cutoff date. After the cache has been running for some time, it will
  * accumulate records that are older than the store limit and the buffer limit,
  * because time is always moving forward, but the limits are managed as fixed
  * elements in the cache. This means that regular maintenance of the cache
  * is necessary. The easiest way to do this is to just restart the pipe, but
  * if you don't want to do that, this method trims the internal memory and
  * database, and then updates the limit to reflect the current value after time
  * moves on. We do this by running through the whole key set database and
  * purging the records which are no longer valid. Once we have done this
  * we perform a purge on the database. The two actions are not really connected
  * but we package them as a single operation for convenience.
  */
  public void purgeDupChkData()
  {
    Long recordDate;
    int recordsPurgedMemory = 0;
    int recordsPurgedDatabase = 0;

    // log the cutoff date
    OpenRate.getOpenRateFrameworkLog().info("Duplicate check purge started. Original cache size = <" + recordList.size() + "> records.");

    // re-calculate the buffer limit cutoff date
    bufferLimit = Calendar.getInstance().getTimeInMillis()/1000 - bufferLimitDays * 86400;

    // re-calculate the store limit cutoff date
    storeLimit = Calendar.getInstance().getTimeInMillis()/1000 - storeLimitDays * 86400;

    try
    {
      // **** Clean up the memory ****
      // Create a new HashMap that will replace the current one. We cannot simply
      // remove the items from the current one due to the ConcurrentModificationException
      ConcurrentHashMap<String, Long> NewRecordList = new ConcurrentHashMap<>(50000);

      // Dump the contents of the current hashmap
      Set<String> keySet = recordList.keySet();

      // loop through the keys and add to the new hashmap only the ones newer than cutoff
      for (String dupKey : keySet)
      {
        recordDate = recordList.get(dupKey);

        if (recordDate < bufferLimit)
        {
          // this will not be stored in the new hashmap
          recordsPurgedMemory++;
        }
        else
        {
          NewRecordList.put(dupKey,recordDate);
        }
      }

      // Swap the existing and new record list over
      recordList = NewRecordList;

      // log that we have moved onto the DB part
      OpenRate.getOpenRateFrameworkLog().info("Duplicate check DB purge started.");

      // **** Clean up the database ****
      // get the connection for DB Modification
      try
      {
        JDBCcon = DBUtil.getConnection(cacheDataSourceName);
      }
      catch (InitializationException ex)
      {
        getHandler().reportException(ex);
      }

      // Now prepare the purge statement
      try
      {
        preparePurgeStatement();
      }
      catch (InitializationException ie)
      {
        message = "Error preparing purge statement for Duplicate Check data purge in module <"+getSymbolicName()+">. message <" + ie.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
      }

      // purge the database
      Timestamp date = new Timestamp(storeLimit*1000);
      try
      {
        StmtPurgeQuery.setTimestamp(1, date);
        recordsPurgedDatabase = StmtPurgeQuery.executeUpdate();
      }
      catch (SQLException ex)
      {
        message = "Error purging the duplicate check database. Query: <" +
                PurgeQuery + "> message: <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
      }
    }
    finally
    {
      DBUtil.close(StmtPurgeQuery);
      DBUtil.close(JDBCcon);
    }

    // log what we have saved
    message = "Duplicate check data purging finished. Purged <" + recordsPurgedMemory + "> records "
            + "from memory, <" + recordsPurgedDatabase + "> records from database";
    OpenRate.getOpenRateFrameworkLog().info(message);
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * prepareSelectStatement creates the statement from the SQL expressions
  * so that they can be run for loading
  *
  * @throws InitializationException
  */
  protected void prepareSelectStatement() throws InitializationException
  {
    try
    {
      // prepare the SQL for the SelectStatement
      StmtSelectQuery = JDBCcon.prepareStatement(SelectQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + SelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (Exception ex)
    {
      message = "Error preparing the statement <" + SelectQuery +
                       ">. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * preparePurgeStatement creates the statements from the SQL expressions
  * so that they can be run to purge old records from the DB
  *
  * @throws InitializationException
  */
  protected void preparePurgeStatement() throws InitializationException
  {
    try
    {
      // prepare the SQL for the PurgeStatement
      StmtPurgeQuery = JDBCcon.prepareStatement(PurgeQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_UPDATABLE);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + PurgeQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (Exception ex)
    {
      message = "Error preparing the statement <" + PurgeQuery +
                       ">. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private int initGetLoadingStep(String ResourceName, String CacheName) throws InitializationException
  {
    String tmpValue;
    int    tmpLoadStep;

    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       SERVICE_LOAD_LOG_STEP,
                                                       "1000");

    // try to convert it
    try
    {
      tmpLoadStep = Integer.parseInt(tmpValue);
    }
    catch (NumberFormatException ex)
    {
      message = "Value provided for property <" + SERVICE_LOAD_LOG_STEP +
                "> was not numeric. Received value <" + tmpValue + ">.";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    return tmpLoadStep;
  }

 /**
  * Gets a connection for use in the insert processing module. If the connection
  * is not available, we create it.
  *
  * @param TransactionNumber The transaction number we are creating for
  * @return The created connection
  */
  public Connection getTransactionInsertConnection(int TransactionNumber)
  {
    Connection tmpConn = null;

    if (insertConnection.containsKey(TransactionNumber))
    {
      return insertConnection.get(TransactionNumber);
    }
    else
    {
      // Try to open the DS for lookup
      try
      {
        tmpConn = DBUtil.getConnection(cacheDataSourceName);
      }
      catch (InitializationException ex)
      {
        getHandler().reportException(ex);
      }

      // store it for later
      insertConnection.put(TransactionNumber, tmpConn);

      return tmpConn;
    }
  }

 /**
  * Closes the connection for use in the insert processing module. If the connection
  * is not available, we create it.
  *
  * @param TransactionNumber The transaction number we are closing for
  */
  public void closeTransactionInsertConnection(int TransactionNumber)
  {
    if (insertConnection.containsKey(TransactionNumber))
    {
      // Close the connection
      DBUtil.close(insertConnection.get(TransactionNumber));

      // Clean the hash
      insertConnection.remove(TransactionNumber);
    }
  }

 /**
  * getInsertStatement creates the statement from the SQL insert expression
  * so that it can be run for updating the database on transaction commit or
  * in processing commit.
  *
  * @param JDBCconInsert The connection to create for
  * @return The prepared insert statement
  */
  public PreparedStatement getInsertStatement(Connection JDBCconInsert)
  {
    PreparedStatement tmpStatement = null;

    try
    {
      // prepare the SQL for the InsertStatement
      tmpStatement = JDBCconInsert.prepareStatement(InsertQuery,
                                                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                    ResultSet.CONCUR_UPDATABLE);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + InsertQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
    }
    catch (Exception ex)
    {
      message = "Error preparing the statement <" + InsertQuery +
                       ">. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().error(message);
    }

    return tmpStatement;
  }
}
