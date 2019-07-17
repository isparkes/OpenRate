

package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.lang.BalanceGroup;
import OpenRate.lang.Counter;
import OpenRate.lang.CounterGroup;
import OpenRate.logging.LogUtil;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * All balances start with a value of zero. CDRs increment the balances
 * which are then used to calculate bonuses. This is a relatively simple
 * module for reading and writing static values.
 *
 * This cache is designed as a read/write cache, but does not at the moment
 * manage transactions. That means if a transaction fails (very unusual)
 * either the balances will have to be cleaned up, or reloaded from the table.
 *
 * The data should be presented in the form:
 *   BalanceGroupId - the identifier of the balance group (integer)
 *   CounterId      - the counter identifier (integer)
 *   RecordId       - the counter instance identifier (integer)
 *   Valid From     - the start date of the instance (UTC date as long)
 *   Valid To       - the end date of the instance (UTC date as long)
 *   Current Balance- the current value of the counter (double)
 *
 * @author i.sparkes
 */
public class BalanceCache extends AbstractCache
                       implements ICacheLoader,
                                  ICacheSaver,
                                  IEventInterface
{
  // This is the source type of the data to load
  private String cacheDataSourceType = null;

  /**
   * This is the location of the file to load (or reload)
   */
  protected String cacheDataSourceName = null;

  /**
   * This is our connection object
   */
  protected Connection JDBCcon;

  /**
   * this is the persistent result set that we use to incrementally get the
   * records
   */
  protected ResultSet mrs = null;

  // these are the statements that we have to prepare to be able to get
  // records once and only once
  private static String cacheDataSelectQuery;

  /**
   * these are the prepared statements
   */
  protected static PreparedStatement stmtCacheDataSelectQuery;

   /**
    * The internal cache is very simply a huge array. No black magic here
    */
   protected Map<Long, BalanceGroup> balanceCache;

   // used for handling date conversions
   private static ConversionUtils conv;
   
   // if we have to save a snapshot even when in DB mode
   private boolean saveSnapshot = false;

  // List of Services that this Client supports
  private final static String SERVICE_DUMP_BALGROUP = "DumpBalGroup";

 /** Constructor
  * Creates a new instance of the Balance Cache.
  */
  public BalanceCache()
  {
    // Initialise the cache hash 
    balanceCache = new ConcurrentHashMap<>(1000);

    // Initialise variables that we will be using regularly - this is the
    // default that can be overwritten using "setDateFormat"
    conv = new ConversionUtils();
    conv.setInputDateFormat("yyyyMMddHHmmss");
  }

 /**
  * Add a value into the BalanceCache. Does not check for current existence.
  * Primarily intended for loading into the cache, because the record id is
  * managed by this method.
  *
  * @param BalanceGroupId The balance group identifier
  * @param CounterId The ID of the counter in the balance group
  * @param RecId The ID of the counter period of the counter
  * @param ValidFrom The start of the validity period for the counter period
  * @param ValidTo The end of the validity period for the counter period
  * @param CurrentBal The current bal to assign to the counter period
  */
  protected void addCounterAutoRecId(long BalanceGroupId, int CounterId, int RecId, long ValidFrom, long ValidTo, double CurrentBal)
  {
    BalanceGroup tmpBalGrp;

    if (balanceCache.containsKey(BalanceGroupId))
    {
      // Add the balance to the existing group
      tmpBalGrp = balanceCache.get(BalanceGroupId);
      tmpBalGrp.addCounter(CounterId,RecId,ValidFrom,ValidTo,CurrentBal);
    }
    else
    {
      // Create a new group
      tmpBalGrp = new BalanceGroup();
      balanceCache.put(BalanceGroupId,tmpBalGrp);
      tmpBalGrp.addCounter(CounterId,RecId,ValidFrom,ValidTo,CurrentBal);
    }
  }

 /**
  * Add a value into the BalanceCache. Does not check for current existence.
  * Intended for use by user applications, as it lets the counter group manage
  * the record id.
  *
  * @param BalanceGroupId The balance group identifier
  * @param CounterId The ID of the counter in the balance group
  * @param ValidFrom The start of the validity period for the counter period
  * @param ValidTo The end of the validity period for the counter period
  * @param CurrentBal The current bal to assign to the counter period
  * @return The counter
  */
  public Counter addCounter(long BalanceGroupId, int CounterId, long ValidFrom, long ValidTo, double CurrentBal)
  {
    BalanceGroup tmpBalGrp;

    if (balanceCache.containsKey(BalanceGroupId))
    {
      // Add the balance to the existing group
      tmpBalGrp = balanceCache.get(BalanceGroupId);
      return tmpBalGrp.addCounter(CounterId,ValidFrom,ValidTo,CurrentBal);
    }
    else
    {
      // Create a new group
      tmpBalGrp = new BalanceGroup();
      balanceCache.put(BalanceGroupId,tmpBalGrp);
      return tmpBalGrp.addCounter(CounterId,ValidFrom,ValidTo,CurrentBal);
    }
  }

 /**
  * Check if a counter exists in the balance cache, and return it if it does
  *
  * @param BalanceGroupId The balance group id
  * @param CounterId The counter group id
  * @param UTCEventDate The date of the counter
  * @return false if it does not exist, otherwise true
  */
  public Counter checkCounterExists(long BalanceGroupId, int CounterId, long UTCEventDate)
  {
    BalanceGroup tmpBalGrp = getBalanceGroup(BalanceGroupId);
    CounterGroup tmpCounterGroup;
    Counter tmpCounter;

    // Check the balance group existence
    if (tmpBalGrp == null)
    {
      return null;
    }
    else
    {
      // check the counter group existence
      tmpCounterGroup = tmpBalGrp.getCounterGroup(CounterId);

      if (tmpCounterGroup == null)
      {
        return null;
      }
      else
      {
        // Check the counter existence
        tmpCounter = tmpCounterGroup.getCounterByUTCDate(UTCEventDate);

        if (tmpCounter == null)
        {
          return null;
        }
        else
        {
          return tmpCounter;
        }
      }
    }
  }

 /**
  * Add a new balance group into the BalanceCache.
  *
  * @param BalanceGroupId The balance group identifier
  * @return The balance group object
  */
  public BalanceGroup addBalanceGroup(long BalanceGroupId)
  {
    BalanceGroup tmpBalGrp = null;

    if (!balanceCache.containsKey(BalanceGroupId))
    {
      // Create a new group
      tmpBalGrp = new BalanceGroup();
      balanceCache.put(BalanceGroupId,tmpBalGrp);
    }

    return tmpBalGrp;
  }

 /**
  * Gets a counter from a balance group by counter id and UTC date
  *
  * @param BalanceGroupId The balance group to retrieve for
  * @param counterId The counter id to retrieve for
  * @param UTCEventDate The date to retrieve for
  * @return The counter or null
  */
  public Counter getCounter(long BalanceGroupId, int counterId, long UTCEventDate)
  {
    BalanceGroup tmpBalGrp = getBalanceGroup(BalanceGroupId);
    CounterGroup tmpCounterGroup;
    Counter tmpCounter;

    if (tmpBalGrp == null)
    {
      return null;
    }
    else
    {
      // Get the counter for euro
      tmpCounterGroup = tmpBalGrp.getCounterGroup(counterId);

      if (tmpCounterGroup == null)
      {
        return null;
      }
      else
      {
        // Find the right counter from the group - this will usually be the first
        tmpCounter = tmpCounterGroup.getCounterByUTCDate(UTCEventDate);

        if (tmpCounter == null)
        {
          return null;
        }
        else
        {
          return tmpCounter;
        }
      }
    }
  }

 /**
  * Gets a balance from the cache - performs no locking or any other concurrency
  * control.
  *
  * @param balanceGroupId The ID of the balance group to retrieve
  * @return The balance group
  */
  public BalanceGroup getBalanceGroup(long balanceGroupId)
  {
    BalanceGroup tmpBalGrp;

    if (!balanceCache.containsKey(balanceGroupId))
    {
      // create the default balance group
      // throw a wobbly?
    }

    // Return the value
    tmpBalGrp = balanceCache.get(balanceGroupId);
    return tmpBalGrp;
  }

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface.
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
    throws InitializationException
  {
	// If we found the type for the data source
    boolean foundCacheDataSourceType = false;

    // date format
    String tmpDateFormat;

    // Get the source of the data to load
    setSymbolicName(CacheName);

    // Find the location of the configuration data
    OpenRate.getOpenRateFrameworkLog().info("Starting cache loading for <" + getSymbolicName() + ">");

    cacheDataSourceType = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "DataSourceType",
                                                       "None");

    if (cacheDataSourceType.equalsIgnoreCase("File") | cacheDataSourceType.equalsIgnoreCase("DB"))
    {
      foundCacheDataSourceType = true;
    }

    if (!foundCacheDataSourceType)
    {
      message = "DataSourceType for cache <" +
                                        getSymbolicName() +
                                        "> must be File or DB, found <" +
                                        cacheDataSourceType + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // get the date format
    tmpDateFormat = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "DateFormat",
                                                       "None");

    if (tmpDateFormat.equalsIgnoreCase("None") == false)
    {
      conv.setInputDateFormat(tmpDateFormat);
    }

    // Get the configuration we are working on
    if (cacheDataSourceType.equalsIgnoreCase("File"))
    {
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                              CacheName,
                                                              "DataFile",
                                                              "None");

      if (cacheDataSourceName.equals("None"))
      {
        message = "Data source file name not found for cache <" + getSymbolicName() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Cache Data File <" + cacheDataSourceName + "> for cache <" + getSymbolicName() + ">");
      }

      loadDataFromFile();
    }

    if (cacheDataSourceType.equalsIgnoreCase("DB"))
    {
      // Get the data source name
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                              CacheName,
                                                              "DataSource",
                                                              "None");

      if (cacheDataSourceName.equals("None"))
      {
        message = "Data source DB name not found for cache <" + getSymbolicName() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Cache Data DB <" + cacheDataSourceName + "> for cache <" + getSymbolicName() + ">");
      }

      // Get the Select statement
      cacheDataSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                       CacheName,
                                                                       "SelectStatement",
                                                                       "None");

      if (cacheDataSelectQuery.equals("None"))
      {
        message = "Data source select statement not found for cache <" +
              getSymbolicName() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug(
              "Found Select Query <" + cacheDataSelectQuery + "> for cache <" +
              getSymbolicName() + ">");
      }

      // The datasource property was added to allow database to database
      // JDBC adapters to work properly using 1 configuration file.
      if(DBUtil.initDataSource(cacheDataSourceName) == null)
      {
        message = "Could not initialise DB connection <" + cacheDataSourceName + "> to in module <" + getSymbolicName() + ">.";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // See if we save snapshots on shutdown
      saveSnapshot = Boolean.valueOf(PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "SaveSnapshotToFile",
              "false"));

      loadDataFromDB();
    }
    
  }

 /**
  * Load the data from the defined file.
  *
  * @throws InitializationException
  */
  public void loadDataFromFile() throws InitializationException
  {
    // Variable declarations
    int            balsLoaded = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       balFields;

    // Loading fields
    String         tmpBalGrpId;
    String         tmpCounterId;
    String         tmpRecId;
    String         tmpValidFrom;
    String         tmpValidTo;
    String         tmpCurrentBal;
    double         currentBal;
    long           validTo = 0;
    long           validFrom = 0;
    int            recId;
    int            counterId;
    long           balanceGroupId;

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Balance Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataSourceName));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      message = "Application is not able to read file : <" +
            cacheDataSourceName + ">";
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

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, ignore
        }
        else
        {
          balFields = tmpFileRecord.split(";");

          tmpBalGrpId = balFields[0];
          tmpCounterId = balFields[1];
          tmpRecId = balFields[2];
          tmpValidFrom = balFields[3];
          tmpValidTo = balFields[4];
          tmpCurrentBal = balFields[5];
          balanceGroupId = Long.parseLong(tmpBalGrpId);
          counterId = Integer.parseInt(tmpCounterId);
          recId = Integer.parseInt(tmpRecId);

          try
          {
            validFrom = conv.convertInputDateToUTC(tmpValidFrom);
          }
          catch (ParseException ex)
          {
            message = "Error converting date in cache <" + getSymbolicName() + ">. Could not convert date <" + tmpValidFrom + "> using formatter <" + conv.getInputDateFormat() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          try
          {
            validTo = conv.convertInputDateToUTC(tmpValidTo);
          }
          catch (ParseException ex)
          {
            message = "Error converting date in cache <" + getSymbolicName() + ">. Could not convert date <" + tmpValidFrom + "> using formatter <" + conv.getInputDateFormat() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          currentBal = Double.parseDouble(tmpCurrentBal);

          addCounterAutoRecId(balanceGroupId,counterId,recId,validFrom,validTo,currentBal);
          balsLoaded++;
        }
      }
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataSourceName +
            "> in record <" + balsLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataSourceName +
            "> in record <" + balsLoaded + ">. Malformed Record.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + cacheDataSourceName +
                  ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Balance Cache Data Loading completed. " + balsLoaded +
          " configuration lines loaded from <" + cacheDataSourceName +
          ">");
  }

 /**
  * Load the data from the defined Data Source.
  *
  * @throws InitializationException
  */
  public void loadDataFromDB() throws InitializationException
  {
    long           balsLoaded = 0;

    // Loading fields
    String         tmpBalGrpId;
    String         tmpCounterId;
    String         tmpRecId;
    String         tmpValidFrom;
    String         tmpValidTo;
    String         tmpCurrentBal;
    double         CurrentBal;
    long           validTo;
    long           validFrom;
    int            recId;
    int            counterId;
    long           balanceGroupId;

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Balance Cache Loading from DB for<" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

      // Execute the query
      try
      {
        mrs = stmtCacheDataSelectQuery.executeQuery();
      }
      catch (SQLException Sex)
      {
        message = "Error performing SQL for retieving Balance data. message <" + Sex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // loop through the results for the balance cache
      try
      {
        while (mrs.next())
        {
          tmpBalGrpId = mrs.getString(1);
          tmpCounterId = mrs.getString(2);
          tmpRecId = mrs.getString(3);
          tmpValidFrom = mrs.getString(4);
          tmpValidTo = mrs.getString(5);
          tmpCurrentBal = mrs.getString(6);
          balanceGroupId = Long.parseLong(tmpBalGrpId);
          counterId = Integer.parseInt(tmpCounterId);
          recId = Integer.parseInt(tmpRecId);

          try
          {
            validFrom = conv.convertInputDateToUTC(tmpValidFrom);
          }
          catch (ParseException ex)
          {
            message = "Error converting date in cache <" + getSymbolicName() + ">. Could not convert date <" + tmpValidFrom + "> using formatter <" + conv.getInputDateFormat() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          try
          {
            validTo = conv.convertInputDateToUTC(tmpValidTo);
          }
          catch (ParseException ex)
          {
            message = "Error converting date in cache <" + getSymbolicName() + ">. Could not convert date <" + tmpValidFrom + "> using formatter <" + conv.getInputDateFormat() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          CurrentBal = Double.parseDouble(tmpCurrentBal);

          addCounterAutoRecId(balanceGroupId,counterId,recId,validFrom,validTo,CurrentBal);
          balsLoaded++;
        }
      }
      catch (SQLException ex)
      {
        message = "Error opening retreiving customer data. SQL error: " + ex.getMessage();
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,ex,getSymbolicName());
      }

      // Close down stuff
      try
      {
        mrs.close();
        stmtCacheDataSelectQuery.close();
        JDBCcon.close();
      }
      catch (SQLException ex)
      {
        message = "Error closing Result Set for Customer information from <" +
              cacheDataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,ex,getSymbolicName());
      }

      OpenRate.getOpenRateFrameworkLog().info("Balance Loading completed. " + balsLoaded +
        " lines loaded from <" + cacheDataSourceName + ">");
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
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.getClientManager().registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DUMP_BALGROUP, ClientManager.PARAM_DYNAMIC);
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
    int    ResultCode = -1;
    String filename;

    if (Command.equalsIgnoreCase(SERVICE_DUMP_BALGROUP))
    {
      switch (Parameter) {
        case "":
          // do nothing
          break;
        case "All":
          // calculate the file name
          filename = "Dump-" + getSymbolicName() + "-All-" + Calendar.getInstance().getTimeInMillis() + ".dump";
          dumpBalanceGroups(filename);
          return "Dumped all balance groups to file <" + filename + ">";
        default:
          long tmpBalGroup;

          // See if the Parameter is numeric
          try
          {
            tmpBalGroup = Long.valueOf(Parameter);
          }
          catch (NumberFormatException nfe)
          {
            return "Balance group parameter was not numeric <" + Parameter + ">";
          }

          // try to get the balance group
          if (balanceCache.containsKey(tmpBalGroup) == false)
          {
            return "Could not locate balance group <" + tmpBalGroup + ">";
          }
          else
          {
            // calculate the file name
            filename = "Dump-" + getSymbolicName() + "-BalGroup-" + tmpBalGroup + "-" + Calendar.getInstance().getTimeInMillis() + ".dump";
            dumpBalanceGroup(filename,tmpBalGroup);
            return "Dumped balance group <" + tmpBalGroup + "> to file <" + filename + ">";
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
      return "Command Not Understood";
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

  /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
   *
   * @throws InitializationException
   */
  protected void prepareStatements()
           throws InitializationException
  {
    try
    {
      // prepare the SQL for the TestStatement
      stmtCacheDataSelectQuery = JDBCcon.prepareStatement(cacheDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + cacheDataSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * This saves the cache back to the location on framework shutdown. Note that
  * this information is not used on startup again, but serves only as a snapshot
  * of the status that there was at the given time.
  * 
  * For the balance cache, we save a snapshot of the balances, 
  */
  @Override
  public void saveCache()
  {
    BufferedWriter     outFile;
    Long               tmpBalGrpKey;
    Integer            tmpCounterId;
    BalanceGroup       tmpBalGrp;
    Iterator<Long>     balGrpIter;
    Iterator<Integer>  counterIter;
    String             stringToWrite;
    CounterGroup       tmpCounterGroup;
    ArrayList<Counter> counters;
    int i;
    Counter tmpCounter;
    String fileName = null;

    if (cacheDataSourceType.equalsIgnoreCase("File")) {
      // Use the defined name
      fileName = cacheDataSourceName;
    } else if (cacheDataSourceType.equalsIgnoreCase("DB") && saveSnapshot) {
      // Set the timestamp
      long timestamp = ConversionUtils.getConversionUtilsObject().getCurrentUTCms();

      fileName = "BalCache_" + getSymbolicName() + "_" + timestamp + ".dump";
    }
      
    if (fileName != null) {
      // Log that we are starting the saving
      OpenRate.getOpenRateFrameworkLog().info("Starting Balance Cache Saving to File: " + fileName);

      // Try to open the file
      try
      {
        outFile = new BufferedWriter(new FileWriter(fileName));
        outFile.write("# Balance data storage file");
        outFile.newLine();
        outFile.newLine();

        // get a list of all the balance groups
        balGrpIter = balanceCache.keySet().iterator();

        while (balGrpIter.hasNext())
        {
          tmpBalGrpKey = balGrpIter.next();
          tmpBalGrp = balanceCache.get(tmpBalGrpKey);

          // Get a list of all the counter groups in the balance group
          counterIter = tmpBalGrp.getCounterIterator();

          while (counterIter.hasNext())
          {
            // get a list of all the counters in the group
            tmpCounterId = counterIter.next();
            tmpCounterGroup = tmpBalGrp.getCounterGroup(tmpCounterId);
            counters = tmpCounterGroup.getCounters();

            for (i = 0 ; i < counters.size() ; i++)
            {
              tmpCounter = counters.get(i);
              stringToWrite = tmpBalGrpKey + ";" + 
                              tmpCounterId + ";" +
                              tmpCounter.RecId + ";" + 
                              tmpCounter.validFrom +  ";" +
                              tmpCounter.validTo + ";" + 
                              tmpCounter.CurrentBalance;
              outFile.write(stringToWrite);
              outFile.newLine();
            }
          }
        }
        outFile.flush();
        outFile.close();
      }
      catch (IOException IOex)
      {
        OpenRate.getOpenRateFrameworkLog().error(
              "Application is not able to write file : <" +
              cacheDataSourceName + ">");
      }

      // Log that we have finished the saving
      OpenRate.getOpenRateFrameworkLog().info("Finished Balance Cache Saving to File");
    }
  }

// -----------------------------------------------------------------------------
// -------------------- Start of local utility functions -----------------------
// -----------------------------------------------------------------------------

 /**
  * Dumps a balance group to file.
  *
  * @param filename The file to dump to
  * @param tmpBalGroup The balance group we are dumping
  */
  private void dumpBalanceGroup(String filename, long tmpBalGroup)
  {
    OpenRate.getOpenRateFrameworkLog().info("Dumping balance group <" + tmpBalGroup + "> to file <" + filename + ">");

    try
    {
      try (BufferedWriter outFile = new BufferedWriter(new FileWriter(filename)))
      {
        outFile.write("# Balance data dump file\n");

        BalanceGroup balanceGroup = balanceCache.get(tmpBalGroup);

        Iterator<Integer> counterGrpIter = balanceGroup.getCounterIterator();
        while (counterGrpIter.hasNext())
        {
          int counterId = counterGrpIter.next();
          CounterGroup counter = balanceGroup.getCounterGroup(counterId);
          ArrayList<Counter> counterList = counter.getCounters();
          Iterator<Counter> counterIter = counterList.iterator();

          while (counterIter.hasNext())
          {
            Counter tmpCounter = counterIter.next();

            outFile.write("BalanceGroup: " + tmpBalGroup +
                          ", CounterID: " + counterId +
                          ", Rec id: " + tmpCounter.RecId +
                          ", valid: " + tmpCounter.validFrom + "-" + tmpCounter.validTo +
                          ", currentBal: " + tmpCounter.CurrentBalance +
                          "\n");
          }
        }
        
        outFile.flush();
        outFile.close();
      }
    }
    catch (IOException ioex)
    {
      OpenRate.getOpenRateFrameworkLog().error("Exception <" + ioex.getMessage() + "> while dumping balance group <" + tmpBalGroup + "> to file <" + filename + ">");
    }

    OpenRate.getOpenRateFrameworkLog().info("Dumping balance group <" + tmpBalGroup + "> to file <" + filename + "> completed");
  }

 /**
  * Dumps all available balance groups to file.
  *
  * @param filename The file to dump to
  */
  private void dumpBalanceGroups(String filename)
  {
    OpenRate.getOpenRateFrameworkLog().info("Dumping balance groups to file <" + filename + ">");

    try
    {
      try (BufferedWriter outFile = new BufferedWriter(new FileWriter(filename))) {
        outFile.write("# Balance data dump file\n");

        Iterator<Long> balIter = balanceCache.keySet().iterator();

        while (balIter.hasNext())
        {
          long balanceGroupId = balIter.next();
          BalanceGroup balanceGroup = balanceCache.get(balanceGroupId);

          Iterator<Integer> counterGrpIter = balanceGroup.getCounterIterator();
          while (counterGrpIter.hasNext())
          {
            int counterId = counterGrpIter.next();
            CounterGroup counter = balanceGroup.getCounterGroup(counterId);
            ArrayList<Counter> counterList = counter.getCounters();
            Iterator<Counter> counterIter = counterList.iterator();

            while (counterIter.hasNext())
            {
              Counter tmpCounter = counterIter.next();

              outFile.write("BalanceGroup: " + balanceGroupId +
                            ", CounterID: " + counterId +
                            ", Rec id: " + tmpCounter.RecId +
                            ", valid: " + tmpCounter.validFrom + "-" + tmpCounter.validTo +
                            ", currentBal: " + tmpCounter.CurrentBalance +
                            "\n");
            }
          }
        }
      }
    }
    catch (IOException ioex)
    {
      OpenRate.getOpenRateFrameworkLog().error("Exception <" + ioex.getMessage() + "> while dumping balance groups to file <" + filename + ">");
    }

    OpenRate.getOpenRateFrameworkLog().info("Dumping balance groups to file <" + filename + "> completed");
  }
}
