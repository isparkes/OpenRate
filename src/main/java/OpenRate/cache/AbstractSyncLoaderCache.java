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
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements an abstract cache loader class that implements sync
 * point handling.
 *
 * The sync loader handling deals with much of the management for caches where
 * transactionally safe reloading is required. This means that the processing
 * must be completely stopped before the loading can begin.
 *
 * @author i.sparkes
 * @author AminS auto reloadable caches
 */
public abstract class AbstractSyncLoaderCache
              extends AbstractCache
           implements ICacheable,
                      ICacheLoader,
                      IEventInterface,
                      ISyncPoint,
                      ICacheAutoReloadable
{
  /**
   * This is the source type of the data to load
   */
  protected String CacheDataSourceType = null;

  /**
   * This is the location of the file to load (or reload)
   */
  protected String cacheDataSourceName = null;

  /**
   * This is used to hold the name of the file to load the data from
   */
  protected String cacheDataFile;

  /**
   * This is our connection object
   */
  protected Connection JDBCcon;

  /**
   * this is the persistent result set that we use to incrementally get the records
   */
  protected ResultSet mrs = null;

  /**
   * these are the statements that we have to prepare to be able to get records
   * once and only once
   */
  protected String CacheDataSelectQuery = null;

  /**
   * this is the name of the method we are to use in the case that the method
   * data source has been defined
   */
  protected String CacheMethodName = null;

  /**
   * This is used by the method loading
   */
  protected ICacheDataMethod DataMethod = null;

  /**
   * these are the prepared statements
   */
  protected PreparedStatement StmtCacheDataSelectQuery;

  // List of Services that this Client supports
  private final static String SERVICE_RELOAD        = "Reload";
  private final static String SERVICE_AUTO_RELOAD   = "AutoReload";
  private final static String SERVICE_NEXT_RELOAD   = "NextReloadIn";
  private final static String SERVICE_DATE_FORMAT   = "DateFormat";
  private final static String SERVICE_LOAD_LOG_STEP = "LoadLogStep";
  private final static String SERVICE_NO_AUTORELOAD = "ExcludeFromAutoReload";

  // Variables for managing the sync points
  private int syncStatus = 0;

  // The pending commands ArrayList provides the buffering for sync processing
  private final ArrayList<String> pendingCommands;

  /**
   * this is used to handle multiple data formats easily
   */
  protected ConversionUtils fieldInterpreter;

  // This is how many seconds between reloads, 0 = no auto reload
  private long autoReloadPeriod = 0;

  // the last time we did a reload
  private long lastReloadUTC = 0;

  // if we are to be excluded from auto-reload
  private boolean excludeFromAutoReload;

 /**
  * the frequency with which we update the log progress messages on loading
  */
  protected long loadingLogNotificationStep = 1000;

 /**
  * Constructor
  */
  public AbstractSyncLoaderCache()
  {
    // Initialise the store of pending commands
    pendingCommands = new ArrayList<>();

    // Initialise the date parsing
    fieldInterpreter = new ConversionUtils();
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
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    // Variable declarations
    boolean foundCacheDataSourceType = false;
    boolean foundStatements;
    boolean foundMethods;
    String  tmpDateFormat;
    String  tmpCacheSource;

    // Get the module symbolic name
    setSymbolicName(CacheName);

    // Find the location of the configuration data
    OpenRate.getOpenRateFrameworkLog().info("Starting cache loading for <" + getSymbolicName() + ">");

    // Get the type of source we are to read from
    tmpCacheSource = initGetCacheSourceType(ResourceName, CacheName);

    if (tmpCacheSource.equalsIgnoreCase("File") |
        tmpCacheSource.equalsIgnoreCase("DB") |
        tmpCacheSource.equalsIgnoreCase("Method"))
    {
      foundCacheDataSourceType = true;
      CacheDataSourceType = tmpCacheSource;
    }

    if (!foundCacheDataSourceType)
    {
      message = "CacheDataSourceType for cache <" + getSymbolicName() +
            "> must be File, DB or method, found <" + CacheDataSourceType + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Get the date format if something is defined
    tmpDateFormat = initGetDateFormat(ResourceName, CacheName);
    if (!tmpDateFormat.equals("None"))
    {
      processControlEvent(SERVICE_DATE_FORMAT, true, tmpDateFormat);
    }

    // Get the loading step, if one is defined
    loadingLogNotificationStep = initGetLoadingStep(ResourceName, CacheName);

    // Get the configuration we are working on
    if (CacheDataSourceType.equalsIgnoreCase("File"))
    {
      // get the data statement(s)
      foundStatements = getDataFiles(ResourceName,CacheName);

      if (foundStatements == false)
      {
        message = "Data files not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        loadDataFromFile();
      }
    }
    else if (CacheDataSourceType.equalsIgnoreCase("DB"))
    {
      // Get the data source name
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                  CacheName,
                                                                  "DataSource",
                                                                  "None");

      if (cacheDataSourceName.equals("None"))
      {
        message = "Data source DB name not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug("Found Cache Data DB <" + cacheDataSourceName + "> for cache <" + getSymbolicName() + ">");
      }

      // get the data statement(s)
      foundStatements = getDataStatements(ResourceName,CacheName);

      if (foundStatements == false)
      {
        message = "One or more select statements not found for cache <" +
              getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug(
              "Found select Query <" + CacheDataSelectQuery + "> for cache <" +
              getSymbolicName() + ">");
      }

      // The datasource property was added to allow database to database
      // JDBC adapters to work properly using 1 configuration file.
      if(DBUtil.initDataSource(cacheDataSourceName) == null)
      {
        message = "Could not initialise DB connection <" + cacheDataSourceName + "> to in module <" + getSymbolicName() + ">.";
        throw new InitializationException(message,getSymbolicName());
      }

      loadDataFromDB();
    }
    else if (CacheDataSourceType.equalsIgnoreCase("Method"))
    {
      // get the data method name(s)
      foundMethods = getDataMethods(ResourceName,CacheName);

      if (foundMethods == false)
      {
        message = "Data source methods not found for cache <" + getSymbolicName() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().debug(
              "Found Select Method <" + CacheMethodName + "> for cache <" +
              getSymbolicName() + ">");
      }

      // Just call the method directly
      loadDataFromMethod();
    }

    // Get the auto reload exclusion
    excludeFromAutoReload = initGetExcludeFromReload(ResourceName, CacheName);
  }

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
  protected boolean getDataStatements(String ResourceName, String CacheName)
          throws InitializationException
  {
    // Get the Select statement
    CacheDataSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "SelectStatement",
                                                                     "None");

    if (CacheDataSelectQuery.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * get the data method to use. Implemented as a separate function so that it can
  * be overwritten in implementation classes. By default the cache picks up the
  * statement with the name "MethodName".
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return True if the statements were found, otherwise false
  * @throws InitializationException
  */
  protected boolean getDataMethods(String ResourceName, String CacheName) throws InitializationException
  {
    // Get the Select statement
    CacheMethodName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "MethodName",
                                                                     "None");

    if (CacheMethodName.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * get the select statement(s). Implemented as a separate function so that it can
  * be overwritten in implementation classes.
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return true if the file were found, else false
  * @throws InitializationException
  */
  protected boolean getDataFiles(String ResourceName, String CacheName) throws InitializationException
  {
    cacheDataFile = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                              CacheName,
                                                              "DataFile",
                                                              "None");

    if (cacheDataFile.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * Reload the data from the defined data source
  *
  * @throws InitializationException
  */
  public void ReloadData() throws InitializationException
  {
    // See if we are excluded from reloading
    if (getExcludeFromAutoReload() == false)
    {
      // Clear down the old information
      clearCacheObjects();

      if (CacheDataSourceType.equalsIgnoreCase("File"))
      {
        // Reload
        loadDataFromFile();
      }

      if (CacheDataSourceType.equalsIgnoreCase("DB"))
      {
        // Reload
        loadDataFromDB();
      }

      if (CacheDataSourceType.equalsIgnoreCase("Method"))
      {
        // Reload
        loadDataFromMethod();
      }

      // inform the user
      System.out.println("    Reload Cacheable Class <" + getSymbolicName() + ">");
    }
    else
    {
      // log that we skipped it
      OpenRate.getOpenRateFrameworkLog().info("Skipped auto reloading cache <" + getSymbolicName() + "> because it is excluded from AutoReload");
    }
  }

 /**
  * Get the data from the data layer method
  *
  * @param cacheToRetrieveFor The name of the cache this method is being called for
  * @param MethodClassName The name of the method class to call to fill this cache
  * @throws InitializationException
  * @return Amorphic list of results
  */
  public List<ArrayList<String>> getMethodData(String cacheToRetrieveFor, String MethodClassName)
    throws InitializationException
  {
    Class<?> MethodClass;
    ArrayList<ArrayList<String>> Result;

    try
    {
      // Get the method instance
      MethodClass = Class.forName(MethodClassName);
    }
    catch (ClassNotFoundException cnfe)
    {
      message = "Error finding data cache class <" + MethodClassName +
                                        "> in data cache <" + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Now try to get the instance
    try
    {
      DataMethod = (ICacheDataMethod)MethodClass.newInstance();
    }
    catch (InstantiationException ex)
    {
      message = "Data method class <" + MethodClassName +
                                        "> instantiation error in cache <" +
                                        getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (IllegalAccessException ex)
    {
      message = "Data method class  <" + MethodClassName +
                                        "> access error in pipeline <" +
                                        getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Now we can get the data from the method
    Result = (ArrayList<ArrayList<String>>) DataMethod.getCacheDataFromMethod(cacheDataFile, MethodClassName);

    return Result;
  }

 /**
  * Load the data from the defined file
  *
  * @throws InitializationException
  */
  public abstract void loadDataFromFile() throws InitializationException;

 /**
  * Load the data from the defined Data Source
  *
  * @throws InitializationException
  */
  public abstract void loadDataFromDB() throws InitializationException;

 /**
  * Load the data from the defined Data Source Method
  *
  * @throws InitializationException
  */
  public abstract void loadDataFromMethod() throws InitializationException;

 /**
  * Clear down the internal cache objects
  */
  public abstract void clearCacheObjects();

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ISyncPoint functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  *
  * @return The current sync status
  */
  @Override
  public int getSyncStatus()
  {
    return syncStatus;
  }

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  *
  * @param newStatus The new sync status to set
  */
  @Override
  public void setSyncStatus(int newStatus)
  {
    if (newStatus == ISyncPoint.SYNC_STATUS_SYNC_FLAGGED)
    {
      // we are being forced to reload by the cache manager
      // Add the command to the pending list
      pendingCommands.add(SERVICE_RELOAD);
      syncStatus = ISyncPoint.SYNC_STATUS_SYNC_FLAGGED;
    }
    else if (newStatus == ISyncPoint.SYNC_STATUS_SYNC_REQUESTED)
    {
      // no end of transaction processing needed, so say that we have finshed
      // We can rely on the pipeline to finish the processing for us.
      syncStatus = ISyncPoint.SYNC_STATUS_SYNC_REACHED;
    }
    else if (newStatus == ISyncPoint.SYNC_STATUS_SYNC_PROCESSING)
    {
      // perform the pending commands.
      processPendingCommands();

      // Report that we have finished processing
      syncStatus = ISyncPoint.SYNC_STATUS_SYNC_FINISHED;
    }
    else
    {
      syncStatus = newStatus;
    }
  }

 /**
  * This method processes any events that have been queued as part of the
  * sync processing.
  */
  private void processPendingCommands()
  {
    String tmpCommand;
    boolean CommandHandled;

    for (int Index = 0 ; Index < pendingCommands.size() ; Index++ )
    {
      tmpCommand = pendingCommands.get(Index);

      // see if we have already processed this buffered event in the current session
      CommandHandled = false;
      for (int ChkIndex = Index ; ChkIndex > 0 ; ChkIndex--)
      {
        if (tmpCommand.equalsIgnoreCase(pendingCommands.get(ChkIndex)))
        {
          // we already dealt with this - skip
          CommandHandled = true;
          break;
        }
      }

      if (!CommandHandled)
      {
        if (tmpCommand.equalsIgnoreCase(SERVICE_RELOAD))
        {
          // Load the new data
          try
          {
            ReloadData();
          }
          catch (InitializationException ex)
          {
            message = "SERVICE_RELOAD not executed because of InitializationException thrown by loadData()";
            OpenRate.getOpenRateFrameworkLog().fatal(message,ex);
          }
        }
      }
    }
    pendingCommands.clear();
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_RELOAD, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_AUTO_RELOAD, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_NEXT_RELOAD, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DATE_FORMAT, ClientManager.PARAM_SYNC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_LOAD_LOG_STEP, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_NO_AUTORELOAD, ClientManager.PARAM_NONE);
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

    if (Command.equalsIgnoreCase(SERVICE_RELOAD))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // Add the command to the pending list
        pendingCommands.add(SERVICE_RELOAD);

        // Flag the sync point
        syncStatus = 1;

        // tell the user
        return "Event buffered";
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        // Don't reload
        ResultCode = 0;
      }
      else if (Parameter.equals(""))
      {
        // return the current state
        if (syncStatus == 0)
        {
          return "false";
        }
        else
        {
          return "true";
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DATE_FORMAT))
    {
      if (Parameter.equals(""))
      {
        return fieldInterpreter.getInputDateFormat();
      }
      else
      {
        // set the new date format
        if (fieldInterpreter.setInputDateFormat(Parameter) == false)
        {
          return "Could not use date format <" + Parameter + ">";
        }
      }
    }

    // Get/Set the auto reload period
    if (Command.equalsIgnoreCase(SERVICE_AUTO_RELOAD))
    {
      if (Parameter.equalsIgnoreCase(""))
      {
        // return the configured value
        return Long.toString(autoReloadPeriod);
      }
      else
      {
        // try to set the new value
        try
        {
          autoReloadPeriod = Long.valueOf(Parameter);
          ResultCode = 0;
        }
        catch (NumberFormatException e)
        {
          return "Could not interpret <" + autoReloadPeriod + "> as an integer value";
        }
      }
    }

    // Get the number of seconds to the next reload
    if (Command.equalsIgnoreCase(SERVICE_NEXT_RELOAD))
    {
      // see if something is configured
      if (autoReloadPeriod == 0)
      {
        // Just return the value
        return Long.toString(autoReloadPeriod);
      }
      else
      {
        // calculate the number of seconds until the reload
        return Long.toString(lastReloadUTC + autoReloadPeriod - ConversionUtils.getConversionUtilsObject().getCurrentUTC());
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
      StmtCacheDataSelectQuery = JDBCcon.prepareStatement(CacheDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + CacheDataSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

  /**
   * Get the period (seconds) of the cache auto reloading
   *
   * @return The configured period
   */
  @Override
  public  long getAutoReloadPeriod()
  {
  	return autoReloadPeriod;
  }

  /**
   * Set the period (seconds) of the cache auto reloading
   *
   * @param newAutoReloadPeriod The new auto-reload period
   */
  @Override
  public void setAutoReloadPeriod(long newAutoReloadPeriod)
  {
  	autoReloadPeriod = newAutoReloadPeriod;
  }

  /**
   * Get the last time that this cache was reloaded
   *
   * @return The last reload time
   */
  @Override
  public long getLastReloadUTC()
  {
  	return lastReloadUTC;
  }

 /**
  * Set the time that this cache was last reloaded
  *
  * @param newLastReloadUTC The reload time
  */
  @Override
 public void setLastReloadUTC(long newLastReloadUTC)
  {
  	lastReloadUTC = newLastReloadUTC;
  }

  /**
   * See if this cache has been excluded from auto reloading
   *
   * @return true if excluded, otherwise false
   */
  @Override
  public boolean getExcludeFromAutoReload()
  {
    return excludeFromAutoReload;
  }

// -----------------------------------------------------------------------------
// -------------------- Start of local utility functions -----------------------
// -----------------------------------------------------------------------------

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetDateFormat(String ResourceName, String CacheName) throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,CacheName,SERVICE_DATE_FORMAT,"None");
    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetCacheSourceType(String ResourceName, String CacheName) throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "DataSourceType",
                                                       "None");

    return tmpValue;
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
    catch (NumberFormatException nfe)
    {
      message = "Value provided for property <" + SERVICE_LOAD_LOG_STEP +
                                        "> was not numeric. Received value <" + tmpValue + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    return tmpLoadStep;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private boolean initGetExcludeFromReload(String ResourceName, String CacheName) throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       SERVICE_NO_AUTORELOAD,
                                                       "False");

    // try to convert it
    if (tmpValue.equalsIgnoreCase("true") || tmpValue.equalsIgnoreCase("false"))
    {
      return Boolean.valueOf(tmpValue);
    }
    else
    {
      message = "Value provided for property <" + SERVICE_NO_AUTORELOAD +
                "> was not boolean. Received value <" + tmpValue + ">.";
      throw new InitializationException(message,getSymbolicName());
    }
  }
}
