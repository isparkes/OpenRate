/* ====================================================================
 * Limited Evaluation License:
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

package OpenRate.resource;

import OpenRate.OpenRate;
import OpenRate.audit.AuditUtils;
import OpenRate.cache.*;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * CacheFactory class manages caching creation/retrieval of specific cache
 * manager. This class uses simple HashMap to store cacheable objects.
 *
 * Each of the cacheable objects created is stored inside a SimpleCacheManager
 * object which is really a minimal wrapper around a hash.
 *
 * The factory then indexes the manager that is responsible for a given class name,
 * thus each factory can be responsible for multiple managers. Managers are loaded
 * automatically on start up of the factory, using the property prefix "CacheableClass."
 *
 * It is a resource class hence has it will be registered into the
 * the ResourceContext and in the init method it initializes all the
 * CacheManagers and its respective caches.
 *
 * The CacheManager also manages the propagation of sync point requests to and
 * from the managed caches.
 */
public class CacheFactory
  implements IResource,
             ICacheFactory,
             IEventInterface,
             ISyncPoint
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: CacheFactory.java,v $, $Revision: 1.1 $, $Date: 2013-05-13 18:12:12 $";

 /**
  * Access to the Framework AstractLogger. All non-pipeline specific messages (e.g.
  * from resources or caches) should go into this log, as well as startup
  * and shutdown messages. Normally the messages will be application driven,
  * not stack traces, which should go into the error log.
  */
  protected ILogger FWLog;

  // The symbolic module name of the class stack
  private String symbolicName;

  // Managers is the list of the caches that we are managing in this factory
  private HashMap<String, CacheManager> managers;

  // This holds all of the caches that can call for a sync point
  private HashMap<String, ISyncPoint> eventCaches;

  // List of Services that this Client supports
  private final static String SERVICE_RELOAD      = "Reload";
  private final static String SERVICE_AUTO_RELOAD = "AutoReload";
  private final static String SERVICE_NEXT_RELOAD = "NextReloadIn";

  // This is how many seconds between reloads, 0 = no auto reload
  private long      autoReloadPeriod = 0;

  // the last time we did a reload
  private long      lastReloadUTC = 0;

  // controls whether resources are loaded sequentially or in parallel
  private boolean   sequentialLoading;
  
  // reference to the exception handler
  private ExceptionHandler handler;

  /**
   * The Key used to get the CacheFactory from the
   * configuration settings.
   */
  public static final String RESOURCE_KEY = "CacheFactory";

  // the list of cacheable classes
  private ArrayList<String> cacheableClassList;
  /**
   * Constructor
   */
  public CacheFactory()
  {
    // Add the version map
    AuditUtils.getAuditUtils().buildVersionMap(CVS_MODULE_INFO,this.getClass());
  }

  /**
   * This init method will be called while Resources are being registered and
   * initialised.  It obtains all the cacheable objects calls the loading
   * methods before adding into the CacheManagers.
   *
   * @param resourceName The resource name
   * @throws InitializationException
   */
  @Override
  public void init(String resourceName) throws InitializationException
  {
    IEventInterface   tmpEventIntf;
    ISyncPoint        tmpSyncIntf;
    CacheManager      cacheManager;
    ICacheable        cacheableObject;
    Class<?>          cacheableClass;
    String            tmpCacheableClassName;
    String            tmpCacheableClass;
    long              loadStartTime;
    long              loadEndTime;
    
    Iterator<String>  cacheableClassIter;
    String            configHelper;

    // Get the log before we start
    FWLog = LogUtil.getLogUtil().getLogger("Framework");

    // Start timer for loading
    loadStartTime = ConversionUtils.getConversionUtilsObject().getCurrentUTCms();
          
    FWLog.info("Starting CacheFactory initialisation");
    symbolicName = resourceName;

    if (!symbolicName.equalsIgnoreCase(RESOURCE_KEY))
    {
      // we are relying on this name to be able to find the resource
      // later, so stop if it is not right
      throw new InitializationException("Cache Factory ModuleName should be <" + RESOURCE_KEY + ">");
    }

    managers = new HashMap<>(50);
    eventCaches = new HashMap<>(50);

    // Get the list of cacheable classes
    cacheableClassList = PropertyUtils.getPropertyUtils().getGenericNameList("Resource.CacheFactory.CacheableClass");

    // Get the loading strategy
    sequentialLoading = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(RESOURCE_KEY, "SequentialLoading", "true").equalsIgnoreCase("true");
    
    // and the iterator we will be using to cycle through them
    cacheableClassIter = cacheableClassList.iterator();

    // Create a thread group for holding the cache objects while they are
    // being created
    ThreadGroup tmpGrpCaches = new ThreadGroup("Caches");
      
    // Iterate for the loading
    while (cacheableClassIter.hasNext())
    {
      tmpCacheableClassName = cacheableClassIter.next();

      try
      {
        tmpCacheableClass = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(resourceName,
                                                              tmpCacheableClassName,
                                                              "ClassName",
                                                              "None");

        // This is where we will launch the resources as separate classes if
        // necessary
        if (tmpCacheableClassName.equalsIgnoreCase("None") || 
           (tmpCacheableClass.equalsIgnoreCase("None")))
        {
          throw new InitializationException("Configuration for <" + tmpCacheableClassName + ">:<" + tmpCacheableClass + "> not complete.");
        }
        else
        {
          FWLog.info("Loading Cacheable Class <" + tmpCacheableClassName + ">...");
          System.out.println("    Loading Cacheable Class <" + tmpCacheableClassName + ">...");
              
          //Add the CacheManager to the manager list.
          cacheManager = new CacheManager();
          managers.put(tmpCacheableClassName, cacheManager);

          // Instantiate the object
          cacheableClass = Class.forName(tmpCacheableClass);
          cacheableObject = (ICacheable) cacheableClass.newInstance();
          cacheableObject.setHandler(handler);
          cacheManager.put(tmpCacheableClassName, cacheableObject);
          
          /*
           * Check if the cacheable instance is implementing
           * CacheLoader, if yes, then call its loadCache() method
           * to load all its data into cache i.e. by adding cache in
           * cache manager that is passed as argument.  Otherwise
           * the cacheable object is understood to be lazy-loaded.
           */
          if (cacheableObject instanceof ICacheLoader)
          {
            if (sequentialLoading)
            {
              ((ICacheLoader)cacheableObject).loadCache(resourceName, tmpCacheableClassName);
              FWLog.info("Loaded  Cacheable Class <" + tmpCacheableClassName + ">...");
              System.out.println("    Loaded  Cacheable Class <" + tmpCacheableClassName + ">...");
            }
            else
            {
              // Create a loader thread
              CacheLoaderThread cacheLoaderThread = new CacheLoaderThread(tmpGrpCaches,tmpCacheableClassName);

              // Set up the thread for loading
              cacheLoaderThread.setCacheObject(cacheableObject);
              cacheLoaderThread.setCacheName(tmpCacheableClassName);
              cacheLoaderThread.setResourceName(resourceName);
              cacheLoaderThread.setLoadStartTime(ConversionUtils.getConversionUtilsObject().getCurrentUTCms());
              cacheLoaderThread.setLog(FWLog);
              cacheLoaderThread.setExceptionHandler(OpenRate.getOpenRateExceptionHandler());

              // Launch the thread
              cacheLoaderThread.start();
            }
          }
        }
      }
      catch (ClassNotFoundException cnfe)
      {
        String Message = "ClassNotFoundException creating <" + tmpCacheableClassName + ">. Message = <" + cnfe.getMessage() + ">";
        handler.reportException(new InitializationException(Message,cnfe));
      }
      catch (InstantiationException ie)
      {
        String Message = "InstantiationException creating <" + tmpCacheableClassName + ">. Message = <" + ie.getMessage() + ">. Perhaps you are trying to use an abstract class.";
        handler.reportException(new InitializationException(Message,ie));
      }
      catch (IllegalAccessException iae)
      {
        String Message = "IllegalAccessException creating <" + tmpCacheableClassName + ">. Message = <" +  iae.getMessage() + ">";
        handler.reportException(new InitializationException(Message,iae));
      }
      catch (NullPointerException npe)
      {
        String Message = "NullPointerException creating <" + tmpCacheableClassName + ">. Message = <" + npe.getMessage() + ">";
        handler.reportException(new InitializationException(Message,npe));
      }
      catch (ClassCastException cce)
      {
        String Message = "Class Cast Exception creating <" + tmpCacheableClassName + ">. Message = <" +  cce.getMessage() + ">";
        handler.reportException(new InitializationException(Message,cce));
      }
      catch (NumberFormatException nfe)
      {
        String Message = "Number Format Exception creating <" + tmpCacheableClassName + ">. Message = <" + nfe.getMessage() + ">";
        handler.reportException(new InitializationException(Message,nfe));
      }
      catch (InitializationException ie)
      {
        handler.reportException(ie);
      }
      catch (OutOfMemoryError oome)
      {
        String Message = "Out of memory creating <" + tmpCacheableClassName + ">. Message = <" + oome.getMessage() + ">";
        handler.reportException(new InitializationException(Message,oome));
      }
      catch (ArrayIndexOutOfBoundsException aioob)
      {
        String Message = "Out of memory creating <" + tmpCacheableClassName + ">. Message = <" + aioob.getMessage() + ">";
        handler.reportException(new InitializationException(Message,aioob));
      }
      catch (Exception e)
      {
        String Message = "Unexpected Exception in <" + tmpCacheableClassName + ">. Message = <" + e.getMessage() + ">";
        handler.reportException(new InitializationException(Message,e));
      }
      catch (Throwable t)
      {
        String Message = "Unexpected Exception in <" + tmpCacheableClassName + ">. Message = <" + t.getMessage() + ">";
        handler.reportException(new InitializationException(Message,t));
      }

      // Set the auto reload parameter
      configHelper = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(resourceName,SERVICE_AUTO_RELOAD,"None");
      
      if (configHelper.equals("None") == false)
      {
        try
        {
          autoReloadPeriod = Integer.parseInt(configHelper);

          // Set the date so that we don't reload right away
          lastReloadUTC = ConversionUtils.getConversionUtilsObject().getCurrentUTC();
        }
        catch (NumberFormatException nfe)
        {
          String Message = "Parameter " + SERVICE_AUTO_RELOAD + " expects a numeric value, but the configured value <" + configHelper + "> is not numeric.";
        handler.reportException(new InitializationException(Message,nfe));
        }
      }
    }
    
    // Join the loading threads we launched
    while (tmpGrpCaches.activeCount() > 0)
    {
      try 
      {
        Thread.sleep(1000);
      } 
      catch (InterruptedException ex) 
      {
        // Nothing
      }
      
      // Check for errors
      if (OpenRate.getOpenRateExceptionHandler().hasError())
      {
        // no point in carrying on
        return;
      }
    }
    
    // destroy the thread group
    tmpGrpCaches.destroy();
    
    // reset the iterator
    cacheableClassIter = cacheableClassList.iterator();
    
    // Iterate for the other set ups
    while (cacheableClassIter.hasNext())
    {
      tmpCacheableClassName = cacheableClassIter.next();

      try
      {
        // Instantiate the object
        CacheManager manager = managers.get(tmpCacheableClassName);
        cacheableObject = manager.get(tmpCacheableClassName);

        /*
          * Reloadable cache
          *
          * check if its a cache that can be reloadable, and if is then get the value of 
          * property and store it in this cache object to be used,
          * later in the pipeline, the autorelaodPeriod is known for this specific cache
          */
        if (cacheableObject instanceof ICacheAutoReloadable)
        {
          String autoLoadPeriod  = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(resourceName,
                                                                                                  tmpCacheableClassName,
                                                                                                  SERVICE_AUTO_RELOAD,
                                                                                                  "0");

          try
          {
            ((ICacheAutoReloadable)cacheableObject).setAutoReloadPeriod(Long.parseLong(autoLoadPeriod));
          }
          catch (NumberFormatException nfe)
          {
            String Message = "Expected a numeric value for <" + SERVICE_AUTO_RELOAD + 
                              ">, in cache <" + getSymbolicName() + "> but got <" + 
                              autoLoadPeriod + ">";
            throw new InitializationException(Message);
          }

          // Log the fact that we are reloading this cache
          FWLog.info("Cache <" + tmpCacheableClassName + "> is set to auto reload with a period of <" + autoLoadPeriod +">");
        }

        // Now see if we have to register with the config manager
        if (cacheableObject instanceof IEventInterface)
        {
          // Register to accept events
          tmpEventIntf = (IEventInterface)cacheableObject;
          tmpEventIntf.registerClientManager();

          // If this class wanta to manage sync points, register that as well
          if (cacheableObject instanceof ISyncPoint)
          {
            // Store the reference to the SyncPoint interface
            tmpSyncIntf = (ISyncPoint)cacheableObject;
            eventCaches.put(tmpCacheableClassName,tmpSyncIntf);
          }
        }

        // Log the creation of the cacheable class
        FWLog.info("Created Cacheable Class <" + tmpCacheableClassName + ">");
      }
      catch (NullPointerException npe)
      {
        String Message = "NullPointerException creating <" + tmpCacheableClassName + ">. Message = <" + npe.getMessage() + ">";
        handler.reportException(new InitializationException(Message,npe));
      }
      catch (ClassCastException cce)
      {
        String Message = "Class Cast Exception creating <" + tmpCacheableClassName + ">. Message = <" +  cce.getMessage() + ">";
        handler.reportException(new InitializationException(Message,cce));
      }
      catch (NumberFormatException nfe)
      {
        String Message = "Number Format Exception creating <" + tmpCacheableClassName + ">. Message = <" + nfe.getMessage() + ">";
        handler.reportException(new InitializationException(Message,nfe));
      }
      catch (InitializationException ie)
      {
        handler.reportException(ie);
      }
      catch (Exception ex)
      {
        String Message = "Unexpected Exception in <" + tmpCacheableClassName + ">. Message = <" + ex.getMessage() + ">";
        handler.reportException(new InitializationException(Message,ex));
      }

      // Set the auto reload parameter
      configHelper = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(resourceName,SERVICE_AUTO_RELOAD,"None");
      
      if (configHelper.equals("None") == false)
      {
        try
        {
          autoReloadPeriod = Integer.parseInt(configHelper);

          // Set the date so that we don't reload right away
          lastReloadUTC = ConversionUtils.getConversionUtilsObject().getCurrentUTC();
        }
        catch (NumberFormatException nfe)
        {
          String Message = "Parameter " + SERVICE_AUTO_RELOAD + " expects a numeric value, but the configured value <" + configHelper + "> is not numeric.";
        handler.reportException(new InitializationException(Message,nfe));
        }
      }
    }
    
    loadEndTime = ConversionUtils.getConversionUtilsObject().getCurrentUTCms();
    FWLog.info("CacheFactory initialised in " + (loadEndTime-loadStartTime) + "ms.");
  }

  /**
   * Retrieves the CacheManager that is specific to the className object.
   *
   * @param className The name of the class we are managing
   * @return The manager
   */
  @Override
  public ICacheManager getManager(String className)
  {
    return managers.get(className);
  }

  /**
   * As part of Resource object it cleans up the CacheManagers
   * which in turn will make cached data garbage collected.
   */
  @Override
  public void close()
  {
    CacheManager cacheableObjectManager;
    Object       cacheableObject;
    Collection<String>   CacheObjects;
    Iterator<String>     CacheObjIterator;
    String       tmpCacheableClassName;

    // Iterate through the managers to find those that have the ICacheSaver
    // interface
    CacheObjects = managers.keySet();
    CacheObjIterator = CacheObjects.iterator();

    while (CacheObjIterator.hasNext())
    {
      tmpCacheableClassName = CacheObjIterator.next();
      cacheableObjectManager = managers.get(tmpCacheableClassName);
      cacheableObject = cacheableObjectManager.get(tmpCacheableClassName);

      if (cacheableObject instanceof ICacheSaver)
      {
        System.out.println(
              "  Destroying Cacheable Class <" + tmpCacheableClassName +
              ">...");
        ((ICacheSaver)cacheableObject).saveCache();
        FWLog.info("Saved Cacheable Class <" + tmpCacheableClassName + ">...");
        System.out.println(
              "  Unloaded Cacheable Class <" + tmpCacheableClassName +
              ">...");
      }
    }

    managers.clear();
  }

  /**
   * Get the cache manager for the given class name.
   *
   * @param className The class name to get the manager for
   * @return The manager for the class
   * @throws ConfigurationException
   */
  public static ICacheManager getGlobalManager(String className) throws InitializationException
  {
    ICacheManager   CM = null;
    ResourceContext ctx = new ResourceContext();

    // try the new Logging model.
    ICacheFactory factory = (ICacheFactory)ctx.get(RESOURCE_KEY);

    if (factory == null)
    {
      // no factory registered, fall back to the old model
      LogUtil.getStaticLogger("Framework").fatal("No Cache Factory loaded");
    }
    else
    {
      CM = factory.getManager(className);
    }

    return CM;
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ISyncPoint functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * Pass through method for managing sync points for the data caches that the
  * cache manager is handling. To calculate the value to return, we do the
  * following:
  * 1) if any of the caches requests a sync point, we return the sync request
  *    immediately (=SyncFlag)
  * 2) We return 3 (=SyncReached) when all managed modules are at 3
  * 3) Likewise 5 (=SyncDone)
  *
  * @return The current sync status
  */
  @Override
  public int getSyncStatus()
  {
    Iterator<String> SyncListIter;
    ISyncPoint       tmpSyncIntf;
    int              tmpReturnCode;
    int              tmpMaxCode = 0;
    boolean          SyncReached = true;
    boolean          SyncFinished = true;

    SyncListIter = eventCaches.keySet().iterator();

    while (SyncListIter.hasNext())
    {
      tmpSyncIntf = eventCaches.get(SyncListIter.next());
      tmpReturnCode = tmpSyncIntf.getSyncStatus();

      // calculate the max code (which will be returned if neither the
      // SyncReached nor the SyncFinished is set
      if (tmpReturnCode > tmpMaxCode)
      {
        tmpMaxCode = tmpReturnCode;
      }

      SyncReached &= (tmpReturnCode == 3);
      SyncFinished &= (tmpReturnCode == 5);
    }

    if (SyncFinished)
    {
      return 5;
    }
    else if (SyncReached)
    {
      return 3;
    }
    else
    {
      return tmpMaxCode;
    }
  }

 /**
  * Pass through method for managing sync points for the data caches that the
  * cache manager is handling. Pass the received value to all managed data
  * cache objects
  *
  * @param newStatus The new sync status to set
  */
  @Override
  public void setSyncStatus(int newStatus)
  {
    Iterator<String> SyncListIter;
    ISyncPoint       tmpSyncIntf;

    SyncListIter = eventCaches.keySet().iterator();

    while (SyncListIter.hasNext())
    {
      tmpSyncIntf = eventCaches.get(SyncListIter.next());
      tmpSyncIntf.setSyncStatus(newStatus);
    }
  }
  
  /**
   * Pass through method for managing sync points for the data caches that the
   * cache manager is handling. Pass the received value to all managed data
   * cache objects
   *
   * @param newStatus The new sync status to set
   * @param cacheName The name of the cache to set for
   */
   
   public void setSyncStatus(int newStatus, String cacheName)
   {
     ISyncPoint       tmpSyncIntf;

     tmpSyncIntf = eventCaches.get(cacheName);
     tmpSyncIntf.setSyncStatus(newStatus);
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
    ClientManager.registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_RELOAD, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_AUTO_RELOAD, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_NEXT_RELOAD, ClientManager.PARAM_DYNAMIC);
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

    // Trigger a sync point
    if (Command.equalsIgnoreCase(SERVICE_RELOAD))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // Instruct all the caches to reload
        setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_FLAGGED);

        // tell the user
        return "Event buffered";
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        // Don't reload
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase(""))
      {
        // return the current state
        if (getSyncStatus() == ISyncPoint.SYNC_STATUS_NORMAL_RUN)
        {
          return "false";
        }
        else
        {
          return "true";
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
          autoReloadPeriod = Long.valueOf(autoReloadPeriod);
          ResultCode = 0;
        }
        catch (Exception e)
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
      FWLog.debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

 /**
  * Return the symbolic name of the class
  *
  * @return The symbolic name of the module
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }

  /**
   * Manages the auto reload function. This means that we will periodically
   * reload the caches without user intervention.
   *
   * Each cache can be configured independently for the load frequency.
   */
   public void updateAutoReload()
   {
    long currentTimeUTC;
    Iterator<String> CacheableClassIter = cacheableClassList.iterator();
    String tmpCacheableClassName;

    // talk to each of the caches in turn
	  while (CacheableClassIter.hasNext())
	  {
      // get the reference to the cache
      tmpCacheableClassName = CacheableClassIter.next();
		  
      Object tmpCacheableClass = getManager(tmpCacheableClassName).get(tmpCacheableClassName);
      if (tmpCacheableClass instanceof ICacheAutoReloadable)
      {
        // get the reload period - this comes from the class itself, or from
        // the factory if no period is set for the class
        ICacheAutoReloadable cacheableObject = (ICacheAutoReloadable)tmpCacheableClass;
        long autoLoadPeriod = cacheableObject.getAutoReloadPeriod();

        // set the auto reload period in the class if none was set there
        // and one was set on the factory
        if(autoLoadPeriod == 0 && this.autoReloadPeriod > 0)
        {
          cacheableObject.setAutoReloadPeriod( this.autoReloadPeriod);
          autoLoadPeriod = this.autoReloadPeriod;
        }

        if(autoLoadPeriod > 0)
        {
          // get the current utc date time that we will be working with
          currentTimeUTC = ConversionUtils.getConversionUtilsObject().getCurrentUTC();

          //If pipeline just started, update the last reload to NOW because the pipeline already loaded all the cache
          //once, so we don't want to do it again
          if(cacheableObject.getLastReloadUTC()==0) {cacheableObject.setLastReloadUTC(currentTimeUTC);}

          // if we have passed the time of the last auto reload
          if ((cacheableObject.getLastReloadUTC() + cacheableObject.getAutoReloadPeriod()) < currentTimeUTC)
          {
            cacheableObject.setLastReloadUTC(currentTimeUTC);

            // Instruct specific cache to reload
            setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_FLAGGED,tmpCacheableClassName);
          }
        }
      }
	  }
  }

  /**
   * Set the exception handler for handling any exceptions.
   *
   * @param handler the handler to set
   */
  @Override
  public void setHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }
}
