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

package OpenRate;

import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.EventHandler;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.resource.ResourceLoaderThread;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.PropertyUtils;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The OpenRate Daemon application is the main class that is responsible for
 * running OpenRate applications in Daemon Mode (meaning that it runs forever
 * until it is stopped by user intervention). You can also run applications in
 * Batch Mode (meaning that the application will start, process any input and
 * then close down). This class is responsible for:<br>
 *  - Creating the framework class<br>
 *  - Creating the resources<br>
 *  - Initialising the resources<br>
 *  - Creating the pipeline (Input, Output and Processing modules)<br>
 *  - Initialising all of the modules in the pipeline<br>
 *  - running the pipeline<br>
 *  - reporting status when the pipeline finishes<br>
 *  - Managing the pipeline sync between multiple pipes for external events that
 *    require all pipes to be inactive before they can be processed (e.g.
 *    reloads of data resource class data)
 *
 *<p>    This is a brief description of how the sync works:<br>
 *    1) When a pipeline receives an event that requires a sync point (for
 *       example a shared data resource needs to be reloaded), it flags the
 *       need by setting the sync status to 1 (=SyncFlag)<br>
 *    2) The framework polls the status and sees that a SyncFlag has been set.
 *       It responds by setting the sync status of the requesting pipeline to
 *       2 (=SyncRequest). It also sets the sync flag of other pipelines to
 *       2 as well.<br>
 *    3) Any pipeline that has a SyncRequest has the obligation to stop
 *       processing after finishing the current transaction. When the transaction
 *       has been closed, it sets the sync status to 3 (=SyncReached)<br>
 *    4) When all pipelines have come to the status "SyncReached", the framework
 *       sets the synch status of all pipelines to 4 (=SyncProcess). Any pipeline
 *       that has pending events now processes them<br>
 *    5) When the event processing has finished, each pipeline sets the sync
 *       status to 5 (=SyncDone).<br>
 *    6) When all pipelines reach "SyncDone" the framework sets the sync status
 *       of all pipelines back to 0, and the sync process finishes<br>
 */
public class OpenRate
  implements IEventInterface,
             Runnable
{

  /**
   * Returns the application version string.
   * 
   * @return the applicationVersionString
   */
  public static String getApplicationVersionString() {
    return applicationVersionString;
  }

 /**
  * Access to the Framework AstractLogger. All non-pipeline specific messages (e.g.
  * from resources or caches) should go into this log, as well as startup
  * and shutdown messages. Normally the messages will be application driven,
  * not stack traces, which should go into the error log.
  */
  private ILogger fwLog = null;

  // Access to the Error AstractLogger. All exception stack traces should go here.
  private ILogger errorLog = null;

  //Access to the Statistics AstractLogger. All statistics info should go here.
  private ILogger statsLog = null;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for a successful completion
   */
  public static final int SUCCESS = 0;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for a successful completion
   */
  public static final int VERSION_FILE_NOT_FOUND = -4;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for a successful completion
   */
  public static final int PROPERTIES_FILE_NOT_FOUND = -5;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for a successful completion
   */
  public static final int WRONG_ARGUMENTS = -3;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for a successful completion, but with some exception
   */
  public static final int SUCCESS_WITH_EXCEPTION = 1;

  /**
   * Return code for the OpenRate instance.
   *
   * Default return code for failure due to a critical error
   */
  public static final int FATAL_EXCEPTION = 2;

  // List of Services that this Client supports
  private final String SERVICE_FRAMEWORK_STOP     = "Shutdown";
  private final String SERVICE_SYNC_STATUS        = "SyncStatus";
  private final String SERVICE_SEQUENTIAL_LOADING = "SequentialLoading";

  // This holds all of the pipelines that we are dealing with in this framework
  private static HashMap<String, IPipeline> pipelineMap;

  // This holds all of the resources that can call for a sync point
  private static HashMap<String, IEventInterface> syncPointResourceMap;

  // This is the application name, used for information only
  private String applicationName = null;

  // Pipeline pointer for performing pipeline related maniputions
  private IPipeline          tmpPipeline;
  private Collection<String> pipelineSet = null;
  private Iterator<String>   pipelineIter = null;
  private ThreadGroup        pipelineThreadGroup;

  // Context for managing the framework resources
  private ResourceContext    resourceContext = new ResourceContext();
  private Collection<String> resourceSet = null;
  private Iterator<String>   resourceIter = null;

  // For managing pipeline synchronisation
  private boolean   syncRequested = false;
  private boolean   syncReached;
  private boolean   syncFinished;

  // the sync status, which is used to control sync processing
  private int       syncStatus = ISyncPoint.SYNC_STATUS_NORMAL_RUN;

  // controls whether resources are loaded sequentially or in parallel
  private boolean   sequentialLoading = true;

  // For managing semaphore files
  private EventHandler ECI = null;

  // For managing the auto reload function
  private CacheFactory cacheFactory = null;

  // Concrete classes for the following member attributes are
  // loaded from a property file and instantiated via reflection.
  private ExceptionHandler frameworkExceptionHandler = new ExceptionHandler();

  // module symbolic name: set during initalisation
  private String symbolicName = "Framework";

  // The application object - split out like this so we can embed OpenRate if we need to
  // and perform unit tests
  private static OpenRate appl;

  // Used to help people with fat fingers not shut the process down by accident
  private int shutdownHookCalls = 0;
  
  // Global application version
  private static String applicationVersionString;
  
  // used to simplify logging and error handling
  private String message;
  
  // value to show if the framework has active processing pipelines or not.
  // This is true all the time that there are some pipes active
  private boolean pipelinesActive = false;
  
  /**
   * default constructor
   */
  public OpenRate()
  {
    // Initialise the pipeline index
    pipelineMap = new HashMap<>(50);

    // Initialise the map of the event aware resources (for sync point handling)
    syncPointResourceMap = new HashMap<>(50);
  }

  /**
   * Main() - create the framework and the pipelines, and then execute them.
   * If you need to have access to a detached OpenRate object, you can use do
   * so by following the schema here:
   *   - Create the OpenRate object
   *   - call "createApplication"
   *   - call the "run" method
   *   - when finished, call the "finaliseApplication"
   *
   * @param args The arguments to pass to the process
   */
  public static void main(String[] args)
  {
    int            status;

    // Create the application - this initialises the entire system
    appl = OpenRate.getApplicationInstance();
    
    if (appl == null)
    {
      System.err.println("Could not get OpenRate instance");
      System.exit(-6);
    }
    
    // Create the application
    status = appl.createApplication(args);

    // run it if we created the app correctly
    if (status == SUCCESS)
    {
      appl.run();
    }

    // print shutdown message
    appl.finaliseApplication();

    // Bye bye, please come back soon
    System.exit(status);
  }

 /**
  * Check that the command line parameters are correct.
  *
  * @param args the passed command line parameters
  * @return 0 if OK, otherwise a return code
  */
  public int checkParameters(String[] args) {
    // Check the arguments - we expect "-p testfile.properties.xml"
    if ((args.length == 2) && (args[0].trim().equals("-p")))
    {
      // Check that the file exists
      URL propertiesFile = getClass().getResource( "/" + args[1] );
      
      if (propertiesFile == null)
      {
        System.out.println("Properties file <"+args[1]+"> not found on the class path. Aborting.");
        return PROPERTIES_FILE_NOT_FOUND;
      }
      
      System.out.println("Found properties file <" + propertiesFile.getFile() + ">.");
    } else {
      // Arguments are not what we want
      System.out.println("Command line not given correctly. Aborting.");
      System.out.println("  Usage: java -cp $CLASSPATH OpenRate.OpenRate -p <properties-file.properties.xml>");
      return WRONG_ARGUMENTS;
    }

    // All OK
    return SUCCESS;
  }

 /**
  * Get the properties file name from the command line parameters.
  *
  * @param args the passed command line parameters
  * @return the file name as a URL, otherwise null
  */
  public URL getPropertiesFileName(String[] args) {
    URL propertiesFile = getClass().getResource( "/" + args[1] );
    return propertiesFile;
  }
  
 /**
  * Get the global version from the version file.
  *
  * @return the application version
  */
  public String getApplicationVersion() throws InitializationException
  {
    boolean foundVersion = false;
    boolean foundBuild = false;
    boolean foundDate = true;
    String  versionID = null;
    String  buildVer = null;
    String  buildDate = null;
    InputStream input;
      
    URL versionResourceFile = getClass().getResource("/VersionFile.txt");
      
    if (versionResourceFile == null)
    {
      message = "Version Information not found";
      throw new InitializationException(message,getSymbolicName());
    }
      
    input = getClass().getResourceAsStream("/VersionFile.txt");
    java.util.Scanner s = new java.util.Scanner(input).useDelimiter("\\A");

    while (s.hasNext())
    {
      String result = s.nextLine();
      if (result.startsWith("OPENRATE_VERSION:"))
      {
        foundVersion = true;
        versionID = result.replaceAll("OPENRATE_VERSION:", "").trim();
      }

      if (result.startsWith("BUILD_VERSION:"))
      {
        foundBuild = true;
        buildVer = result.replaceAll("BUILD_VERSION:", "").trim();
      }

      if (result.startsWith("BUILD_DATE:"))
      {
        foundDate = true;
        buildDate = result.replaceAll("BUILD_DATE:", "").trim();
      }
    }

    if (foundVersion && foundBuild && foundDate)
    {
      return "OpenRate V"+versionID+", Build " + buildVer + " (" + buildDate + ")";
    }
    else
    {
      return null;
    }
  }
  
  /**
   * Creates the OpenRate application. This is primarily here so that the
   * OpenRate core can be launched in embedded mode.
   *
   * @param args The command line arguments
   * @return The exit code
   */
  public int createApplication(String[] args)
  {
    int               status;
    ArrayList<String> pipelineList;
    String            tmpPipelineToCreate;
    boolean           initError = false;

    // *********************** Initialization Block ****************************
    // Set the version string
      try
      {
        // Get the global SVN version info
        applicationVersionString = getApplicationVersion();
      }
      catch (InitializationException ex) {
        System.err.println("Could not locate version file. Aborting.");
        return VERSION_FILE_NOT_FOUND;
      }
    
      // Check that the parameters we got are formally correct
      status = checkParameters(args);
    
      // Check if we could find it
      if (status != 0)
      {
        // We could not locate the properties file
        return status;
      }
      
      // Get the properties file
      URL propertiesFileName = getPropertiesFileName(args);

      // Check if we could find it
      if (propertiesFileName == null)
      {
        // We could not locate the properties file
        return status;
      }
      
      // Load it
      try
      {
        PropertyUtils.getPropertyUtils().loadPropertiesXML(propertiesFileName,"FWProps");
      }
      catch (InitializationException ex) {
        System.err.println("Error loading properties file <"+propertiesFileName+">. Aborting.");
        return PROPERTIES_FILE_NOT_FOUND;
      }
    
    // Prepare the framework environment - Get the default logger until we
    // read the properties file to get the correct logger
    loadDefaultLogger();
      
    // Start the dialogue with the user
    System.out.println("");
    System.out.println("--------------------------------------------------------");
    System.out.println("  " + getApplicationVersionString());
    System.out.println("  Copyright OpenRate Project, 2005-2013");
    System.out.println("--------------------------------------------------------");

    // Start up the framework and load the resources etc
    if (startupFramework() == true)
    {
      // Get a list of the pipelines that we are working with from the client
      // Manager instance
      pipelineList = PropertyUtils.getPropertyUtils().getGenericNameList("PipelineList");

      // Now that we have finished our internal initialisation, create the
      // pipelines
      Iterator<String> pipelineIterator = pipelineList.iterator();

      while (pipelineIterator.hasNext())
      {
        tmpPipelineToCreate = pipelineIterator.next();
        System.out.println("Creating pipeline:<" + tmpPipelineToCreate + ">");
        if (createPipeline(tmpPipelineToCreate) == false)
        {
          initError = true;

          // don't need to carry on
          break;
        }
      }

      // ************************* Execution Block *******************************
      if (initError == false)
      {
        // Attach shutdown hook 
        appl.attachShutDownHook();
        
        // run the application, run() exits on closedown
        System.out.println("Running...");
      }
      else
      {
        System.err.println("Error during framework startup. See Error Log for details. Aborting.");
        status = FATAL_EXCEPTION;
      }
    }
    else
    {
      // Dump the error log
      checkFrameworkExceptions();
      
      System.err.println("Error during framework startup. See Error Log for details. Aborting.");
      status = FATAL_EXCEPTION;
    }

     return status;
  }

  /**
   * Finalises the close down of the OpenRate application. Primarily here
   * so that the OpenRate core can be embedded in other applications.
   */
  public void finaliseApplication()
  {
    // Close down any resources or modules gracefully
    System.out.println("Shutting down...");

    // put a message into the log
    if( getFwLog() != null){
      getFwLog().info(":::: OpenRate Stopped ::::");
    }
    
    // clean up resources and logs
    cleanup();
    
    System.out.println("Finished");
    System.out.println("---------------------------------------------------");
    
    // clean up the instance
    appl = null;
  }

  /**
   * Start application. Initialize any common framework components,
   * and then call the abstract run() method to allow sub-classes to
   * do the real processing.
   *
   * We pass arguments to define the configuration that we are using.
   *
   * This is a top level method, in which exception handling must be performed.
   *
   * @param args The arguments to pass to the framework
   * @return true if the startup went OK, otherwise false
   */
  public boolean startupFramework()
  {
    try
    {
      // Get a default logger just in case - we will overwrite this later
      setFwLog(LogUtil.getLogUtil().getDefaultLogger());
      getFwLog().info("Set default logger");
      
      // Get the (completely useless but informative) application Name, and
      // well, inform the user
      //ApplicationName = PropertyUtils.getPropertyValueDef(resources,
      applicationName = PropertyUtils.getPropertyUtils().getPropertyValueDef("Application", "Undefined");
      System.out.println("Initialising application <" + applicationName + ">");

      // Initialse the FWLog. This should be done before anything else so that
      // we are able to get a record of all the messages that occur
      System.out.println("Intialising log resource...");
      resourceContext = new ResourceContext();

      // Initialize the AstractLogger from the ResourceContext. This means that we will
      // have access to the logging functionality in time for the resource
      // and module intialisation

      // if we fail to get the logger, there's not much we can do other than
      // pass this up to the loader, because we can't log it...
      loadResources(true);

      // Intialise the logger from the resource context. This overwrites the
      // default logger that was set up previously and rewire the logger in the
      // resource context which was forced to use the default logger up until
      // now
      setFwLog(LogUtil.getLogUtil().getLogger("Framework"));
      getFwLog().info(":::: OpenRate Started ::::");

      // Initalise the error log - this is intended to log all stack trace
      // type events, keeping the main output as clean and businesslike as possible.
      setErrorLog(LogUtil.getLogUtil().getLogger("ErrorLog"));

      // Initalise the stats log - this is intended to log all statistics
      // information, dealing with performance and profiling
      setStatsLog(LogUtil.getLogUtil().getLogger("Statistics"));

      // Log our version
      getFwLog().info("OpenRate Build " + getApplicationVersionString());

      // Get the sequential loading flag
      sequentialLoading = Boolean.valueOf(PropertyUtils.getPropertyUtils().getFrameworkPropertyValueDef(SERVICE_SEQUENTIAL_LOADING, "true"));

      // Intialise the other resources in the framework so that these are
      // available for the module initialisation
      System.out.println("Initialising other resources...");
      loadResources(false);

      // register ourself as an event handler client
      registerClientManager();
    }
    catch (InitializationException iex)
    {
      // last resort error handler
      frameworkExceptionHandler.reportException(iex);
    }
    catch (Exception ex)
    {
      // last resort error handler
      message = "Unexpected Exception starting up Framework";
      frameworkExceptionHandler.reportException(new InitializationException(message,ex,getSymbolicName()));
    }
    catch (Throwable th)
    {
      // even laster resort error handler
      message = "Unexpected Throwable starting up Framework";
      frameworkExceptionHandler.reportException(new InitializationException(message,getSymbolicName(),true,true,th));
    }

    // If we had an error starting up, tell the rest of the loading
    if (getHandler().hasError())
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * Check if any exceptions occurred and deal with them
  */
  public boolean checkFrameworkExceptions()
  {
    // check if there have been any errors in the threads, and if there
    // have, pass the exception up
    if (getHandler().hasError())
    {
      // Failure occurred, propagate the error
      getFwLog().error("Exception thrown in module <" + getSymbolicName() + ">");

      // report the exceptions to the ErrorLog
      Iterator<Exception> excList = getHandler().getExceptionList().iterator();

      while (excList.hasNext())
      {
        // Handle the exceptions in order
        Exception tmpException = excList.next();
        handleOpenRateException(tmpException);
      }

      // Clear down the list
      getHandler().clearExceptions();

      return true;
    }
    else
    {
      // No exceptions, continue
      return false;
    }
  }

 /**
  * Handle, format and report top level initialisation exceptions.
  *
  * @param ex
  */
  public void handleOpenRateException(Exception ex)
  {
    String Message;

    // see if we have an Error Log - if so report to it
    if (ex instanceof InitializationException)
    {
      Message = "InitializationException thrown. Message <" + ex.getMessage() + ">";
    }
    else if (ex instanceof ProcessingException)
    {
      Message = "ProcessingException thrown. Message <" + ex.getMessage() + ">";
    }
    else
    {
      Message = "Exception thrown. Message <" + ex.getMessage() + ">";
    }

    if (getErrorLog() != null)
    {
      // We have an Error Log initialised, so use it
      getErrorLog().fatal(Message,ex);
      
      // Also send to system out
      System.err.println(Message);
    }
    else
    {
      // else we have no Error, so give a minimum of feedback on the console
      System.err.println("No Error Log defined! Please set the logging properties correctly!");
      System.err.println(Message);
    }

    // see if we have the Framework log, and if yes, log to it
    if (getFwLog() != null)
    {
      // We have a FWLog initialised, so use it
      getFwLog().fatal(Message);
    }
    else
    {
      // else we have no FWLog, so give a minimum of feedback on the console
      System.err.println(Message);
    }
  }

  /**
  * Load the default logger for the startup procedure
  */
  private void loadDefaultLogger()
  {
    setFwLog(LogUtil.getLogUtil().getDefaultLogger());
  }

 /**
  * Create the pipeline for the defined pipeline name, creating the input and
  * output adapters, the processing modules in multi threaded mode.
  *
  * This is a top level method, in which exception handling must be performed.
  *
  * @param pipelineName The name of the pipeline we are creating
  * @return true if the pipe was created OK, otherwise false
  */
  public boolean createPipeline(String pipelineName)
  {
    boolean retVal = true;

    tmpPipeline = new Pipeline();
    pipelineMap.put(pipelineName, tmpPipeline);
    
    // Add the pipeline to the threadgroup
    
    try
    {
      tmpPipeline.init(pipelineName);
    }
    catch (Exception ex)
    {
      handleOpenRateException(ex);
      System.err.println("Error during startup in pipeline <" + pipelineName + ">, message <" + ex.getMessage() + ">. See Error Log for more details.");
      retVal = false;
    }

    return retVal;
  }

 /**
  * Run the pipelines that we have created. Visit each in turn and run it.
  * We monitor the pipeline status in a low cost loop, and when all pipes have
  * stopped, we exit the application.
  *
  * This loop performs the following tasks (initialisation):
  *  - check if there are any pipelines defined. If not, close down.
  *  - create the pipeline threads and start them
  *  - get a list of resources where sync point handling will be monitored
  *
  * And while running, performs these tasks:
  *  - runs until all pipes have stopped
  *  - Check for a sync point
  *  - If a sync point is found, manage the processing
  *  - When a sync point is reached, process any semaphores
  *  - Do any auto loading that is needed
  *  - Go back to sleep again
  *
  * This is a top level method, in which exception handling must be performed.
  */
  @Override
  public void run()
  {
    String       tmpPipeName;
    String       tmpResourceName;
    int          Index;
    Thread       tmpPipeThread;
    ISyncPoint[] tmpPipeList;
    ISyncPoint   tmpResource;
    ISyncPoint[] tmpResourceList;
    int          pipesActive;
    int          resourcesManaged = 0;

    try
    {
      pipelineSet = pipelineMap.keySet();
      pipelineIter = pipelineSet.iterator();
      tmpPipeList = new IPipeline[pipelineSet.size()];
      pipelineThreadGroup = new ThreadGroup("Pipelines");

      // initialise the variables for managing the sync point status for recources
      resourceSet = syncPointResourceMap.keySet();
      resourceIter = resourceSet.iterator();
      tmpResourceList = new ISyncPoint[resourceSet.size()];

      // initialise the ECI link
      ECI = (EventHandler) resourceContext.get(EventHandler.RESOURCE_KEY);

      // initialise the cache factory link
      cacheFactory = (CacheFactory) resourceContext.get(CacheFactory.RESOURCE_KEY);

      if (!pipelineIter.hasNext())
      {
        // Seems the user has not defined any pipelines. Abort
        getFwLog().error("No Pipelines defined. Closing down...");
      }
      else
      {
        // Prepare the main processing loop
        Index = 0;
        while (pipelineIter.hasNext())
        {
          tmpPipeName = pipelineIter.next();
          tmpPipeline = pipelineMap.get(tmpPipeName);
          tmpPipeList[Index] = tmpPipeline;

          // Keep track of the pipeline threads we have
          tmpPipeThread = new Thread(pipelineThreadGroup, tmpPipeline, "Pipeline-" + tmpPipeName);

          // Start the thread - the pipeline threads are free running
          tmpPipeThread.start();
          Index++;
        }

        // Get the list of resources that need managing for sync points
        Index = 0;
        while (resourceIter.hasNext())
        {
          tmpResourceName = resourceIter.next();
          if (syncPointResourceMap.get(tmpResourceName) instanceof ISyncPoint)
          {
            tmpResourceList[resourcesManaged] = (ISyncPoint)syncPointResourceMap.get(tmpResourceName);
            Index++;

            // Because not all of the resources need managing, we have to
            // keep track of only those that do
            resourcesManaged++;
          }
        }

        // Check the state of the pipelines
        pipesActive = 1;
        syncRequested = false;

        // ********************** main processing loop **************************
        while (pipesActive > 0)
        {
          syncReached = true;
          syncFinished = true;

          for (Index = 0; Index < tmpPipeList.length; Index++)
          {
            // Find out if one of the pipes is requesting a synch point
            // This means that the other pipelines will be requested to stop
            // accepting transactions, until all pipes are stopped, at which
            // point the pipes will be ordered to do any pending event
            // processing
            tmpPipeline = (IPipeline)tmpPipeList[Index];
            syncStatus = tmpPipeline.getSyncStatus();

            // see if we have finished the sync - this is when all
            // pipes report back that they have status SYNC_STATUS_RESTARTING
            syncFinished &= (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_FINISHED);

            // see if we have reached the sync point - this is when all
            // pipes report back that they have status SYNC_STATUS_SYNC_PROCESSING
            syncReached &= (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_REACHED);

            if (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_FLAGGED)
            {
              // Mark that we are requesting a synch
              syncRequested = true;
              getFwLog().info("Sync point requested by pipeline <" + tmpPipeline.getSymbolicName() + ">");

              // Confirm that we have got the message
              tmpPipeline.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_REQUESTED);
              syncStatus = ISyncPoint.SYNC_STATUS_SYNC_REQUESTED;
            }

            // propogate the sync command - this might take two cycles to
            // complete
            if ((syncStatus == 0) & (syncRequested))
            {
              // propogate the sync message
              tmpPipeline.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_REQUESTED);
              syncStatus = ISyncPoint.SYNC_STATUS_SYNC_REQUESTED;
              System.out.println("Initiating Sync Processing...");
            }

            // see if we had an abort - this is caused by an exception in the pipe
            // and we have configured the pipe to stop on exception
            if(tmpPipeline.isAborted())
            {
              // bring the rest of the framework down
              stopAllPipelines();
            }
          }
          
          // Count the number of pipes active right now
          pipesActive = pipelineThreadGroup.activeCount();
          //pipelineThreadGroup.list();

          // Update the framework active flag
          pipelinesActive = (pipesActive > 0);

          // Check if we have reached the synch point
          if (syncReached)
          {
            for (Index = 0; Index < tmpPipeList.length; Index++)
            {
              // Update the pipe status
              tmpPipeline = (IPipeline)tmpPipeList[Index];

              // start the synch processing
              tmpPipeline.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_PROCESSING);
            }
          }

          // Check if we are clearing down a synch point
          if (syncFinished)
          {
            for (Index = 0; Index < tmpPipeList.length; Index++)
            {
              // Update the pipe status
              tmpPipeline = (IPipeline)tmpPipeList[Index];

              // start the record processing
              tmpPipeline.setSyncStatus(ISyncPoint.SYNC_STATUS_NORMAL_RUN);
            }

            // clear down the request
            syncRequested = false;
            syncStatus = ISyncPoint.SYNC_STATUS_NORMAL_RUN;
            getFwLog().debug("Running...");
            System.out.println("Running...");
          }

          // See if any of the resources is requesting a sync point
          for (Index = 0; Index < resourcesManaged; Index++)
          {
            tmpResource = tmpResourceList[Index];

            syncStatus = tmpResource.getSyncStatus();
            if (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_FLAGGED)
            {
              // Mark that we are requesting a synch
              syncRequested = true;
                            getFwLog().info("Sync point requested by resource <" + tmpResource.getSymbolicName() + ">");

              // Confirm that we have got the message
              tmpResource.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_REQUESTED);
              syncStatus = ISyncPoint.SYNC_STATUS_SYNC_REQUESTED;
            }

            // propogate the sync command - this might take two cycles to
            // complete
            if ((syncStatus == ISyncPoint.SYNC_STATUS_NORMAL_RUN) & (syncRequested))
            {
              // propogate the sync message
              tmpResource.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_REQUESTED);
              syncStatus = ISyncPoint.SYNC_STATUS_SYNC_REQUESTED;
            }

            // Check if we have reached the synch point
            if (syncReached)
            {
              // start the processing
              tmpResource.setSyncStatus(ISyncPoint.SYNC_STATUS_SYNC_PROCESSING);
              syncStatus = ISyncPoint.SYNC_STATUS_SYNC_PROCESSING;
            }

            // Check if we are clearing down a synch point
            if (syncFinished)
            {
              // finish the processing
              tmpResource.setSyncStatus(ISyncPoint.SYNC_STATUS_NORMAL_RUN);

              // clear down the request
              syncRequested = false;
              syncStatus = ISyncPoint.SYNC_STATUS_NORMAL_RUN;
            }

            // see if we have reached the sync point - this is when all
            // pipes report back that they have status SYNC_STATUS_SYNC_PROCESSING
            syncReached &= (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_REACHED);

            // see if we have finished the sync - this is when all
            // pipes report back that they have status SYNC_STATUS_RESTARTING
            syncFinished &= (syncStatus == ISyncPoint.SYNC_STATUS_SYNC_FINISHED);
          }

          // FWLog some stuff
          if (syncReached)
          {
            getFwLog().info("Sync point reached");
          }

          if (syncFinished)
          {
            getFwLog().info("Sync point finished");
          }

          // Process any semaphore file ther is to process
          ECI.processSemaphoreFile();

          // Manage the auto reload
          if ((cacheFactory == null) == false)
          {
            cacheFactory.updateAutoReload();
          }

          // Go into a low processor cost loop to monitor the pipes. When the last
          // stops, close down
          if (pipelinesActive)
          {
            try
            {
              Thread.sleep(1000);
            }
            catch (InterruptedException ex)
            {
              // This can't happen, so NOP
            }
          }
        }
        
        // Destroy the threadgroup that held the pipes
        pipelineThreadGroup.destroy();
        pipelineThreadGroup = null;

        // Log that we have finished with the pipes
        getFwLog().debug("Stopped all pipes");
      }
    }
    catch (Exception ex)
    {
      handleOpenRateException(ex);
    }
  }

  /**
   * Perform any cleanup required before the application exits.
   */
  protected void closePipelines()
  {
    if (pipelineSet != null)
    {
      pipelineIter = pipelineSet.iterator();

      while (pipelineIter.hasNext())
      {
        tmpPipeline = pipelineMap.get(pipelineIter.next());
        System.out.println("Closing pipeline <" + tmpPipeline.getSymbolicName() + ">");
        tmpPipeline.shutdownPipeline();
        System.out.println("Destroying pipeline <" + tmpPipeline.getSymbolicName() + ">");
        tmpPipeline.cleanupPipeline();
      }
    }
  }

  /**
   * Perform any cleanup required by the application once the processing has
   * been terminated and the pipelines have been closed.
   *
   * This method will also call the doCleanup() abstract method to allow
   * concrete application classes to shutdown gracefully.
   */
  public final void cleanup()
  {
    // Close resources
    closeResources();
    
    // Clear out the exception handler
    frameworkExceptionHandler.clearExceptions();
    
    // Deallocate the properties object
    PropertyUtils.closePropertyUtils();
    
    
  }

  /**
   * Stop all pipelines in the framework. This method gives the command to stop
   * the pipelines, but the completion of the stop will happen some time later
   * (pipelines might be in the middle of processing a transaction, and they
   * will finish this before stopping).
   */
  public final void stopAllPipelines()
  {
    // Set the shutdown flag
    getFwLog().info("Shutdown command received. Shutting down pipelines as soon as possible.");
    pipelineSet = pipelineMap.keySet();
    pipelineIter = pipelineSet.iterator();

    // Iterate through all the pipelines we have and tell them to stop
    while (pipelineIter.hasNext())
    {
      tmpPipeline = pipelineMap.get(pipelineIter.next());

      // markForShutdown tells the pipe to stop accepting new transactions
      // and mark that a stop is pending. The pipeline will stop when all
      // transactions are finished.
      tmpPipeline.markForShutdown();
    }
  }

  /**
   * Shutdown hook. Attaching shutdown hook to the Application, so that we
   * gracefully stop when the process is interrupted.
   */
    private void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (shutdownHookCalls == 0)
                {
                    // Hook triggered, starting shutdown process
                    // Stop all pipes
                    stopAllPipelines();
                    
                    getFwLog().info("Shutdown command called");
                }

                // increment the shutdown hook
                shutdownHookCalls++;
                
                // If the person shutting down is too insistent, we just abort
                if (shutdownHookCalls == 3)
                {
                  getFwLog().error("Hard shutdown forced by 3 kills. Aborting.");
                  System.exit(-99);
                }
            }
        });
    }

 /**
  * Load the framework resources. Can be called in two different ways:
  * 1) To load the logger only. Clearly the logger is very useful when loading
  *    other resources, so we come in and do a quick load just of the logger
  *    if LoadLoggerOnly is set to true
  * 2) Load all the other resources: Once we have a logger, we load all the
  *    remaining resources. This skips the logger resource
  *
  * @param loggerOnly True if we are loading the logger, otherwise false
  * @throws InitializationException
  */
  private void loadResources(boolean loggerOnly) throws InitializationException
  {
    IEventInterface   tmpEventIntf;
    String            tmpResourceName = null;
    String            tmpResourceClassName;
    Class<?>          resourceClass;
    IResource         resource;
    ArrayList<String> tmpResourceNameList;
    ThreadGroup       tmpGrpResource;

    // Get the resource list
    tmpResourceNameList = PropertyUtils.getPropertyUtils().getGenericNameList("Resource");

    if (tmpResourceNameList == null || tmpResourceNameList.isEmpty())
    {
      frameworkExceptionHandler.reportException(new InitializationException("No resources defined. Aborting.",getSymbolicName()));
      
      // we are done
      return;
    }

    // Check if we got the Framework Log resource
    if ((tmpResourceNameList.contains(AbstractLogFactory.RESOURCE_KEY) == false) & loggerOnly)
    {
      // No FWLog resource.
      frameworkExceptionHandler.reportException(new InitializationException("Log resource not found. Framework aborting.",getSymbolicName()));
      return;
    }

    // Check if we got the ECI resource
    if ((tmpResourceNameList.contains(EventHandler.RESOURCE_KEY) == false) & (!loggerOnly))
    {
      // No FWLog resource.
      frameworkExceptionHandler.reportException(new InitializationException("ECI resource not found. Framework aborting.",getSymbolicName()));
      return;
    }

    try
    {
      // Iterate through the resources and create them
      resourceIter = tmpResourceNameList.iterator();

      // The thread group for holding the resources if we use multi-threaded loading
      tmpGrpResource = new ThreadGroup("Resources");

      while (resourceIter.hasNext() && (getHandler().hasError() == false))
      {
        // Get the next resource
        tmpResourceName = resourceIter.next();

        // If we are loading the log factory, just load the first, else load all but the first
        if (((loggerOnly) & (tmpResourceName.equalsIgnoreCase(AbstractLogFactory.RESOURCE_KEY))) |
            ((!loggerOnly) & (tmpResourceName.equalsIgnoreCase(AbstractLogFactory.RESOURCE_KEY) == false)))
        {
          // This is where we will launch the resources as separate classes if
          // necessary
          tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(tmpResourceName,"ClassName");
          resourceClass = Class.forName(tmpResourceClassName);
          resource = (IResource)resourceClass.newInstance();

          System.out.println("  Initialising Resource <" + tmpResourceName + ">...");

          // see if we are using sequential or threaded loading
          // (Sequential is easier to use, but clearly takes longer to load)
          if (sequentialLoading)
          {
            // perform initialisation
            resource.init(tmpResourceName);

            // register the created resource with the context so we can find it later
            resourceContext.register(tmpResourceName, resource);

            // Now see if we have to register with the config manager
            if (resource instanceof IEventInterface)
            {
              // Register
              tmpEventIntf = (IEventInterface)resource;
              tmpEventIntf.registerClientManager();

              // Add the resource to the list of the resources that can call for
              // a sync point
              syncPointResourceMap.put(tmpResourceName, tmpEventIntf);
            }
          }
          else
          {
            // Multi-threaded loading - create a new thread for each resource
            ResourceLoaderThread resourceLoaderThread = new ResourceLoaderThread(tmpGrpResource,tmpResourceName);

            // Set up the thread with the information it will need
            resourceLoaderThread.setResource(resource);
            resourceLoaderThread.setResourceName(tmpResourceName);
            resourceLoaderThread.setResourceContext(resourceContext);
            resourceLoaderThread.setsyncPointResourceMap(syncPointResourceMap);

            // Launch it
            resourceLoaderThread.start();
          }
        }
      }

      // Close down the thread group if we have finished using it (all threads finished)
      while ( tmpGrpResource.activeCount() > 0)
      {
        Thread.sleep(1000);
      }

      // Destroy the thread group
      tmpGrpResource.destroy();
    }
    catch (ClassNotFoundException ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("ClassNotFoundException: " +
                                        "Class not found for Resource <" +
                                        tmpResourceName + ">", ex,getSymbolicName()));
    }
    catch (InstantiationException ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("InstantiationException: " +
                                        "No default constructor found for for Resource <" +
                                        tmpResourceName + ">", ex,getSymbolicName()));
    }
    catch (IllegalAccessException ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("IllegalAccessException: " +
                                        "Check that the Resource <" +
                                        tmpResourceName +
                                        "> has a public default constructor.",
                                        ex,getSymbolicName()));
    }
    catch (ClassCastException ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("ClassCastException: " +
                                        "Class identified as Resource <" +
                                          tmpResourceName +
                                        "> does not implement " +
                                        "Resource interface.", ex,getSymbolicName()));
    }
    catch (NullPointerException ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("Null pointer exception creating Resource <" +
                                        tmpResourceName + ">", ex,getSymbolicName()));
    }
    catch (InitializationException ex)
    {
      // We don't need to nest/interpret this exception, just report it
      frameworkExceptionHandler.reportException(ex);
    }
    catch (OutOfMemoryError ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("Out of memory creating <" + tmpResourceName + ">",getSymbolicName(),true,true,ex));
    }
    catch (Exception ex)
    {
      frameworkExceptionHandler.reportException(new InitializationException("Unexpected exception creating Resource <" +
                                        tmpResourceName + ">", ex,getSymbolicName()));
    }
    catch (Throwable th)
    {
      frameworkExceptionHandler.reportException(new InitializationException("Unexpected exception creating Resource <" +
                                        tmpResourceName + ">",getSymbolicName(),true,true,th));
    }
  }

  /**
  * Unload the framework resources, using the priority order defined in the
  * properties.
  */
  private void closeResources()
  {
    // shutdown any configured resources via ResourceContext.
    System.out.println("Closing resources...");

    resourceContext.cleanup();
    
    // Sometimes we will have to wait for some resources to finish
    while(resourceContext.isActive())
    {
      getFwLog().debug("Waiting 100mS for the resource context to stop");
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException ex) {
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

  /**
  * Register ourselves with the event handler so that we can stop the
  * framework gracefully.
   *
   */
  @Override
  public void registerClientManager() throws InitializationException
  {
    IEventInterface tmpEventIntf;

    //Register this Client
    ClientManager.getClientManager().registerClient("Framework", getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(),SERVICE_FRAMEWORK_STOP,  ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(),SERVICE_SYNC_STATUS,     ClientManager.PARAM_NONE);

    // Register each of the pipelines' services
    pipelineSet = pipelineMap.keySet();
    pipelineIter = pipelineSet.iterator();

    while (pipelineIter.hasNext())
    {
      tmpEventIntf = (IEventInterface)pipelineMap.get(pipelineIter.next());

      // Register the pipeline with the manager. The pipeline will register
      // under its own name, so we pass null
      tmpEventIntf.registerClientManager();
    }
  }

 /**
  * process control events.
  *
  * @param Command The command to process
  * @param Init True if we are during framework init
  * @param Parameter The parameter to process for the command
  * @return The status of the processing
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_FRAMEWORK_STOP))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        stopAllPipelines();
      }
      else
      {
        return "false";
      }

      ResultCode = 0;
    }

    if (Command.equalsIgnoreCase(SERVICE_SYNC_STATUS))
    {
      return Integer.toString(syncStatus);
    }

    if (ResultCode == 0)
    {
            getFwLog().debug(LogUtil.LogECIFWCommand(Command, Parameter));

      return "OK";
    }
    else
    {
      return "Error: Command not understood.";
    }
  }

 /**
  * return the symbolic name
  *
  * @return The symbolic name for this plug in
  */
  public String getSymbolicName()
  {
      return symbolicName;
  }

 /**
  * set the symbolic name
  *
  * @param Name The new symbolic name for this plug in
  */
  public void setSymbolicName(String Name)
  {
      symbolicName=Name;
  }

 /**
  * Create the OpenRate application instance
  * 
  * @return the instance
  */
  public static OpenRate getApplicationInstance() {
    if (appl == null)
    {
      appl = new OpenRate();
    }
    
    return appl;
  }
    
 /**
  * Get the framework exception handler.
  *
  * @return The framework exception handler
  */
  public static ILogger getOpenRateFrameworkLog()
  {
    return appl.getFwLog();
  }

 /**
  * Get the framework exception handler.
  *
  * @return The framework exception handler
  */
  public static ILogger getOpenRateErrorLog()
  {
    return appl.getErrorLog();
  }

 /**
  * Get the framework exception handler.
  *
  * @return The framework exception handler
  */
  public static ILogger getOpenRateStatsLog()
  {
    return appl.getStatsLog();
  }

 /**
  * Get the framework exception handler.
  *
  * @return The framework exception handler
  */
  public static ExceptionHandler getFrameworkExceptionHandler()
  {
    return appl.getHandler();
  }

  /**
   * Return the Framework Exception handler.
   * 
   * @return the handler
   */
  public ExceptionHandler getHandler() {
    return frameworkExceptionHandler;
  }

  /**
   * @param handler the handler to set
   */
  public void setHandler(ExceptionHandler handler) {
    this.frameworkExceptionHandler = handler;
  }

    /**
     * @return the fwLog
     */
    public ILogger getFwLog() {
        return fwLog;
    }

    /**
     * @param fwLog the fwLog to set
     */
    public void setFwLog(ILogger fwLog) {
        this.fwLog = fwLog;
    }

    /**
     * @return the errorLog
     */
    public ILogger getErrorLog() {
        return errorLog;
    }

    /**
     * @param errorLog the errorLog to set
     */
    public void setErrorLog(ILogger errorLog) {
        this.errorLog = errorLog;
    }

    /**
     * @return the statsLog
     */
    public ILogger getStatsLog() {
        return statsLog;
    }

    /**
     * @param statsLog the statsLog to set
     */
    public void setStatsLog(ILogger statsLog) {
        this.statsLog = statsLog;
    }
    
   /**
    * Get the reference to a pipeline.
    * 
    * @param pipename The pipeline to get
    * @return The pipeline reference
    */
    public static IPipeline getPipeline(String pipename)
    {
      return pipelineMap.get(pipename);
    }

   /**
    * Insert a pipeline into the map. Used to unit tests, where we want to
    * concentrate on the module we are testing. Normally the pipeline map is
    * populated by the framework startup.
    * 
    * @param pipename The pipeline to get
    * @return The pipeline reference
    */
    public static void setPipeline(String pipename, IPipeline pipeline)
    {
      pipelineMap.put(pipename,pipeline);
    }

    /**
     * Returns the state of the processing 
     * @return the frameworkActive
     */
    public boolean isFrameworkActive() {
        return pipelinesActive;
    }
}
