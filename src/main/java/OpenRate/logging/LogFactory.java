
package OpenRate.logging;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;

import java.io.File;
import java.net.URL;
import java.util.HashMap;

import org.apache.logging.log4j.core.config.Configurator;

/**
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Log'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 * The LogFactory provides access to the logging mechanism, based on Log4J. It
 * provides a map to the various log objects that can exist in the system, and
 * this can provide access to them.
 */
public class LogFactory extends AbstractLogFactory implements IEventInterface {

  // cache Categories
  private static final HashMap<String, Log4JLogger> LogStreams = new HashMap<>();

  // The properties we are working from
  private static String log4j_properties;

  /*
   * Inititialization flag.
   * Only allows the logging factory to be initialized once per
   * application. If you run multiple processes within the same
   * application you would attempt to initialize the logfactory 3
   * times since it is a Resource and would be reloaded each time the
   * ResourceContext is re-initialized for each process. Ensure that this
   * doesn't happen by holding on to a static flag that ensures init()
   * only happens once.
   */
  private static boolean loaded = false;

  // List of Services that this Client supports
  private final static String SERVICE_RELOAD = "Reload";

  // This is the symbolic name of the resource
  private String symbolicName;

  /**
   * default constructor - protected
   */
  public LogFactory() {
    super();
  }

  /**
   * Perform whatever initialization is required of the resource. This method
   * should only be called once per application instance.
   *
   * @param ResourceName
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String ResourceName) throws InitializationException {
    /* in the case of multi-process applications, the init() may
     * be called more than once. Ensure that log4j only creates one file by
     * only configuring it one time per application.
     */
    if (!isLoaded()) {
      // Set the symbolic name
      symbolicName = ResourceName;

      if (!symbolicName.equalsIgnoreCase(RESOURCE_KEY)) {
        // we are relying on this name to be able to find the resource
        // later, so stop if it is not right
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Log ModuleName should be <" + RESOURCE_KEY + ">", getSymbolicName()));
      }

      // configure log4j if a log4j config file is provided in the
      // configuration. If not, ignore configuration on the assumption
      // that log4j will handle it internally.
      log4j_properties = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName, "Properties", "None");

      // See if we got a logger definition
      if (log4j_properties.equals("None")) {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Logger Configuration File <" + ResourceName + "> not defined in Logger resource", getSymbolicName()));
      }

      // Get the file from the classpath
      URL fqConfigFileName = getClass().getResource("/" + log4j_properties);

      // Does it exist?
      if (fqConfigFileName == null) {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not open Configuration File <" + fqConfigFileName + "> not defined in Logger resource", getSymbolicName()));
      } else {
        // Is it a file?
        if (new File(fqConfigFileName.getFile()).isFile() == false) {
          OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not open Configuration File <" + fqConfigFileName + "> not defined in Logger resource", getSymbolicName()));
        }

        if (log4j_properties.endsWith(".xml")) {
          // ToDo: Add configure and watch
          // use the XML model
          Configurator.initialize(symbolicName, fqConfigFileName.getPath());
        } else {
          // ToDo: Add configure and watch
          // use the traditional properties file model: Deprecated
          OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Properties file model is no longer supported. Convert to XML based model.", getSymbolicName()));
        }

        // log4j initialized.
        loaded = true;

        System.out.println("Logger initialised using configuration <" + fqConfigFileName.getFile() + ">");
      }
    } else {
      System.err.println("Logger already loaded");
    }
  }

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close() {
    if (isLoaded()) {
      LogStreams.clear();
      loaded = false;
    }
  }

  /**
   * Utility to return the reference to the logger resource
   *
   * @return the logger matching the given name
   */
  @Override
  public AstractLogger getLogger(String type) {
    Log4JLogger tmpLogger = new Log4JLogger(type);
    if (LogStreams.containsKey(type) == false) {
      LogStreams.put(type, tmpLogger);
    }

    return LogStreams.get(type);
  }

  /**
   * Returns the loaded status.
   *
   * @return true if loaded
   */
  private boolean isLoaded() {
    return loaded;
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * registerClientManager registers the client module to the ClientManager
   * class which manages all the client modules available in this OpenRate
   * Application.
   *
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
    //Register this Client
    ClientManager.getClientManager().registerClient("Resource", getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_RELOAD, ClientManager.PARAM_MANDATORY);
  }

  /**
   * processControlEvent is the event processing hook for the External Control
   * Interface (ECI). This allows interaction with the external world, for
   * example turning the dumping on and off.
   */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter) {

    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_RELOAD)) {
      if (Parameter.equalsIgnoreCase("true")) {
        ResultCode = 0;
      } else if (Parameter.equalsIgnoreCase("false")) {
        // Don't reload
        ResultCode = 0;
      } else if (Parameter.equalsIgnoreCase("")) {
        // return something that sounds meaningful
        return "false";
      }
    }

    // Currently this cannot handle any dynamic events
    if (ResultCode == 0) {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getSymbolicName(), Command, Parameter));
      return "OK";
    } else {
      return "Command Not Understood";
    }
  }

  /**
   * Return the resource symbolic name
   *
   * @return The symbolic name
   */
  @Override
  public String getSymbolicName() {
    return symbolicName;
  }
}
