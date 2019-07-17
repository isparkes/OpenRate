
package OpenRate.resource;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.IDBDataSource;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import javax.sql.DataSource;

/**
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Data_Source_Manager'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 * Factory for creating DataSources that pool database connections. Encapsulates
 * creation of DataSource so that user deals with DataSource objects in an
 * entirely standard way. The DataSource returned is a 100% compliant DataSource
 * that pool DB connections for the user. In addition, the factory provides a
 * static reference to the DataSources so that only 1 will be created per JVM.
 * Multiple DBs can be connected to by providing different ResourceBundle
 * filenames to the getBundle() method. The filename makes the bundle unique.
 *
 */
public class DataSourceFactory implements IResource, IEventInterface {

  // This is the symbolic name of the resource

  private String symbolicName;

  /**
   * the configuration key used by the ResourceContext to look for & return the
   * configured DataSourceFactory
   */
  public static final String RESOURCE_KEY = "DataSourceFactory";

  /**
   * List of Services that this Client supports
   */
  private final static String SERVICE_STATUS_KEY = "Status";

  // for handling thread safety
  private static final Object lock = new Object();
  private static final HashMap<String, DataSource> sources = new HashMap<>();
  private IDBDataSource builder = null;

  /**
   * Default Constructor
   */
  public DataSourceFactory() {
    super();
  }

  /**
   * Initialize the data source factory
   *
   * @param ResourceName The resource name to initialise
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String ResourceName) throws InitializationException {
    String tmpSymbolicName;
    String builderClassName = "";
    Class<?> builderClass;
    String tmpDataSourceName = "";
    ArrayList<String> DataSourceList;

    // Set the symbolic name
    symbolicName = ResourceName;

    // Register with the event manager
    registerClientManager();

    try {
      // Get the name of the module
      tmpSymbolicName = ResourceName;

      if (!tmpSymbolicName.equalsIgnoreCase(RESOURCE_KEY)) {
        // we are relying on this name to be able to find the resource
        // later, so stop if it is not right
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("DataSourceFactory ModuleName should be <" + RESOURCE_KEY + ">", getSymbolicName()));
      }

      // Get the builder class
      builderClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName, "DataSourceBuilder.ClassName", "None");

      if (builderClassName.equals("None")) {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("No Data Source Builder Class defined", getSymbolicName()));
      }

      builderClass = Class.forName(builderClassName);
      this.builder = (IDBDataSource) builderClass.newInstance();
    } catch (ClassNotFoundException ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Data source builder class not found <" + builderClassName + ">", ex, getSymbolicName()));
    } catch (InstantiationException ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not instantiate data source builder class <" + builderClassName + ">", ex, getSymbolicName()));
    } catch (IllegalAccessException ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Error accessing data source builder class <" + builderClassName + ">", ex, getSymbolicName()));
    } catch (NoClassDefFoundError ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not find data source builder class <" + builderClassName + ">", getSymbolicName(), false, true, ex));
    }

    // Now go and get all the data sources that have to be created
    DataSourceList = PropertyUtils.getPropertyUtils().getGenericNameList("Resource.DataSourceFactory.DataSource");

    Iterator<String> DataSourceIter = DataSourceList.iterator();

    try {
      while (DataSourceIter.hasNext()) {
        tmpDataSourceName = DataSourceIter.next();

        /* only initialize the data source 1x. */
        if (sources.get(tmpDataSourceName) == null) {
          sources.put(tmpDataSourceName, getDataSourceBuilder().getDataSource(ResourceName, tmpDataSourceName));
          OpenRate.getOpenRateFrameworkLog().info("Successfully created DataSource <" + tmpDataSourceName + ">");
          System.out.println("    Created DataSource <" + tmpDataSourceName + ">");
        }
      }
    } catch (NoClassDefFoundError ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not find data source class for data source <"
              + tmpDataSourceName + ">", getSymbolicName(), false, true, ex));
    } catch (InitializationException ex) {
      OpenRate.getFrameworkExceptionHandler().reportException(ex);
    }
  }

  /**
   * Return the key set of the sources we are using
   *
   * @return Key get of the data sources
   */
  public Collection<String> keySet() {
    return sources.keySet();
  }

  /**
   * retrieve a data source by name.
   *
   * @param dsName The name of the data source to get
   * @return the data source
   */
  public DataSource getDataSource(String dsName) {
    return sources.get(dsName);
  }

  /**
   * Cleanup the object pool that provides the connections.
   */
  @Override
  public void close() {
    sources.clear();
    OpenRate.getOpenRateFrameworkLog().debug("Shutdown Data Source Factory");
  }

  /**
   * Returns the builder.
   *
   * @return The builder object
   */
  public IDBDataSource getDataSourceBuilder() {
    return this.builder;
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

  /**
   * processControlEvent is the method that will be called when an event is
   * received for a module that has registered itself as a client of the
   * External Control Interface
   *
   * @param Command - command that is understand by the client module
   * @param Init - we are performing initial configuration if true
   * @param Parameter - parameter for the command
   * @return The result string of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter) {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_STATUS_KEY)) {
      //getDataSourceBuilder().printStatus();
      ResultCode = 0;
    }

    if (ResultCode == 0) {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), symbolicName, Command, Parameter));

      return "OK";
    } else {
      return "Command Not Understood";
    }
  }

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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATUS_KEY, ClientManager.PARAM_DYNAMIC);
  }
}
