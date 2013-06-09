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

package OpenRate.resource;

import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.IDBDataSource;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import javax.sql.DataSource;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Data_Source_Manager'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * Factory for creating DataSources that pool database connections.
 * Encapsulates creation of DataSource so that user deals with DataSource
 * objects in an entirely standard way. The DataSource returned is
 * a 100% compliant DataSource that pool DB connections for the user.
 * In addition, the factory provides a static reference to the
 * DataSources so that only 1 will be created per JVM. Multiple DBs
 * can be connected to by providing different ResourceBundle filenames
 * to the getBundle() method. The filename makes the bundle unique.
 *
 */
public class DataSourceFactory implements IResource, IEventInterface
{
  // Get access to the framework logger
  private ILogger FWLog = LogUtil.getLogUtil().getLogger("Framework");

  // This is the symbolic name of the resource
  private String SymbolicName;

  /**
   * the configuration key used by the ResourceContext to look for & return
   * the configured DataSourceFactory
   */
  public static final String RESOURCE_KEY = "DataSourceFactory";

  /**
   * List of Services that this Client supports
   */
  private final static String SERVICE_STATUS_KEY = "Status";

  // reference to the exception handler
  private ExceptionHandler handler;

  // for handling thread safety
  private static Object  lock    = new Object();
  private static HashMap<String, DataSource> sources = new HashMap<>();
  private IDBDataSource  builder = null;

  /**
   * Default Constructor
   */
  public DataSourceFactory()
  {
    super();
  }

 /**
  * Initialize the data source factory
  *
  * @param ResourceName The resource name to initialise
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    String tmpSymbolicName;
    String builderClassName = "";
    Class  builderClass;
    String tmpDataSourceName = "";
    ArrayList<String> DataSourceList;

    // Set the symbolic name
    SymbolicName = ResourceName;

    // Register with the event manager
    registerClientManager();

    try
    {
      // Get the name of the module
      tmpSymbolicName = ResourceName;

      if (!tmpSymbolicName.equalsIgnoreCase(RESOURCE_KEY))
      {
        // we are relying on this name to be able to find the resource
        // later, so stop if it is not right
        handler.reportException(new InitializationException("DataSourceFactory ModuleName should be <" + RESOURCE_KEY + ">"));
      }

      // Get the builder class
      builderClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"DataSourceBuilder.ClassName","None");

      if (builderClassName.equals("None"))
      {
        handler.reportException(new InitializationException("No Data Source Builder Class defined"));
      }

      builderClass = Class.forName(builderClassName);
      this.builder = (IDBDataSource) builderClass.newInstance();
    }
    catch (ClassNotFoundException cnfe)
    {
      handler.reportException(new InitializationException("Data source builder class not found <" + builderClassName + ">",cnfe));
    }
    catch (InstantiationException ie)
    {
      handler.reportException(new InitializationException("Could not instantiate data source builder class <" + builderClassName + ">",ie));
    }
    catch (IllegalAccessException iae)
    {
      handler.reportException(new InitializationException("Error accessing data source builder class <" + builderClassName + ">",iae));
    }
    catch (NoClassDefFoundError ncdfe)
    {
      handler.reportException(new InitializationException("Could not find data source builder class <" + builderClassName + ">",ncdfe));
    }

    // Now go and get all the data sources that have to be created
    DataSourceList = PropertyUtils.getPropertyUtils().getGenericNameList("Resource.DataSourceFactory.DataSource");

    Iterator<String> DataSourceIter = DataSourceList.iterator();

    try
    {
      while (DataSourceIter.hasNext())
      {
        tmpDataSourceName = DataSourceIter.next();

        /* only initialize the data source 1x. */
        if (sources.get(tmpDataSourceName) == null)
        {
          sources.put(tmpDataSourceName, getDataSourceBuilder().getDataSource(ResourceName,tmpDataSourceName));
          FWLog.info("Successfully created DataSource <" + tmpDataSourceName + ">");
          System.out.println("    Created DataSource <" + tmpDataSourceName + ">");
        }
      }
    }
    catch (NoClassDefFoundError ncdfe)
    {
      handler.reportException(new InitializationException("Could not find data source class for data source<" +
              tmpDataSourceName + ">",ncdfe));
    }
    catch (InitializationException ie)
    {
      handler.reportException(ie);
    }
  }

  /**
   * Return the key set of the sources we are using
   *
   * @return Key get of the data sources
   */
  public Collection<String> keySet()
  {
    return sources.keySet();
  }

  /**
   * retrieve a data source by name.
   *
   * @param dsName The name of the data source to get
   * @return the data source
   */
  public DataSource getDataSource(String dsName)
  {
    return sources.get(dsName);
  }

  /**
   * Cleanup the object pool that provides the connections.
   */
  @Override
  public void close()
  {
    FWLog.debug("Shutdown Data Source Factory");
  }

  /**
   * Returns the builder.
   *
   * @return The builder object
   */
  public IDBDataSource getDataSourceBuilder()
  {
    return this.builder;
  }

 /**
  * Return the resource symbolic name
  *
  * @return The symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return SymbolicName;
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
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_STATUS_KEY))
    {
      //getDataSourceBuilder().printStatus();
      ResultCode = 0;
    }

    if (ResultCode == 0)
    {
      FWLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), SymbolicName, Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

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
    ClientManager.registerClientService(getSymbolicName(), SERVICE_STATUS_KEY, ClientManager.PARAM_DYNAMIC);
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
