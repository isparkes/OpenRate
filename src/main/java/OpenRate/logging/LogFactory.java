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

package OpenRate.logging;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;
import java.io.File;
import java.net.URL;
import java.util.HashMap;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Log'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * The LogFactory provides access to the logging mechanism, based on Log4J.
 * It provides a map to the various log objects that can exist in the system,
 * and this can provide access to them.
 */
public class LogFactory extends AbstractLogFactory implements IEventInterface
{
  // cache Categories
  private static HashMap<String, Log4JLogger> LogStreams = new HashMap<>();

  // Default logger name
  private static String defaultLoggerName = "DefaultLogger";

  // The properties we are working from
  private static String log4j_properties;

  // Get access to the framework logger - is set after initial load
  private ILogger fwLog = null;

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
  public LogFactory()
  {
    super();
  }

  /**
   * Perform whatever initialization is required of the resource.
   * This method should only be called once per application instance.
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    /* in the case of multi-process applications, the init() may
     * be called more than once. Ensure that log4j only creates one file by
     * only configuring it one time per application.
     */
    if (!isLoaded())
    {
      // Set the symbolic name
      symbolicName = ResourceName;

      if (!symbolicName.equalsIgnoreCase(RESOURCE_KEY))
      {
        // we are relying on this name to be able to find the resource
        // later, so stop if it is not right
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Log ModuleName should be <" + RESOURCE_KEY + ">",getSymbolicName()));
      }

      // configure log4j if a log4j config file is provided in the
      // configuration. If not, ignore configuration on the assumption
      // that log4j will handle it internally.
      log4j_properties = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Properties","None");

      // See if we got a logger definition
      if (log4j_properties.equals("None"))
      {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Logger Configuration File <" + ResourceName + "> not defined in Logger resource",getSymbolicName()));
      }

      // Get the file from the classpath
      URL fqConfigFileName = getClass().getResource( "/" + log4j_properties );
      
      // Does it exist?
      if (fqConfigFileName == null)
      {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not open Configuration File <" + fqConfigFileName + "> not defined in Logger resource",getSymbolicName()));
      }
      
      // Is it a file?
      if (new File(fqConfigFileName.getFile()).isFile() == false)
      {
        OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("Could not open Configuration File <" + fqConfigFileName + "> not defined in Logger resource",getSymbolicName()));
      }

      if (log4j_properties.endsWith(".xml"))
      {
        // ToDo: Add configure and watch
        // use the XML model
        DOMConfigurator.configure(fqConfigFileName);
      }
      else
      {
        // ToDo: Add configure and watch
        // use the traditional properties file model: Deprecated
        PropertyConfigurator.configure(fqConfigFileName);
      }

      // log4j initialized.
      loaded              = true;

      System.out.println("Logger initialised using configuration <" + fqConfigFileName.getFile() + ">");

      // If there is a default logger configured in the properties file then
      // use that.  Otherwise use AstractLogger.DEFAULT_CATEGORY
      defaultLoggerName   = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"DefaultCategory","Default");
    }
  }

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close()
  {
    LogStreams.clear();
  }

  /**
   * Utility to return the reference to the logger resource
   */
  @Override
  public AstractLogger getLogger(String type)
  {
    Log4JLogger tmpLogger = new Log4JLogger(type);
    if (LogStreams.containsKey(type) == false)
    {
      LogStreams.put(type, tmpLogger);
    }

    return LogStreams.get(type);
  }

  /**
   * Get default logger. This method exists to support backward
   * compatibility prior to the factory class. Prefer
   * getLogger(String type) instead.
   */
  @Override
  public AstractLogger getDefaultLogger()
  {
    return getLogger(defaultLoggerName);
  }

  /**
   * Returns the loaded status.
   *
   * @return true if loaded
   */
  protected boolean isLoaded()
  {
    return loaded;
  }

 /**
  * Set the framework log after it has been initialised.
  *
  * @param NewFWLog
  */
  public void setFrameworkLog(ILogger NewFWLog)
  {
    this.fwLog = NewFWLog;
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_RELOAD, ClientManager.PARAM_MANDATORY);
  }

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {

    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_RELOAD))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        // Don't reload
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase(""))
      {
        // return something that sounds meaningful
        return "false";
      }
    }

    // Currently this cannot handle any dynamic events
    if (ResultCode == 0)
    {
      fwLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getSymbolicName(), Command, Parameter));
      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
