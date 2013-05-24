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

import OpenRate.exception.InitializationException;
import OpenRate.resource.ResourceContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Log4JLogger
 *
 */
public final class Log4JLogger extends OpenRate.logging.AstractLogger
{
  /**
   * space for message templates.
   */
  public static final String SPACER = "   ";

  // make logging details work.
  private static final String FQCN   = Log4JLogger.class.getName();
  private Logger              logger;

  /**
   * Default constructor. - for backward compatibility.
   */
  public Log4JLogger()
  {
    logger                           = null;
  }

  /**
   * constructor used for new model, where multiple Loggers are
   * supported.
   *
   * @param categoryName The logger category name
   */
  public Log4JLogger(String categoryName)
  {
    logger = Logger.getLogger(categoryName);
  }

  /**
   * Perform whatever initialization is required of the resource.
   * This method should only be called once per application instance.
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    // Set the symbolic name
    setSymbolicName(ResourceName);

    /*
     * Apply a pseudo copy constructor to initialize this Log4JLogger
     * usually the old AstractLogger model. New users should use the AbstractLogFactory
     * instead..
     */
    AbstractLogFactory  factory         = getFactory(ResourceName);
    Log4JLogger defaultInstance = (Log4JLogger) factory.getDefaultLogger();

    // pseudo copy constructor to reset this objects state with the
    // configured AstractLogger state.
    this.logger = defaultInstance.logger;
  }

 /**
  * Load factory - used only for backward compatibility to support users
  * who don't know about the new AbstractLogFactory interface & are initializing
  * the AstractLogger class directly.
  *
  * Register the AbstractLogFactory so that it can be cleaned up by the
  * architecture upon application shutdown.
  *
  * @param ResourceName The resource name to get the factory for
  * @return The factory for the resource
  * @throws ConfigurationException
  * @throws InitializationException
  */
  public AbstractLogFactory getFactory(String ResourceName)
    throws InitializationException
  {
    AbstractLogFactory factory = AbstractLogFactory.getFactory(AbstractLogFactory.RESOURCE_KEY);
    factory.init(ResourceName);
    // register with ResourceContext for later.
    {
      ResourceContext ctx = new ResourceContext();
      ctx.register(AbstractLogFactory.RESOURCE_KEY, factory);
    }

    return factory;
  }

  /**
   * Perform whatever cleanup is required of the underlying object..
   */
  @Override
  public void close()
  {
    // no op
  }

 /**
  * Log a message with a fatal priority. Fatal messages are
  * usually not recoverable & will precede an application
  * shutdown. Actual support for this priority is log
  * implementation specific, but should be available in
  * most packages. (e.g. Log4J, JDK1.4)
  *
  * @param message The message
  */
  @Override
  public void fatal(String message)
  {
    logger.log(FQCN, Level.FATAL, message, null);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  */
  public void fatal(String className, String message)
  {
    logger.log(className, Level.FATAL, message, null);
  }

 /**
  * Log a message with a fatal priority. Fatal messages are
  * usually not recoverable & will precede an application
  * shutdown. Actual support for this priority is log
  * implementation specific, but should be available in
  * most packages. (e.g. Log4J, JDK1.4)
  *
  * @param message The message
  */
  @Override
  public void fatal(String message, Throwable t)
  {
    logger.log(FQCN, Level.FATAL, message, t);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  * @param t The throwable
  */
  public void fatal(String className, String message, Throwable t)
  {
    logger.log(className, Level.FATAL, message, t);
  }

  /**
   * Log the provided error message.
   *
   * Note: The Priority of messages logged with the error() method is
   * <link>org.apache.log4j.Priority.ERROR</link>.
   *
  * @param message The message
   */
  @Override
  public void error(String message)
  {
    logger.log(FQCN, Level.ERROR, message, null);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  */
  public void error(String className, String message)
  {
    logger.log(className, Level.ERROR, message, null);
  }

 /**
  * Log the provided error message.
  *
  * Note: The Priority of messages logged with the error() method is
  * <link>org.apache.log4j.Priority.ERROR</link>.
  *
  * @param message The message
  * @param t The throwable
  */
  @Override
  public void error(String message, Throwable t)
  {
    logger.log(FQCN, Level.ERROR, message, t);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  * @param t The throwable
  */
  public void error(String className, String message, Throwable t)
  {
    logger.log(className, Level.ERROR, message, t);
  }

 /**
  * Log the provided warning message. This method allows for developers
  * to easily log free form messages about the program status. It is
  * designed to be easy to use & simple to encourage debug messages
  * in the code.
  *
  * Note: The Priority of messages logged with the info() method is
  * <link>org.apache.log4j.Priority.WARNING</link>. These message can
  * be disabled in the log4j configuration file when deploying to
  * a production environment. (or for performance testing.)
  *
  * @param message The message
  */
  @Override
  public final void warning(String message)
  {
    logger.log(FQCN, Level.WARN, message, null);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  */
  public final void warning(String className, String message)
  {
    logger.log(className, Level.WARN, message, null);
  }

  /**
   * Check whether this category is enabled for the WARN Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isWarningEnabled()
  {
    return logger.isEnabledFor(Level.WARN);
  }

 /**
  * log the provided informational message. This method allows for
  * developers to easily log free form messages about the program
  * status. It is designed to be easy to use & simple to encourage
  * debug messages in the code.
  *
  * Note: The Priority of messages logged with the info() method is
  * <link>org.apache.log4j.Priority.INFO</link>. These message can
  * be disabled in the log4j configuration file when deploying to
  * a production environment. (or for performance testing.)
  *
  * @param message The message
  */
  @Override
  public final void info(String message)
  {
    logger.log(FQCN, Level.INFO, message, null);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly
  *
  * @param className The classname reporting the message
  * @param message The message
  */
  public final void info(String className, String message)
  {
    logger.log(className, Level.INFO, message, null);
  }

  /**
   * Check whether this category is enabled for the INFO Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isInfoEnabled()
  {
    return logger.isInfoEnabled();
  }

 /**
  * Log the provided debug message. This method allows for developers
  * to easily log free form messages about the program status. It is
  * designed to be easy to use & simple to encourage debug messages
  * in the code.
  *
  * Note: The Priority of messages logged with the info() method is
  * <link>org.apache.log4j.Priority.INFO</link>. These message can
  * be disabled in the log4j configuration file when deploying to
  * a production environment. (or for performance testing.)
  *
  * @param message The message
  */
  @Override
  public final void debug(String message)
  {
    logger.log(FQCN, Level.DEBUG, message, null);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The classname reporting the message
  * @param message The message
  */
  public final void debug(String className, String message)
  {
    logger.log(className, Level.DEBUG, message, null);
  }

  /**
   * Check whether this category is enabled for the DEBUG Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isDebugEnabled()
  {
    return logger.isDebugEnabled();
  }
}

