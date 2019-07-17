
package OpenRate.logging;

import OpenRate.exception.InitializationException;
import OpenRate.resource.ResourceContext;

import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.LifeCycle;

//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;

/**
 * Log4JLogger
 *
 */
public final class Log4JLogger extends AstractLogger {

  /**
   * space for message templates.
   */
  // make logging details work.
  private final Logger logger;

  /**
   * Default constructor. - for backward compatibility.
   */
  public Log4JLogger()
  {
    logger = null;
  }

  /**
   * constructor used for new model, where multiple Loggers are supported.
   *
   * @param categoryName The logger category name
   */
  public Log4JLogger(String categoryName)
  {
    logger = LogManager.getLogger(categoryName);
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
    // Set the symbolic name
    setSymbolicName(ResourceName);
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
  private AbstractLogFactory getFactory(String ResourceName)
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
	  LifeCycle lc = ((LifeCycle)LogManager.getContext());
	  if (lc.isStarted())
	  {
	     lc.stop();
	  }
  }

  /**
   * Log a message with a fatal priority. Fatal messages are usually not
   * recoverable & will precede an application shutdown. Actual support for this
   * priority is log implementation specific, but should be available in most
   * packages. (e.g. Log4J, JDK1.4)
   *
   * @param message The message
   */
  @Override
  public void fatal(String message)
  {
    logger.log(Level.FATAL, message);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  */
  public void fatal(String className, String message)
  {
    logger.log(Level.FATAL, className, message);
  }

  /**
   * Log a message with a fatal priority. Fatal messages are usually not
   * recoverable & will precede an application shutdown. Actual support for this
   * priority is log implementation specific, but should be available in most
   * packages. (e.g. Log4J, JDK1.4)
   *
   * @param message The message
   * @param t throwable to log
   */
  @Override
  public void fatal(String message, Throwable t)
  {
    logger.log(Level.FATAL, message, t);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  * @param t The throwable
  */
  public void fatal(String className, String message, Throwable t)
  {
    logger.log(Level.FATAL, className, message, t);
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
    logger.log(Level.ERROR, message);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  */
  public void error(String className, String message)
  {
    logger.log(Level.ERROR, className, message);
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
    logger.log(Level.ERROR, message, t);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  * @param t The throwable
  */
  public void error(String className, String message, Throwable t)
  {
    logger.log(Level.ERROR, className, message, t);
  }

  /**
   * Log the provided warning message. This method allows for developers to
   * easily log free form messages about the program status. It is designed to
   * be easy to use & simple to encourage debug messages in the code.
   *
   * Note: The Priority of messages logged with the info() method is
   * <link>org.apache.log4j.Priority.WARNING</link>. These message can be
   * disabled in the log4j configuration file when deploying to a production
   * environment. (or for performance testing.)
   *
   * @param message The message
   */
  @Override
  public final void warning(String message)
  {
    logger.log(Level.WARN, message);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  */
  public final void warning(String className, String message)
  {
    logger.log(Level.WARN, className, message);
  }

  /**
   * Check whether this category is enabled for the WARN Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isWarningEnabled()
  {
    return logger.isEnabled(Level.WARN);
  }

  /**
   * log the provided informational message. This method allows for developers
   * to easily log free form messages about the program status. It is designed
   * to be easy to use & simple to encourage debug messages in the code.
   *
   * Note: The Priority of messages logged with the info() method is
   * <link>org.apache.log4j.Priority.INFO</link>. These message can be disabled
   * in the log4j configuration file when deploying to a production environment.
   * (or for performance testing.)
   *
   * @param message The message
   */
  @Override
  public final void info(String message)
  {
	logger.log(Level.INFO, message);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly
  *
  * @param className The class name reporting the message
  * @param message The message
  */
  public final void info(String className, String message)
  {	 
    logger.log(Level.INFO, className, message);
  }

  /**
   * Check whether this category is enabled for the INFO Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  /**
   * Log the provided debug message. This method allows for developers to easily
   * log free form messages about the program status. It is designed to be easy
   * to use & simple to encourage debug messages in the code.
   *
   * Note: The Priority of messages logged with the info() method is
   * <link>org.apache.log4j.Priority.INFO</link>. These message can be disabled
   * in the log4j configuration file when deploying to a production environment.
   * (or for performance testing.)
   *
   * @param message The message
   */
  @Override
  public final void debug(String message)
  {
    logger.log(Level.DEBUG, message);
  }

 /**
  * So that InterfaceLogger works correctly (details such as
  * filename, line number, etc... so up correctly.)
  *
  * @param className The class name reporting the message
  * @param message The message
  */
  public final void debug(String className, String message)
  {
    logger.log(Level.DEBUG, className, message);
  }

  /**
   * Check whether this category is enabled for the DEBUG Level.
   *
   * @return true if enabled
   */
  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }
}
