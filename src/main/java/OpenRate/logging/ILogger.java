

package OpenRate.logging;

import OpenRate.exception.InitializationException;
import OpenRate.resource.IResource;

/**
 * ILogger interface. Allows the implementation of the logger to be manages
 * independently of the code.
 */
public interface ILogger extends IResource
{
  /**
   * ResourceContext key used to load correct AstractLogger type
   */
  public static final String KEY = "Logger";

  /**
   * default category to use
   */
  public static final String DEFAULT_CATEGORY = "DEFAULT";

  /**
   * Perform whatever initialization is required of the resource.
   */
  @Override
  public abstract void init(String ResourceName) throws InitializationException;

  /**
   * Perform whatever cleanup is required of the underlying object..
   */
  @Override
  public abstract void close();

  /**
   * Log a message with a fatal priority. Fatal messages are
   * usually not recoverable & will precede an application
   * shutdown. Actual support for this priority is log
   * implementation specific, but should be available in
   * most packages. (e.g. Log4J, JDK1.4)
   *
  * @param message The message
   */
  public abstract void fatal(String message);

  /**
   * Log a message with a fatal priority. Fatal messages are
   * usually not recoverable & will precede an application
   * shutdown. Actual support for this priority is log
   * implementation specific, but should be available in
   * most packages. (e.g. Log4J, JDK1.4)
   *
  * @param message The message
  * @param t The throwable
   */
  public abstract void fatal(String message, Throwable t);

  /**
   * Log a message with an error priority. Errors are used for
   * problems found during processing that are not fatal. Actual
   * support for this priority is log implementation specific,
   * but should be available in most packages. (e.g. Log4J,
   * JDK1.4)
   *
  * @param message The message
   */
  public abstract void error(String message);

  /**
   * Log a message with an error priority. Errors are used for
   * problems found during processing that are not fatal. Actual
   * support for this priority is log implementation specific,
   * but should be available in most packages. (e.g. Log4J,
   * JDK1.4)
   *
  * @param message The message
  * @param t The throwable
   */
  public abstract void error(String message, Throwable t);

  /**
   * Log a message with a  warning priority. Warnings are useful
   * for messages that signal an error in the future if not
   * corrected. (e.g. low disk space) or potential errors that
   * may not be a problem depending on context that the logging
   * component does not have. Actual support for this priority
   * is log implementation specific, but should be available in
   * most packages. (e.g. Log4J, JDK1.4)
   *
  * @param message The message
   */
  public abstract void warning(String message);

  /**
   * Check whether this category is enabled for the WARNING Level.
   *
   * @return true if enabled
   */
  public abstract boolean isWarningEnabled();

  /**
   * Log a message with an informational priority. Informational
   * messages will include things like operational statistics,
   * which are nice to have, but don't signal an error of any
   * kind in the application. The number of informational
   * messages used in a process should be very low. Actual
   * support for this priority is log implementation specific,
   * but should be available in most packages. (e.g. Log4J,
   * JDK1.4)
   *
  * @param message The message
   */
  public abstract void info(String message);

  /**
   * Check whether this category is enabled for the INFO Level.
   *
   * @return true if enabled
   */
  public abstract boolean isInfoEnabled();

  /**
   * Log a message with a debug priority. Debug messages are normally be
   * disabled in production environments, but can be quite useful during
   * development.
   *
   * @param message
   */
  public abstract void debug(String message);

  /**
   * Check whether this category is enabled for the DEBUG Level.
   *
   * @return true if enabled
   */
  public abstract boolean isDebugEnabled();
}
