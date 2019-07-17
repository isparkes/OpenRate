
package OpenRate.logging;

import OpenRate.exception.InitializationException;

/**
 * The console logger writes the output to the system console, and is used
 * during framework startup (so that errors getting the log can be reported, for
 * example) and in the case that the logger can not be initialised.
 */
public class DefaultLogger extends OpenRate.logging.AstractLogger
{
  // These are the variables that control the output of the data
  private boolean DebugEnabled;
  private boolean InfoEnabled;
  private boolean WarningEnabled;

 /**
  * Constructor for DefaultLogger.
  */
  public DefaultLogger()
  {
    super();
  }

 /**
  * init()
  */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    // Set up the default values
    DebugEnabled = false;
    InfoEnabled = true;
    WarningEnabled = true;

    // Set the symbolic name
    setSymbolicName(ResourceName);
  }

 /**
  * close()
  */
  @Override
  public void close()
  {
    // nothing to do
  }

 /**
  * print a message with the fatal level. This will by default go to syserr, and
  * we assume that there is no switch for this
  *
  * @param message The message
  */
  @Override
  public void fatal(String message)
  {
    System.err.println(message);
  }

 /**
  * print a message with the fatal level. This will by default go to syserr, and
  * we assume that there is no switch for this. Add in the stack trace.
  *
  * @param message The message
  * @param t The throwable
  */
  @Override
  public void fatal(String message, Throwable t)
  {
    System.err.println(message);
    t.printStackTrace(System.err);
  }

 /**
  * print a message with the error level. This will by default go to syserr, and
  * we assume that there is no switch for this
  *
  * @param message The message
  */
  @Override
  public void error(String message)
  {
    System.err.println(message);
  }

 /**
  * print a message with the error level. This will by default go to syserr, and
  * we assume that there is no switch for this. Add in the stack trace.
  *
  * @param message The message
  * @param t The throwable
  */
  @Override
  public void error(String message, Throwable t)
  {
    System.err.println(message);
    t.printStackTrace(System.err);
  }

 /**
  * print a message with the warning level. This will go to syserr
  *
  * @param message The message
  */
  @Override
  public void warning(String message)
  {
    if (WarningEnabled)
      System.err.println(message);
  }

  /**
   * see if we can print a message with the warning level
   *
   * @return true if we can print a message with the warning level
   */
  @Override
  public boolean isWarningEnabled()
  {
    return WarningEnabled;
  }

  /**
   * Set the logger to accept warnings
   *
   * @param NewValue true if we are to accept warnings
   */
  public void setWarningEnabled(boolean NewValue)
  {
    WarningEnabled = NewValue;
  }

  /**
   * Set the logger to accept info
   *
   * @param NewValue true if we are to accept info
   */
  public void setInfoEnabled(boolean NewValue)
  {
    InfoEnabled = NewValue;
  }

  /**
   * Set the logger to accept debug messages
   *
   * @param NewValue true if we are to accept debug messages
   */
  public void setDebugEnabled(boolean NewValue)
  {
    DebugEnabled = NewValue;
  }

 /**
  * print a message with the info level. This will go to the sysout device.
  *
  * @param message The message
  */
  @Override
  public void info(String message)
  {
    if (InfoEnabled)
      System.out.println(message);
  }

  /**
   * see if we can print a message with the info level
   *
   * @return true if we can print a message with the info level
   */
  @Override
  public boolean isInfoEnabled()
  {
    return InfoEnabled;
  }

 /**
  * print a message with the debug level, because this should not normally
  * muddy the waters for errors, we send it to the syserr device.
  *
  * @param message The message
  */
  @Override
  public void debug(String message)
  {
    if (DebugEnabled)
      System.err.println(message);
  }

 /**
  * see if we can print a message with the debug level
  *
  * @return true if we can print a message with the debug level
  */
  @Override
  public boolean isDebugEnabled()
  {
    return DebugEnabled;
  }
}
