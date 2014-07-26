/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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
