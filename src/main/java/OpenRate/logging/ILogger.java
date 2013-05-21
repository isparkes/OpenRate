/* ====================================================================
 * Limited Evaluation License:
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
import OpenRate.resource.IResource;

/**
 * ILogger interface. Allows the implementation of the logger to be manages
 * independently of the code.
 */
public interface ILogger extends IResource
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: ILogger.java,v $, $Revision: 1.1 $, $Date: 2013-05-13 18:12:12 $";

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
