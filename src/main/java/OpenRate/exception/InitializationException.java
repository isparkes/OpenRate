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

package OpenRate.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * InitializationException Class. Used by batch application
 * framework to report exceptions. As part of the new exception handling 
 * framework, we demand additional information about exceptions, so that we are
 * able to control the framework correctly.
 * 
 * InitializationEceptions are reserved for exceptions that occur during
 * framework startup, or reloading of reference data.
 * 
 * Parameters:
 *  - The message text (mandatory): The descriptive text of the error
 *  - The module name (mandatory): The OpenRate module that reported the 
 *      exception. This is usually recovered by getting the symbolic name of the
 *      module with "getSymbolicName().
 *  - The cause (optional): The exception or throwable which caused the
 *      underlying exception
 */
public class InitializationException extends NestableException
{
  // Serial UID
  private static final long serialVersionUID = 4048205287633180700L;

  // Whether we want to report this to the console or just to the error log
  private boolean report = false;
  
  // Whether we want to abort
  private boolean abort = false;
  
  /**
   * Constructor for InitializationException. For cases where are don't have (or
   * don't want to report) an underlying exception.
   *
   * @param msg The exception message
   * @param moduleName the name of the module that threw the exception 
   */
  public InitializationException(String msg, String moduleName)
  {
    super(msg);
  }

  /**
   * Constructor for InitializationException.
   *
   * @param msg The exception message
   * @param cause The underlying exception
   * @param moduleName the name of the module that threw the exception 
   */
  public InitializationException(String msg, Exception cause, String moduleName)
  {
    super(msg, cause);
  }
  
  /**
   * Constructor for InitializationException.
   *
   * @param msg The exception message
   * @param cause The throwable cause
   * @param moduleName the name of the module that threw the exception 
   */
  public InitializationException(String msg, String moduleName, boolean report, boolean abort, Throwable cause)
  {
    super(msg, cause);
    
    setReport(report); 
    setAbort(abort);
  }

    /**
     * @return the reportToConsole
     */
    public boolean isReport() {
        return report;
    }

    /**
     * @param report the reportToConsole to set
     */
    public final void setReport(boolean report) {
        this.report = report;
    }

    /**
     * @return the abortExecution
     */
    public boolean isAbort() {
        return abort;
    }

    /**
     * @param abortExecution the abortExecution to set
     */
    public final void setAbort(boolean abort) {
        this.abort = abort;
    }
  
}
