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

package OpenRate.record;

/**
 * AbstractError class. Provides basic error functionality common
 * to all record errors. Each record can have one or more errors or messages
 * attached to it. These can be collected and/or evaluated during processing.
 */
public abstract class AbstractError implements IError
{
  /**
   * Serial UID for serial class
   */
  private static final long serialVersionUID = -1858978399504501227L;

  private ErrorType type;
  private int       errorNumber = 0;
  private String    message;
  private Exception exception;
  private int       severity;
  private String    moduleName = "";
  private String    errorDescription = "";

  /**
   * Constructor to create an error of a given type with a given message
   *
   * @param Message The message to attach to the error
   * @param type The error type to attach to the error
   */
  public AbstractError(String Message, ErrorType type)
  {
    this.type = type;
    this.message = Message;
  }

  /**
   * Constructor to create an error of a given type with a given message
   *
   * @param message The message to attach to the error
   * @param type The error type to attach to the error
   * @param moduleName The name of the setting module
   * @param errorDescription The description of the error
   */
  public AbstractError(String message, ErrorType type, String moduleName, String errorDescription)
  {
    this.type = type;
    this.message = message;
    this.moduleName = moduleName;
    this.errorDescription = errorDescription;
  }

  /**
   * Constructor to create an error of a given type with a given message
   *
   * @param message The message to attach to the error
   * @param type The error type to attach to the error
   * @param moduleName The name of the setting module
   */
  public AbstractError(String message, ErrorType type, String moduleName)
  {
    this.type = type;
    this.message = message;
    this.moduleName = moduleName;
  }

  /**
   * Get the ErrorType object.
   *
   * @return The error type for this error
   */
  @Override
  public ErrorType getType()
  {
    return this.type;
  }

  /**
   * Set the ErrorType object
   *
   * @param type The error type for this error
   */
  public void setType(ErrorType type)
  {
    this.type = type;
  }

  /**
   * The error can store the result of an exception for later evaluation.
   * This should normally be avoided for performance reasons.
   *
   * @return The exception that caused the error
   */
  public Exception getException()
  {
    return this.exception;
  }

  /**
   * Set the exception information associated with the error
   *
   * @param ex The exception that caused the error
   */
  public void setException(Exception ex)
  {
    this.exception = ex;
  }

  /**
   * Return the error message
   *
   * @return The error message
   */
  @Override
  public String getMessage()
  {
    return this.message;
  }

 /**
  * Set a message
  *
  * @param message
  */
  @Override
  public void setMessage(String message)
  {
    this.message = message;
  }

 /**
  * Set the severity of the error
  *
  * @param severity The severity to set
  */
  @Override
  public void setSeverity(int severity)
  {
    this.severity = severity;
  }

 /**
  * Get the severity of this error
  *
  * @return The severity of this error
  */
  @Override
  public int getSeverity()
  {
    return this.severity;
  }

 /**
  * Set the creating module of the error
  *
  * @param moduleName The name of the module setting the error
  */
  @Override
  public void setModuleName(String moduleName)
  {
    this.moduleName = moduleName;
  }

 /**
  * Get the module name of this error
  *
  * @return The module name that set this error
  */
  @Override
  public String getModuleName()
  {
    return this.moduleName;
  }

 /**
  * Set the error number of the error
  *
  * @param errorNumber The error number of the error
  */
  @Override
  public void setErrorNumber(int errorNumber)
  {
    this.errorNumber = errorNumber;
  }

 /**
  * Get the module name of this error
  *
  * @return The module name that set this error
  */
  @Override
  public int getErrorNumber()
  {
    return this.errorNumber;
  }

 /**
  * Set the description text of the error
  *
  * @param errorDescription The description text of the error
  */
  @Override
  public void setErrorDescription(String errorDescription)
  {
    this.errorDescription = errorDescription;
  }

 /**
  * Get the module name of this error
  *
  * @return The module name that set this error
  */
  @Override
  public String getErrorDescription()
  {
    return this.errorDescription;
  }
}
