

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
