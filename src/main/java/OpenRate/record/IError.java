

package OpenRate.record;


import java.io.Serializable;

/**
 * Interface definition for Error records
 */
public interface IError extends Serializable
{
 /**
  * Get the ErrorType object, which is used to classify errors into logical
  * groups
  *
  * @return The error type of this record
  */
  public ErrorType getType();

 /**
  * Get the severity of the message
  *
  * @return The severity of the message
  */
  public int getSeverity();

 /**
  * Set the severity of the message
  *
  * @param severity The severity of the message
  */
  public void setSeverity(int severity);

 /**
  * Get the text of the message. Usually an error code like "ERR_SOMETHING_WRONG".
  *
  * @return The error text of the message
  */
  public String getMessage();

 /**
  * Set the message. Usually an error code like "ERR_SOMETHING_WRONG".
  *
  * @param Message The text of the message
  */
  public void setMessage(String Message);

 /**
  * Set the creating module of the error
  *
  * @param ModuleName The name of the module setting the error
  */
  public void setModuleName(String ModuleName);

 /**
  * Get the module name of this error
  *
  * @return The module name that set this error
  */
  public String getModuleName();

 /**
  * Set the error number (error code, like -100) of the error
  *
  * @param ErrorNumber The new error number
  */
  public void setErrorNumber(int ErrorNumber);

 /**
  * Get the error number of this error
  *
  * @return The error number of this error
  */
  public int getErrorNumber();

 /**
  * Set the description string of the error, this is additional information
  * that is over an above the error code or error message
  *
  * @param ErrorDescription The description of the error
  */
  public void setErrorDescription(String ErrorDescription);

 /**
  * Get the error description of this error
  *
  * @return The description of this error
  */
  public String getErrorDescription();
}
