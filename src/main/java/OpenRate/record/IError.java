/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
