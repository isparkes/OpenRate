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


package OpenRate.exception;


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
public class InitializationException extends Exception
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
