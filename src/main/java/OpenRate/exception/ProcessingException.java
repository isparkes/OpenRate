

package OpenRate.exception;

import OpenRate.OpenRate;
import org.apache.commons.lang.exception.NestableException;

/**
 * ProcessingException Class. Used in the OpenRate architecture
 * when an execution thread encounters an unexpected problem at run time. Can
 * be thrown by any element of the pipeline. (input adapter, pipeline,
 * output adapter).
 * 
 * Normally a processing exception should cause the framework to abort, but
 * for applications where keeping running is important, you can configure
 * OpenRate to simply report the exception but keep running.
 */
public final class ProcessingException extends NestableException
{
  private static final long serialVersionUID = -7872768799697114024L;
  private int exitCode;         // The exit code that should be passed back
  private String moduleName;    // The name of the module that threw the exc

  /**
   * Default Constructor
   */
  public ProcessingException() {
    super();
    this.exitCode = OpenRate.FATAL_EXCEPTION;
  }

  /**
   * Constructor for a simple ProcessingException with a message only.
   * 
   * @param msg The message the Exception has
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(String msg, String moduleName) {
    super(msg);
    setExitCode(OpenRate.FATAL_EXCEPTION);
    setModuleName(moduleName);
  }

  /**
   * Constructor with a message and an error code
   * 
   * @param msg The message the exception has
   * @param code The error code associated with the exception
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(String msg, int code, String moduleName) {
    super(msg);
    setExitCode(OpenRate.FATAL_EXCEPTION);
    setModuleName(moduleName);
  }

  /**
   * Constructor for re-throwing a throwable.
   * 
   * @param cause The throwable that caused the exception
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(Throwable cause, String moduleName) {
    super(cause);
    setExitCode(OpenRate.FATAL_EXCEPTION);
    setModuleName(moduleName);
  }

  /**
   * Constructor for re-throwing a throwable with an assigned code.
   * 
   * @param cause The throwable that caused the exception
   * @param code The code associated with the throwable
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(Throwable cause, int code, String moduleName) {
    super(cause);
    setExitCode(code);
    setModuleName(moduleName);
  }

  /**
   * Constructor for re-throwing a throwable with a message text.
   * 
   * @param msg The message to be associated with the exception
   * @param cause The throwable that caused the exception
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(String msg, Throwable cause, String moduleName) {
    super(msg, cause);
    setExitCode(OpenRate.FATAL_EXCEPTION);
    setModuleName(moduleName);
  }

  /**
   * Constructor for re-throwing a throwable with a message text and a code.
   * 
   * @param msg The message to be associated with the exception
   * @param cause The throwable that caused the exception
   * @param code The error code associated with the exception
   * @param moduleName the name of the module that threw the exception 
   */
  public ProcessingException(String msg, Throwable cause, int code, String moduleName) {
    super(msg, cause);
    setExitCode(code);
    setModuleName(moduleName);
  }

  /**
   * get returnCode
   * @return the exit code to be used by the application for this type
   * of error.
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * set returnCode
   * @param exitCode
   */
  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

    /**
     * Return the name of the module that caused the error
     * 
     * @return the moduleName
     */
    public String getModuleName() {
        return moduleName;
    }

    /**
     * Set the name of the module that caused the error
     * 
     * @param moduleName the moduleName to set
     */
    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }
}

