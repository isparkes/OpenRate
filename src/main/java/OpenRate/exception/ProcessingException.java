/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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

