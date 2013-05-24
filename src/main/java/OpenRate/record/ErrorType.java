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

import java.io.Serializable;

/**
 * Represent a Type of error. An error type will store a unique id
 * and name.
 */
public class ErrorType implements Serializable
{
  private static final long serialVersionUID = 4762306080190537754L;

 /*
  * lock for ensuring syncrhronization. do not use the nextId attribute
  * directly. only use it via the getNextId() method.
  */
  private final static  Object idLock = new Object();
  private static        int    nextId = 0;

  /**
   * data not found error type
   */
  public static ErrorType DATA_NOT_FOUND = new ErrorType("DATA_NOT_FOUND");

  /**
   * data validation error type
   */
  public static ErrorType DATA_VALIDATION = new ErrorType("DATA_VALIDATION");

  /**
   * sql exception error type
   */
  public static ErrorType SQL_EXECUTION = new ErrorType("SQL_EXECUTION");

  /**
   * fatal sql exception error type
   */
  public static ErrorType SQL_EXECUTION_FATAL = new ErrorType("SQL_EXECUTION_FATAL");

  /**
   * informational msg template
   */
  public static ErrorType INFORMATION = new ErrorType("INFORMATION");

  /**
   * fatal error type
   */
  public static ErrorType FATAL = new ErrorType("FATAL");

  /**
   * special error type
   */
  public static ErrorType SPECIAL = new ErrorType("SPECIAL");

  /**
   * IO error type
   */
  public static ErrorType FILE_ERROR = new ErrorType("FILE_ERROR");

  /**
   * warning error type
   */
  public static ErrorType WARNING = new ErrorType("WARNING");

  /**
   * data not found error type
   */
  public static ErrorType LOOKUP_FAILED = new ErrorType("LOOKUP_FAILED");

  private int    id;
  private String name;

  /**
   * Constructor
   */
  public ErrorType()
  {
    this.id     = nextId++;
    this.name   = "NONE-" + id;
  }

  /**
   * Constructor
   *
   * @param name The name of the error type
   */
  public ErrorType(String name)
  {
    this.id     = nextId++;
    this.name   = name;
  }

  /**
   * Returns the id of the error type
   *
   * @return The error id of the error type
   */
  public int getId()
  {
    return id;
  }

  /**
   * Returns the name.
   *
   * @return The error name
   */
  public String getName()
  {
    return name;
  }

  /**
   * Return the hashCode (id) of the error type
   *
   * @return the id hashcode of the error type
   */
  @Override
  public int hashCode()
  {
    return id;
  }

  /**
   * Compare two error types
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ErrorType other = (ErrorType) obj;
    if (this.id != other.id) {
      return false;
    }
    return true;
  }

  /**
   * Gte the String representation of the error type
   *
   * @return the string representation
   */
  @Override
  public String toString()
  {
    return "ErrorType:" + name;
  }

  /**
   * Get the next id
   *
   * @return The next id
   */
  protected int getNextId()
  {
    synchronized (idLock)
    {
      return nextId++;
    }
  }
}

