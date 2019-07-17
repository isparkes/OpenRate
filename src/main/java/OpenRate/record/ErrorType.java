

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

