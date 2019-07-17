

package OpenRate.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * Generic cache exception class.
 */
public class CacheException extends NestableException
{
  private static final long serialVersionUID = 4909964323623796674L;

  /**
   * Constructor
   */
  public CacheException()
  {
    super();
  }

  /**
   * Constructor
   *
   * @param pMessage The cache exception message
   */
  public CacheException(String pMessage)
  {
    super(pMessage);
  }

  /**
   * Constructor
   *
   * @param cause Throwable cause
   */
  public CacheException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Constructor
   *
   * @param msg The message string
   * @param cause The throwable cause
   */
  public CacheException(String msg, Throwable cause)
  {
    super(msg, cause);
  }
}

