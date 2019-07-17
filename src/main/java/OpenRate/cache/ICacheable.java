

package OpenRate.cache;

import OpenRate.exception.ExceptionHandler;


/**
 * Interface used by the caching system for cacheable
 * objects.
 */
public interface ICacheable
{
  /**
   * Use this delimiter to delimit key for a cacheable object
   * if it is formed with multiple attributes.
   */
  public static final String MULTIPLE_KEY_SEPARATOR = "_";

 /**
  * Set the exception handler for this cache class.
  *
  * @param handler The exception handler to set
  */
  public void setHandler(ExceptionHandler handler);
}
