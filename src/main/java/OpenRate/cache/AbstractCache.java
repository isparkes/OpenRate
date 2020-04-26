package OpenRate.cache;

import OpenRate.exception.ExceptionHandler;

/**
 * This class encapsulates the most fundamental elements of a cache class, thus
 * freeing up descendents from having to define these anew.
 *
 * @author TGDSPIA1
 */
public abstract class AbstractCache
           implements ICacheable
{
  // The symbolic module name of the class stack
  private String symbolicName;

  // The exception handler that we use for reporting errors
  private ExceptionHandler handler;

  // used to simplify logging and exception handling
  public String message;
  
  /**
   * @return the symbolicName
   */
  public String getSymbolicName() {
    return symbolicName;
  }

  /**
   * @param symbolicName the symbolicName to set
   */
  public void setSymbolicName(String symbolicName) {
    this.symbolicName = symbolicName;
  }
  
  /**
   * @return the handler
   */
  public ExceptionHandler getHandler() {
    return handler;
  }

  /**
   * @param handler the handler to set
   */
    @Override
  public void setHandler(ExceptionHandler handler) {
    this.handler = handler;
  }
}
