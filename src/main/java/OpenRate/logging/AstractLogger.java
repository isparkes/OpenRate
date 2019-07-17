package OpenRate.logging;

/**
 * AstractLogger class. This abstract class needs to be overridden by a
 * concrete implementation instance for use. In normal cases this will be via
 * the log4j logger.
 */
public abstract class AstractLogger implements ILogger
{
  // This is the symbolic name of the resource
  private String symbolicName;

  /**
   * @param symbolicName the SymbolicName to set
   */
  public void setSymbolicName(String symbolicName) {
    this.symbolicName = symbolicName;
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
