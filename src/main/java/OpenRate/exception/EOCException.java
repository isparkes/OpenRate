


package OpenRate.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * ASN1Exception
 *
 * @author Magnus
 */
public class EOCException extends NestableException
{
 /**
  * constructor
  */
  public EOCException()
  {
    super();
  }

  /**
   * Constructor for ConfigurationException.
   *
   * @param arg0 The message string
   */
  public EOCException(String arg0)
  {
    super(arg0);
  }

  /**
   * Constructor for ConfigurationException.
   *
   * @param arg0 The throwable
   */
  public EOCException(Throwable arg0)
  {
    super(arg0);
}

  /**
   * Constructor for ConfigurationException.
   *
   * @param arg0 The message string
   * @param arg1 The throwable
   */
  public EOCException(String arg0, Throwable arg1)
  {
    super(arg0, arg1);
  }
}
