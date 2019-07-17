package OpenRate.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * ASN1Exception
 *
 * @author Magnus
 */
public class ASN1Exception extends NestableException
{
 /**
  * constructor
  */
  public ASN1Exception()
  {
    super();
  }

  /**
   * Create an exception with a string argument
   *
   * @param arg0 The string argument
   */
  public ASN1Exception(String arg0)
  {
    super(arg0);
  }

  /**
   * Create an exception with a throwable argument
   *
   * @param arg0 The throwable argument
   */
  public ASN1Exception(Throwable arg0)
  {
    super(arg0);
  }

  /**
   * Create an exception with string and throwable arguments
   *
   * @param arg0 The string argument
   * @param arg1 The throwable argument
   */
  public ASN1Exception(String arg0, Throwable arg1)
  {
    super(arg0, arg1);
  }
}
