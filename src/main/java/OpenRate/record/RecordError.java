

package OpenRate.record;

/**
 * Errors are added to the record at any time during processing.
 */
public class RecordError extends AbstractError
{
  private static final long serialVersionUID = -5050358890913890453L;

 /**
  * Constructor for RecordError, providing the type as well as the message and
  * the module name
  *
  * @param msg The message to attach to the error
  * @param type The error type
  * @param moduleName The name of the calling module
  */
  public RecordError(String msg, ErrorType type, String moduleName, String errorDescription)
  {
    super(msg, type, moduleName, errorDescription);
  }

 /**
  * Constructor for RecordError, providing the type as well as the message and
  * the module name
  *
  * @param msg The message to attach to the error
  * @param type The error type
  * @param moduleName The name of the calling module
  */
  public RecordError(String msg, ErrorType type, String moduleName)
  {
    super(msg, type, moduleName);
  }

 /**
  * Constructor for RecordError, providing the type as well as the message
  *
  * @param msg The message to attach to the error
  * @param type The error type
  */
  public RecordError(String msg, ErrorType type)
  {
    super(msg, type);
  }

  /**
   * Constructor for RecordError, uses the default special type
   *
   * @param msg The message to attach to the error. The type will be "SPECIAL"
   */
  public RecordError(String msg)
  {
    super(msg, ErrorType.SPECIAL);
  }
}
