

package OpenRate.configurationmanager;

/**
 * @author = g.z.
 * Interface for Socket protocols
 */
public interface ISocketProtocol
{
 /**
  * Processes the input and returns the reply of the protocol
  *
  * @param input The input to process
  * @return The retuen string
  */
  public String processInput(String input);
}
