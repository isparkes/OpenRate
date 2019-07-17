

package OpenRate.configurationmanager;

import OpenRate.exception.InitializationException;

/**
 * IEventInterface defines the interface that will be used to control
 * modules that must interact with the outside world. The interface creates
 * a method that will receive events from the event controller and which
 * must be implemented to allow the correct interaction between the module
 * and the controller.
 */
public interface IEventInterface
{
 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  public String processControlEvent(String Command, boolean Init, String Parameter);

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  * @throws InitializationException
  */
  public void registerClientManager() throws InitializationException;
}
