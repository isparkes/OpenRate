

package OpenRate.configurationmanager;

import java.util.HashMap;

/**
 * ClientContainer defines the container object that holds the client module properties of
 * OpenRate.  This will be used by the ClientManager in holding up a client module repository.
 *
 * @author a.villena
 */
public class ClientContainer
{
  private String  clientPipelineName;
  private String  clientSymbolicName;
  private String  clientClassName;
  private Object  objClientContainer;
  private HashMap<String, ServiceContainer> HSServiceContainer;

 /**
  * Creates a new instance of ClientContainer.
  *
  * @param pipelineName The pipeline this client is in
  * @param strClientSybName The client symbolic name
  * @param objClient The client object reference
  */
  public ClientContainer(String pipelineName, String strClientSybName, Object objClient)
  {
    clientPipelineName = pipelineName;
    clientSymbolicName = strClientSybName;
    clientClassName = objClient.getClass().getName();
    objClientContainer = objClient;
    HSServiceContainer = new HashMap<>();
  }

  /**
   * setClientPipelineName is a setter method for the pipeline name of the client module
   *
   * @param pipelineName - pipeline name of the client module
   */
  public void setClientPipelineName(String pipelineName)
  {
    clientPipelineName = pipelineName;
  }

 /**
  * getSymbolicName is a getter method for the symbolic name of the client module
  *
  * @return clientPipelineName - pipeline name of the client module
  */
  public String getClientPipelineName()
  {
    return clientPipelineName;
  }

  /**
   * setclientSymbolicName is a setter method for the symbolic name of the client module
   *
   * @param strClientSybName - new symbolic name of the client module
   */
  public void setClientSymbolicName(String strClientSybName)
  {
    clientSymbolicName = strClientSybName;
  }

 /**
  * getSymbolicName is a getter method for the symbolic name of the client module
  *
  * @return clientSymbolicName - symbolic name of the client module
  */
  public String getClientSymbolicName()
  {
    return clientSymbolicName;
  }

 /**
  * setclientSymbolicName is a setter method for the symbolic name of the client module
  *
  * @param strClientClassName The class name of the client
  */
  public void setClientClassName(String strClientClassName)
  {
    clientClassName = strClientClassName;
  }

 /**
  * getSymbolicName is a getter method for the symbolic name of the client module
  *
  * @return clientSymbolicName - symbolic name of the client module
  */
  public String getClientClassName()
  {
    return clientClassName;
  }

 /**
  * setClientObject stores the client module instance represented by the symbolic name
  *
  * @param objClient - instance of the client module
  */
  public void setClientObject(Object objClient)
  {
    objClientContainer = objClient;
  }

  /**
   * getClientObject returns the client module instance represented by the symbolic name
   *
   * @return Object - instance of the client module
   */
  public Object getClientObject()
  {
    return objClientContainer;
  }

 /**
  * addService adds the services/commands that the client module understands
  *
  * @param CommandName Symbolic name of the client module
  * @param Mandatory Flag for setting if the command is mandatory
  * @param Dynamic Flag for setting if the command is dynamic
  * @param RequireSync Flag for setting if this command will start a sync
  */
  public void addService(String CommandName, boolean Mandatory,
                         boolean Dynamic, boolean RequireSync)
  {
    ServiceContainer tmpServ;
    tmpServ = new ServiceContainer(CommandName, Mandatory, Dynamic, RequireSync);
    HSServiceContainer.put(CommandName, tmpServ);
  }

 /**
  * getService is a getter method for getting the list of services/commands
  *
  * @return HashMap - HashMap which contains the service/command list; key is symbolic name
  * and the value is a ServiceContainer object
  */
  public HashMap<String, ServiceContainer> getService()
  {
    return HSServiceContainer;
  }
}
