/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

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
