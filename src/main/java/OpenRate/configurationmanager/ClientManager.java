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

import OpenRate.OpenRate;
import OpenRate.Pipeline;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;

/**
 * ClientManager is used for managing the client modules available for this
 * OpenRate Application. It serves as a repository of the client modules that
 * can be readily executed.
 *
 * @author a.villena
 */
public class ClientManager {

  // This is the map of the clients we know, indexed by the symbolic name

  private static HashMap<String, ClientContainer> HMClientMap = new HashMap<>();

  // This is the singleton instance
  private static ClientManager clientManager = null;

  /**
   * None of MANDATORY, DYNAMIC or SYNC
   */
  public static int PARAM_NONE = 0x00;

  /**
   * Indicates that the parameter is a mandatory parameter which must be loaded
   * on startup
   */
  public static int PARAM_MANDATORY = 0x01;

  /**
   * Indicates that this is a parameter which can be changed at run time
   */
  public static int PARAM_DYNAMIC = 0x02;

  /**
   * Indicates that this is a parameter which will trigger a sync point
   */
  public static int PARAM_SYNC = 0x04;

  /**
   * Mandatory and Dynamic
   */
  public static int PARAM_MANDATORY_DYNAMIC = 0x03;

  /**
   * Dynamic and Sync
   */
  public static int PARAM_DYNAMIC_SYNC = 0x06;

  // module symbolic name: never changes in this module, so not set dynamically
  private String SymbolicName = "ClientManager";

// -----------------------------------------------------------------------------
// --------------------- Client manager utility functions ----------------------
// -----------------------------------------------------------------------------
  /**
   * Returns the instance of the client manager.
   *
   * @return the client manager instance.
   */
  public static ClientManager getClientManager() {
    // Initialise the singleton if we need
    if (clientManager == null) {
      clientManager = new ClientManager();
    }

    // return the reference so we can work on it
    return clientManager;
  }

  /**
   * puts the client in the HashMap which contains all the client modules
   * registered
   *
   * @param SymbolicName - the symbolic name of the client module that will be
   * the key
   * @param clientContainer - clientContainer object which holds the client
   * module
   */
  private void put(String SymbolicName, ClientContainer clientContainer) {
    HMClientMap.put(SymbolicName, clientContainer);
  }

  /**
   * getter method for returning the ClientContainer of a specific client module
   *
   * @param SymbolicName - symbolic name of the client module
   * @return The client container object
   */
  public ClientContainer get(String SymbolicName) {
    if (HMClientMap.containsKey(SymbolicName)) {
      ClientContainer clCont = HMClientMap.get(SymbolicName);

      return clCont;
    } else {
      return null;
    }
  }

  /**
   * Clears down the map: used primarily for testing.
   */
  public void clear() {
    HMClientMap.clear();
  }
  
// -----------------------------------------------------------------------------
// ------------------------- IEventInterface functions -------------------------
// -----------------------------------------------------------------------------
  /**
   * registerClient registers the client module to the ClientManager. This also
   * caches the client module object that can be accessed and executed.
   *
   * @param pipelineName
   * @param symbolicName - symbolic name of the client module to add
   * @param objClient - instance of the client module
   * @throws InitializationException
   */
  public void registerClient(String pipelineName, String symbolicName, Object objClient) throws InitializationException {
    if (pipelineName == null) {
      throw new InitializationException("Pipeline Name cannot be empty", getSymbolicName());
    }

    if (symbolicName == null) {
      throw new InitializationException("Symbolic Name cannot be empty", getSymbolicName());
    }

    ClientContainer clCont = new ClientContainer(pipelineName, symbolicName, objClient);
    put(symbolicName, clCont);
    LogUtil.getStaticLogger("Framework").debug("Registered Client <" + symbolicName + ">");
  }

  /**
   * registerClientService registers the command available for the client module
   *
   * @param symbolicName - symbolic name of the client module to add the command
   * to
   * @param commandName - name of the command to register
   * @param paramFlags The parameter flags for this service (Mandatory, Dynamic,
   * Sync)
   *
   */
  public void registerClientService(String symbolicName,
          String commandName,
          int paramFlags) {
    ClientContainer clCont = get(symbolicName);

    boolean Mandatory = (paramFlags & PARAM_MANDATORY) == PARAM_MANDATORY;
    boolean Dynamic = (paramFlags & PARAM_DYNAMIC) == PARAM_DYNAMIC;
    boolean RequireSync = (paramFlags & PARAM_SYNC) == PARAM_SYNC;

    clCont.addService(commandName, Mandatory, Dynamic, RequireSync);
    OpenRate.getOpenRateFrameworkLog().debug("Registered Client Service <" + commandName + ">");
  }

// -----------------------------------------------------------------------------
// ---------------- Client Manager Configuration Push Functions ----------------
// -----------------------------------------------------------------------------
  /**
   * Client Manager Configuration Push is an innovative feature to allow the
   * Client Manager to check the configuration before trying to initialise the
   * pipelines. It should work like this: 1) The Client Manager reads in the
   * whole of the configuration on start up 2) The Client Manager then performs
   * a first level of plausibility checks on the configuration that has been
   * passed. 3) The Framework requests a list of the resources to create 4) The
   * Framework creates the resources 5) The Client Manager then pushes the
   * configuration to the resources 6) The Framework requests a list of the
   * pipelines to create 7) The Framework creates the pipelines 8) The Client
   * manager pushes the configuration to the pipeline modules
   */
  /**
   * This pushes the initial configuration into the plug-in with the symbolic
   * name given. This passes the configuration from the client manager into the
   * plug-in after the class has been instantiated to perform the initial
   * configuration
   *
   * @param SymbolicName The name of the plug-in to initialise
   */
  public void pushPlugInInit(String SymbolicName) {
    // ToDo
  }

  /**
   * Get a list of the resource modules to create
   *
   * @return List of the resources
   */
  public ArrayList<String> getResourceList() {
    // ToDo
    return null;
  }

  /**
   * Get a list of the pipelines to create
   *
   * @return List of the pipelines
   */
  public ArrayList<Pipeline> getPipelineList() {
    // ToDo
    return null;
  }

  /**
   * Get a list of the plug in modules to create for a given pipeline
   *
   * @param PipelineName The pipeline name we are querying
   * @return List of the modules the pipeline contains
   */
  public ArrayList<String> getPipelinePlugins(String PipelineName) {
    // ToDo
    return null;
  }

// -----------------------------------------------------------------------------
// ---------------------- Start stream handling functions ----------------------
// -----------------------------------------------------------------------------
  /**
   * getCommandList returns a StringBuffer containing the list of commands
   * accepted in the following format:
   *
   * SymbolicName ClassCommandName isMandatory isDynamic
   *
   * if parameter strSymbolicName is set to null, all commands of all Clients
   * will be listed; else specify the symbolicName of a Client to get the
   * specific commands of it. if isMandatory is set to true, only mandatory
   * commands will only be included in the list; set to false if you want to get
   * all the commands
   *
   * @param strSymbolicName - symbolic name to search
   * @param isMandatoryOnly - flag to get only the mandatory commands or all
   * commands
   * @param GUIMode - Return the list in GUIMode or not (GUIMode is a machine
   * readable format)
   * @return StringBuffer - list of commands available
   */
  public StringBuffer getCommandList(String strSymbolicName,
          boolean isMandatoryOnly,
          boolean GUIMode) {
    StringBuffer strCmdList = new StringBuffer();
    String strSymbolicKey;
    String strHelper;
    String strCurrentValue = "";
    String strClassName;
    ClientContainer clCont;
    ServiceContainer svCont;
    Object ClientObject;
    IEventInterface ClientEvent;
    String strCommandName;
    String pipelineName;

    if (!GUIMode) {
      strCmdList.append(
              "OpenRate command listing:\r\n+-----------------------------------------------------------------+---+---+---+\r\n");
      strCmdList.append("| Command                                                         | M | D | S |\r\n");
      strCmdList.append("+-----------------------------------------------------------------+---+---+---+\r\n");
    }

    // Sort the modules
    List<String> clientKeys = new ArrayList<>(HMClientMap.keySet());
    Collections.sort(clientKeys);

    Iterator<String> iterNameList = clientKeys.iterator();

    if (strSymbolicName == null) {
      strSymbolicName = "*";
    }

    while (iterNameList.hasNext()) {
      strSymbolicKey = iterNameList.next();

      if ((strSymbolicName.endsWith("*"))
              | (strSymbolicName.equals(strSymbolicKey))) {
        clCont = HMClientMap.get(strSymbolicKey);

        // get the pipe name for this container
        pipelineName = clCont.getClientPipelineName();

        HashMap<String, ServiceContainer> HMSvcList = clCont.getService();

        // Sort the commands within the module
        List<String> commandKeys = new ArrayList<>(HMSvcList.keySet());
        Collections.sort(commandKeys);

        // Get the interator
        Iterator<String> iterSvcList = commandKeys.iterator();

        // Get the current values
        ClientObject = clCont.getClientObject();
        strClassName = clCont.getClientClassName();

        if (!GUIMode) {
          strHelper = "|" + pipelineName + ":" + strSymbolicKey
                  + "                                                                  ";
          strCmdList.append(strHelper.substring(0, 66)).append("|   |   |   |\r\n");
        } else {
          strHelper = "Module;" + pipelineName + ";" + strSymbolicKey + ";" + strClassName + "\r\n";
          strCmdList.append(strHelper);
        }

        while (iterSvcList.hasNext()) {
          strCommandName = iterSvcList.next();

          svCont = HMSvcList.get(strCommandName);

          if (ClientObject instanceof IEventInterface) {
            ClientEvent = (IEventInterface) ClientObject;

            try {
              strCurrentValue = ClientEvent.processControlEvent(strCommandName, false, "");
            } catch (Exception e) {
              System.err.println("Error processing control event <" + strCommandName + "> in module <" + strSymbolicKey + ">");
            }
          }

          //check if we want to list mandatory commands only
          if (isMandatoryOnly) {
            //check if the command is mandatory; if yes add it to the command list
            if (svCont.getMandatory()) {
              if (!GUIMode) {
                strHelper = getFormatCommand(pipelineName,
                        strSymbolicKey,
                        strCommandName,
                        strCurrentValue,
                        svCont.getMandatory(),
                        svCont.getDynamic(),
                        svCont.getRequireSync());
                strCmdList.append(strHelper).append("\r\n");
              } else {
                strHelper = getFormatCommandGUI(pipelineName,
                        strSymbolicKey,
                        strCommandName,
                        strCurrentValue,
                        svCont.getMandatory(),
                        svCont.getDynamic(),
                        svCont.getRequireSync());
                strCmdList.append("Command;").append(strHelper).append("\r\n");
              }
            }
          } else {

            if (!GUIMode) {
              //add to command list whether mandatory or not
              strHelper = getFormatCommand(pipelineName,
                      strSymbolicKey,
                      strCommandName,
                      strCurrentValue,
                      svCont.getMandatory(),
                      svCont.getDynamic(),
                      svCont.getRequireSync());
              strCmdList.append(strHelper).append("\r\n");
            } else {
              strHelper = getFormatCommandGUI(pipelineName,
                      strSymbolicKey,
                      strCommandName,
                      strCurrentValue,
                      svCont.getMandatory(),
                      svCont.getDynamic(),
                      svCont.getRequireSync());
              strCmdList.append("Command;").append(strHelper).append("\r\n");
            }
          }
        }
      }
    }

    if (!GUIMode) {
      strCmdList.append(
              "+-----------------------------------------------------------------+---+---+---+");
    } else {
      // Add the end tag
      strCmdList.append("OK");
    }

    return strCmdList;
  }

  /**
   * getModuleList returns all the available client modules that have been
   * registered in the framework.
   *
   * @param GUIMode True if the list should be returned for use in the GUI
   * @return StringBuffer - containing the client module symbolic names
   */
  public StringBuffer getModuleList(boolean GUIMode) {
    StringBuffer strModuleList = new StringBuffer();
    String strSymbolicKey;
    String strClassName;
    String strHelper;
    String pipelineName;

    if (!GUIMode) {
      strModuleList.append(
              "OpenRate module listing:\r\n+--------------------+----------------------------------------+----------------------------------------------------+\r\n");
      strModuleList.append(
              "| Pipeline Name      | Module Name                            | Class                                              |\r\n");
      strModuleList.append(
              "+--------------------+----------------------------------------+----------------------------------------------------+\r\n");
    }

    List<String> clientKeys = new ArrayList<>(HMClientMap.keySet());
    Collections.sort(clientKeys);
    Iterator<String> iterNameList = clientKeys.iterator();

    while (iterNameList.hasNext()) {
      strSymbolicKey = iterNameList.next();

      ClientContainer clCont = HMClientMap.get(strSymbolicKey);
      strClassName = clCont.getClientClassName();
      pipelineName = clCont.getClientPipelineName();

      if (!GUIMode) {
        strHelper = getFormatModule(pipelineName, strSymbolicKey, strClassName);
      } else {
        strHelper = getFormatModuleGUI(pipelineName, strSymbolicKey, strClassName);
      }
      strModuleList.append(strHelper).append("\r\n");

    }

    if (!GUIMode) {
      strModuleList.append(
              "+--------------------+----------------------------------------+----------------------------------------------------+");
    } else {
      // Add the end tag
      strModuleList.append("OK");
    }

    return strModuleList;
  }

  /**
   * getModuleList returns all the available client modules that have been
   * registered in the framework.
   *
   * @return StringBuffer - containing the client module symbolic names
   */
  public StringBuffer getThreadList() {
    StringBuffer strThreadList = new StringBuffer();

    long totalCpuTime = 0l;
    long totalUserTime = 0l;

    strThreadList.append(
            "OpenRate thread listing:\r\n+--------------------+------------------------------------+\r\n");

    ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    strThreadList.append("current-thread-count       <").append(threads.getThreadCount()).append(">\r\n");
    strThreadList.append("total-started-thread-count <").append(threads.getTotalStartedThreadCount()).append(">\r\n");
    strThreadList.append("daemon-thread-count        <").append(threads.getDaemonThreadCount()).append(">\r\n");
    strThreadList.append("peak-thread-count          <").append(threads.getPeakThreadCount()).append(">\r\n");

    // Parse each thread
    ThreadInfo[] threadInfos = threads.getThreadInfo(threads.getAllThreadIds());
    for (int i = 0; i < threadInfos.length; i++) {
      strThreadList.append("id             <").append(Long.toString(threadInfos[i].getThreadId())).append(">\r\n");
      strThreadList.append("name           <").append(threadInfos[i].getThreadName()).append(">\r\n");
      strThreadList.append("cpu-time-nano  <").append(Long.toString(threads.getThreadCpuTime(threadInfos[i].getThreadId()))).append(">\r\n");
      strThreadList.append("cpu-time-ms    <").append(Long.toString(threads.getThreadCpuTime(threadInfos[i].getThreadId()) / 1000000l)).append(">\r\n");
      strThreadList.append("user-time-nano <").append(Long.toString(threads.getThreadUserTime(threadInfos[i].getThreadId()))).append(">\r\n");
      strThreadList.append("user-time-ms   <").append(Long.toString(threads.getThreadUserTime(threadInfos[i].getThreadId()) / 1000000l)).append(">\r\n");
      strThreadList.append("blocked-count  <").append(Long.toString(threadInfos[i].getBlockedCount())).append(">\r\n");
      strThreadList.append("blocked-time   <").append(Long.toString(threadInfos[i].getBlockedTime())).append(">\r\n");
      strThreadList.append("waited-count   <").append(Long.toString(threadInfos[i].getWaitedCount())).append(">\r\n");
      strThreadList.append("waited-time    <").append(Long.toString(threadInfos[i].getWaitedTime())).append(">\r\n");
      strThreadList.append("+--------------------+\r\n");

      // Update our aggregate values
      totalCpuTime += threads.getThreadCpuTime(threadInfos[ i].getThreadId());
      totalUserTime += threads.getThreadUserTime(threadInfos[ i].getThreadId());
    }
    long totalCpuTimeMs = totalCpuTime / 1000000l;
    long totalUserTimeMs = totalUserTime / 1000000l;

    strThreadList.append("total-cpu-time-nano  <").append(Long.toString(totalCpuTime)).append(">\r\n");
    strThreadList.append("total-user-time-nano <").append(Long.toString(totalUserTime)).append(">\r\n");
    strThreadList.append("total-cpu-time-ms    <").append(Long.toString(totalCpuTimeMs)).append(">\r\n");
    strThreadList.append("total-user-time-ms   <").append(Long.toString(totalUserTimeMs)).append(">\r\n");

    strThreadList.append(
            "+--------------------+------------------------------------+");

    return strThreadList;
  }

  /**
   * Formats the command items for display
   *
   * @param pipelineName Name of the pipeline holding the module
   * @param symbolicName Name of the module
   * @param command The command name
   * @param currentValue The current value
   * @param mandatory True if mandatory
   * @param dynamic True if dynamic
   * @param requireSync True if requires sync point
   * @return Formatted string
   */
  public String getFormatCommand(String pipelineName,
          String symbolicName,
          String command,
          String currentValue,
          boolean mandatory,
          boolean dynamic,
          boolean requireSync) {
    String strHelperName;
    String strHelperMandatory;
    String strHelperDynamic;
    String strHelperReqSync;

    strHelperName = "|  - " + symbolicName + ":" + command
            + "                                                                  ";
    strHelperName = strHelperName.substring(0, 66) + "|";

    if (mandatory) {
      strHelperMandatory = " X |";
    } else {
      strHelperMandatory = "   |";
    }

    if (dynamic) {
      strHelperDynamic = " X |";
    } else {
      strHelperDynamic = "   |";
    }

    if (requireSync) {
      strHelperReqSync = " X |";
    } else {
      strHelperReqSync = "   |";
    }
    return strHelperName + strHelperMandatory + strHelperDynamic + strHelperReqSync;
  }

  /**
   * Formats the command items for the GUI
   *
   * @param pipelineName Name of the pipeline holding the module
   * @param symbolicName Name of the module
   * @param command The command name
   * @param currentValue The current value
   * @param mandatory True if mandatory
   * @param dynamic True if dynamic
   * @param requireSync True if requires sync point
   * @return Formatted string
   */
  public String getFormatCommandGUI(String pipelineName,
          String symbolicName,
          String command,
          String currentValue,
          boolean mandatory,
          boolean dynamic,
          boolean requireSync) {
    String strHelperOutput;
    String strHelperMandatory;
    String strHelperDynamic;
    String strHelperReqSync;
    strHelperOutput = pipelineName + ";" + symbolicName + ";" + command + ";" + currentValue + ";";

    if (mandatory) {
      strHelperMandatory = "X;";
    } else {
      strHelperMandatory = " ;";
    }

    if (dynamic) {
      strHelperDynamic = "X;";
    } else {
      strHelperDynamic = " ;";
    }

    if (requireSync) {
      strHelperReqSync = "X;";
    } else {
      strHelperReqSync = " ;";
    }
    return strHelperOutput + strHelperMandatory + strHelperDynamic + strHelperReqSync;
  }

  /**
   * Formats the module items for display
   *
   * @param pipelineName Name of the pipeline the module is in
   * @param SymbolicName Name of the module
   * @param Class Class name of the module
   * @return Formatted string
   */
  public String getFormatModule(String pipelineName, String SymbolicName, String Class) {
    String strHelperPipe;
    String strHelperName;
    String strHelperClass;
    strHelperPipe = "| " + pipelineName + "                                          ";
    strHelperPipe = strHelperPipe.substring(0, 21);
    strHelperName = "| " + SymbolicName + "                                          ";
    strHelperName = strHelperName.substring(0, 41) + "| ";
    strHelperClass = Class + "                                                                      ";
    strHelperClass = strHelperClass.substring(0, 51) + "| ";

    return strHelperPipe + strHelperName + strHelperClass;
  }

  /**
   * Formats the module items for GUI
   *
   * @param pipelineName Name of the pipeline the module is in
   * @param SymbolicName Name of the module
   * @param Class Class name of the module
   * @return Formatted string
   */
  public String getFormatModuleGUI(String pipelineName, String SymbolicName, String Class) {
    String strHelper;

    strHelper = "Module;" + pipelineName + ";" + SymbolicName + ";" + Class;

    return strHelper;
  }

  /**
   * @return the SymbolicName
   */
  public String getSymbolicName() {
    return SymbolicName;
  }

  /**
   * @param SymbolicName the SymbolicName to set
   */
  public void setSymbolicName(String SymbolicName) {
    this.SymbolicName = SymbolicName;
  }
}
