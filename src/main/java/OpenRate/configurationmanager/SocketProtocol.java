/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

/**
 * This object serves as the Protocol for the command issued in the socket<br/>
 * listener. It decides what to reply and what to do with the command issued.
 *
 * @author = g.z.
 */
public class SocketProtocol implements ISocketProtocol
{
  // holds if we are in GUI mode or not
  private boolean GUIMode = false;

  // Create a client manager we are going to work with
  ClientManager clMan = new ClientManager();

  /**
   * This method processes the command issued by the user or admin. It
   * decides what to do with command and determines what response is to be
   * returned.
   *
   * @param input The input to process
   * @return The response of the processing
   */
  @Override
  public String processInput(String input)
  {
    boolean boolKnown = false;
    String output = "";
    String CmdModuleSymbolicName;
    String CmdCommand;
    String CmdParameter;
    IEventInterface ClientEvent;
    ClientContainer clCon;
    Object ClientObject;

    // make sure that input string does not contain any non-ASCII chars
    // some terminals, like putty, prefix some weird chars to the input string
    input = input.replaceAll("[^a-zA-Z0-9= \\:\\._]", "").trim();
    if((input.equalsIgnoreCase(SocketConstants.HELP)) |
       (input.equalsIgnoreCase(SocketConstants.HELP_SHORT)))
    {
      //if 'help' is the command issued
      output = SocketConstants.HELPMESSAGE;
    }
    else if((input.equalsIgnoreCase(SocketConstants.LISTMODULES)) |
            (input.equalsIgnoreCase(SocketConstants.LISTMODULES_SHORT)))
    {
      //if 'list modules' command is issued
      //list all of the modules registered in the framework
      output = clMan.getModuleList(GUIMode).toString();
    }
    else if((input.toUpperCase().startsWith(SocketConstants.LISTCOMMANDS.toUpperCase() + " ")) |
            (input.toUpperCase().startsWith(SocketConstants.LISTCOMMANDS_SHORT.toUpperCase() + " ")) |
            (input.equalsIgnoreCase(SocketConstants.LISTCOMMANDS)) |
            (input.equalsIgnoreCase(SocketConstants.LISTCOMMANDS_SHORT)))
    {
      //if 'list commands' command is issued with an an additional parameter
      //list the commands registered to the module
      String [] CmdParameterList = input.split("\\s");

      if (CmdParameterList.length > 1)
      {
        output = "";
        for (int Index = 1 ; Index < CmdParameterList.length ; Index++)
        {
          output += clMan.getCommandList(CmdParameterList[Index],false,GUIMode).toString();
        }
      }
      else
      {
        // get all commands
        output = clMan.getCommandList(null,false,GUIMode).toString();
      }
    }
    else if((input.equalsIgnoreCase(SocketConstants.QUIT)) |
            (input.equalsIgnoreCase(SocketConstants.QUIT_SHORT)) |
            (input.equalsIgnoreCase(SocketConstants.EXIT)) |
            (input.equalsIgnoreCase(SocketConstants.EXIT_SHORT)))
    {
      //if 'quit' the is the command issued
      output = SocketConstants.GOODBYE;
    }
    else if((input.equalsIgnoreCase(SocketConstants.THREADSTATUS)) |
            (input.equalsIgnoreCase(SocketConstants.THREADSTATUS_SHORT)))
    {
      // show the status of the threads
      output = ClientManager.getClientManager().getThreadList().toString();
    }
    else if(input.equalsIgnoreCase(SocketConstants.GUIMODE))
    {
      // set GUI mode, meaning that we use a simplified output format for use
      // with the GUI
      GUIMode = true;
      output = SocketConstants.OKMESSAGE;
    }
    else if(input.trim().length() < 1)
    {
      //if no command is entered but user pressed the 'enter' or 'return' key
      output = "";
    }
    else
    {
      //parse command string by splitting on the ":" and "=" notation
      String strInputParams[] = input.split(":");

      if (strInputParams.length == 2)
      {
        // Command appears to be in the correct form of notation, try to process
        // it further.  It is assumed that the form of command is
        // "Client:Service=Args" where Service=Args is the specific service to
        // be executed with a single argument

        // This is the module symbolic name that we are dealing with
        CmdModuleSymbolicName = strInputParams[0];

        // See if there are any arguments
        String CommandParams[] = strInputParams[1].split("=");

        if (CommandParams.length == 1)
        {
          CmdParameter = "";
          CmdCommand = strInputParams[1];
        }
        else
        {
          CmdCommand = CommandParams[0];
          CmdParameter = CommandParams[1];
        }

        clCon = ClientManager.getClientManager().get(CmdModuleSymbolicName);

        if (clCon != null)
        {
          //there is a cached ClientObject; process it

          ClientObject  = clCon.getClientObject();
          if (ClientObject instanceof IEventInterface)
          {
            ClientEvent = (IEventInterface) ClientObject;

            //call ProcesControlEvent on second parameter of input
            output = ClientEvent.processControlEvent(CmdCommand,false,CmdParameter);
            boolKnown = true;
          }
          else
          {
            OpenRate.getOpenRateFrameworkLog().error("Module <" + CmdModuleSymbolicName + "> is not able to respond to the event interface");
          }
        }
      }

      if (!boolKnown)
      {
        //if the command issued is unknown
        output = SocketConstants.OPENRATETAB.concat(input).concat(SocketConstants.UNKNOWNCOMMAND);
      }
    }

    //returns the response of the listener
    return output;
  }
  
  public static String getWelcomeMessage() {
    return 
    "--------------------------------------------------------------\r\n" +
    "OpenRate Admin Console, "+OpenRate.getApplicationVersionString()+"\r\n" +
    "Copyright The OpenRate Project, 2006-2015\r\n" +
    "--------------------------------------------------------------\r\n\r\n" +
    "Type 'Help' for more information.\r\n\r\n";
    
  }
  
}
