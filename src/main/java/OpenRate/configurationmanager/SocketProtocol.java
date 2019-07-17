

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
