

package OpenRate.configurationmanager;

import OpenRate.OpenRate;
import OpenRate.exception.ProcessingException;
import java.io.*;
import java.net.Socket;

/**
 * This class implements the processes that are executed when a connection<br/>
 * to the socket listener is allowed.
 *
 * @author = g.z.
 */
public class SocketListener implements Runnable
{
  private Socket socket = null;
  private SocketConnectionData socData = null;

  /**
   * Constructor with Socket and SocketConnectionData objects passed as<br/>
   * parameters.
   * @param socket
   * @param socData
   */
  public SocketListener(Socket socket, SocketConnectionData socData)
  {
    this.socket = socket;
    this.socData = socData;
  }

  /**
   * Method that executes the processes after connection has been accepted<br/>
   * by the listener.
   */
  @Override
  public void run()
  {
    PrintStream out = null;
    BufferedReader br = null;
    try
    {
      //instantiates the PrintStream object
      out = new PrintStream(new BufferedOutputStream(socket.getOutputStream(), 1024), false);

      //isntantiates the BufferedReader object
      br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

      String input;
      String output;

      //displays the welcome message of the console
      SocketHelper.displayWelcomeMessage(out);

      SocketProtocol socProt = new SocketProtocol();

      //reads the bufferedreader or whatever the user or admin types in the
      //console
      while((input = br.readLine()) != null)
      {
        // Trim leading and trailing spaces
        input = input.trim();

        //processes the command executed by the user or admin.
        output = socProt.processInput(input);

        // Sometimes we might end up getting a null here, as the communications
        // protocol does not dictate what the values of parameters might be
        // Therefore intercept it and make it something we can use
        if (output == null)
        {
          output = "<null>";
        }

        if(!output.equalsIgnoreCase(SocketConstants.GOODBYE))
        {
          //if response is not goodbye, display the response.
          SocketHelper.displayResponse(out,output + "\r\n");
        }
        else
        {
          //if reponse is goodbye, breaks from the loop and quits.
          SocketHelper.displayQuitMessage(out);
          break;
        }
      }
    }
    catch(IOException ex)
    {
      ProcessingException listenerEx = new ProcessingException("IO Exception in Listener",ex,"Listener");
      OpenRate.getFrameworkExceptionHandler().reportException(listenerEx);
    }
    catch(Exception ex)
    {
      ProcessingException listenerEx = new ProcessingException("IO Exception in Listener",ex,"Listener");
      OpenRate.getFrameworkExceptionHandler().reportException(listenerEx);
    }
    finally
    {
      //closes PrintStream, BufferedReader and Socket objects.
      try
      {
        if(out!=null)
        {
          out.close();
        }

        if(br!=null)
        {
          br.close();
        }

        if(socket!=null)
        {
          socket.close();
        }
      }
      catch(IOException e)
      {
        OpenRate.getOpenRateFrameworkLog().error("SocketListener.run() error closing objects: " + e.getClass()
        + ": " + e.getMessage(),e);
      }
      //removes one connection count from the SockectConnectionData since
      //this connection has been terminated or stopped.
      SocketHelper.removeAConnectionCount(socData);
    }
  }
}
