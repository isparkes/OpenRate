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

import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
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
  private ILogger log = LogUtil.getLogUtil().getLogger("Framework");
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
      out = new PrintStream(new BufferedOutputStream(socket
        .getOutputStream(), 1024), false);

      //isntantiates the BufferedReader object
      br = new BufferedReader(new InputStreamReader(
        socket.getInputStream()));

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
    catch(IOException e)
    {
      log.error("OpenRate ECI Listener error: " + e.getClass() + ": " +
        e.getMessage());
    }
    catch(Exception e)
    {
      log.error("OpenRate ECI Listener error: " + e.getClass() + ": " +
        e.getMessage());
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
        log.error("SocketListener.run() error closing objects: " + e.getClass()
        + ": " + e.getMessage(),e);
      }
      //removes one connection count from the SockectConnectionData since
      //this connection has been terminated or stopped.
      SocketHelper.removeAConnectionCount(socData);
    }
  }
}
