/* ====================================================================
 * Limited Evaluation License:
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
package OpenRate.process;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

/**
 * Logger for process states
 * 
 * @author TGDSPIA1
 */
public class ProcessStateLogger extends AbstractPlugIn implements Runnable
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: ProcessStateLogger.java,v $, $Revision: 1.10 $, $Date: 2013-05-13 18:12:10 $";

  private LinkedList<Object> logQueue;

  private boolean isClientAvailable = false;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
    throws InitializationException
  {
    logQueue = new LinkedList<Object>();
    SocketOutputThread thread = new SocketOutputThread();
    thread.start();

    // Do the inherited work, e.g. setting the symbolic name etc
    super.init(PipelineName,ModuleName);

    // Get the cache object reference
//    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
//       ModuleName,"DataCache");

    
  }

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

// -----------------------------------------------------------------------------
// ----------------------- Start of utility functions --------------------------
// -----------------------------------------------------------------------------

 /**
  * 
  * @param data
  * @throws ProcessingException
  */
  protected void logData(Object data) throws ProcessingException
  {
      if(isClientAvailable){
          synchronized(this){
//            System.out.println(" adding to Queue"+logQueue.size());
            logQueue.add(data);
//            System.out.println(" added to Queue"+logQueue.size());
          }
      }
      
  }



  @Override
  public IRecord procValidRecord(IRecord r) throws ProcessingException
  {
      if(r.getCurrentStateObject() != null)
      {
        logData(r.getCurrentStateObject());
      }
      return r;
  }

  @Override
  public IRecord procErrorRecord(IRecord r) throws ProcessingException
  {
      return r;
  }

private class SocketOutputThread extends Thread {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    public SocketOutputThread() {
        super("SocketOutputThread-" + new java.util.Date());
        try {
            serverSocket = new ServerSocket(8208);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void run() {
      DataOutputStream out;
      while (!isInterrupted()) {
        try{
            isClientAvailable = false;
            clientSocket = serverSocket.accept();
            isClientAvailable = true;
            out = new DataOutputStream(clientSocket.getOutputStream());
            
            // get a task to run
            Object dataToLog;
            try {
              dataToLog = getLogData();
                while (dataToLog != null) {
                     System.out.println(dataToLog);
                     out.writeUTF(dataToLog.toString()+"\r\n");
                     out.flush();
                     dataToLog = getLogData();
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
      }catch(Exception e){
          e.printStackTrace();
      }
    }
  }
    protected Object getLogData() throws InterruptedException {
        while (logQueue.size() == 0) {
              SocketOutputThread.sleep(500);
        }
        return logQueue.removeFirst();
    }
  }


}
