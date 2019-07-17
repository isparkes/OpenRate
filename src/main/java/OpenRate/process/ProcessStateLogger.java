
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
    logQueue = new LinkedList<>();
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
