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

package OpenRate.adapter.socket;

import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.Iterator;

/**
 * Flat File Output Adapter. Writes to a file stream output, using transaction
 * aware handling.
 */
public abstract class SocketOutputAdapter
  extends AbstractTransactionalOutputAdapter
  implements IEventInterface
{
  // Port of the Socket to listen on
  private int ListenerPort;
  /*
   * Socket is initialized in the init() method and is kept open for loadBatch()
   * calls and then closed in cleanup().
   */
  private String HostName;

  // Used to hold current communication
  private Socket OutputSocket;

  // Used to write the record to socket
  private PrintWriter OutputRecord;

  // Maximum retries for socket
  private static final int MAX_SOCKET_RETRIES = 10;

  /**
   * writing result terminator
   */
  public static final String REPLY_RECORD_TERMINATOR = "\n";

  /**
    * Default Constructor.
    */
  public SocketOutputAdapter()
  {
    super();
  }


 /**
  * Initialize the output adapter with the configuration that is to be used
  * for this instance of the adapter.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;

    // Register ourself with the client manager
    super.init(PipelineName, ModuleName);

    // Now we load the properties
    // Get the host name
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(PipelineName, ModuleName, "HostName", "0");

    if (ConfigHelper.equals("0"))
    {
      message = "Please set the host name using the HostName property";
      throw new InitializationException(message,getSymbolicName());
    }
    this.HostName = ConfigHelper;
    // Get the port number
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(PipelineName, ModuleName, "ListenerPort", "0");

    if (ConfigHelper.equals("0"))
    {
      message = "Please set the port number to listen on using the ListenerPort property";
      throw new InitializationException(message,getSymbolicName());
    }

    // see if we can convert it
    try
    {
      this.ListenerPort = Integer.parseInt(ConfigHelper);
    }
    catch (NumberFormatException nfe)
    {
      // Could not use the value we got
      message = "Could not parse the ListenerPort value <" + ConfigHelper + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Check the file name scanning variables, throw initialisation exception
    // if something is wrong.
    try{
        initSocket();
    }catch (IOException nfe)
    {
      // Could not use the value we got
      message = "Unable to open socket at host <"+HostName+"> with specified port <" + ListenerPort + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * Process the stream header. Get the file base name and open the transaction.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord procHeader(IRecord r) throws ProcessingException
  {
    HeaderRecord tmpHeader;

    tmpHeader = (HeaderRecord)r;

    super.procHeader(r);

//    if(isSocketValid()){
//        OutputRecord.write(r.toString());
//        OutputRecord.write(REPLY_RECORD_TERMINATOR);
//        OutputRecord.flushStream();
//        closeStream();
//    }else{
//        r = null;
//    }

    return r;
  }

 /**
  * Write good records to the defined output stream. This method performs
  * record expansion (the opposite of record compression) and then calls the
  * write for each of the records that results.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException
  {
    Collection<IRecord> outRecCol;
    FlatRecord          outRec;
    Iterator<IRecord>   outRecIter;

    outRecCol = procValidRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null)
    {
      outRecIter = outRecCol.iterator();
      if(isSocketValid())
      {
        try
        {
          OutputRecord = new PrintWriter(new BufferedOutputStream(OutputSocket.getOutputStream(), 1024), false);
        }
        catch (IOException ioe)
        {
          this.getExceptionHandler().reportException(new ProcessingException(ioe,getSymbolicName()));
        }
        while (outRecIter.hasNext())
        {
          outRec = (FlatRecord)outRecIter.next();
          OutputRecord.write(outRec.getData());
          OutputRecord.write(REPLY_RECORD_TERMINATOR);
          OutputRecord.flush();
        }
//        closeStream();
      }
      else
      {
        r = null;
      }
    }

    return r;
  }

 /**
  * Write bad records to the defined output stream.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException
  {
    Collection<IRecord> outRecCol;
    FlatRecord          outRec;
    Iterator<IRecord>   outRecIter;

    outRecCol = procErrorRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null)
    {
      outRecIter = outRecCol.iterator();
      if(isSocketValid())
      {
        try
        {
          OutputRecord = new PrintWriter(new BufferedOutputStream(OutputSocket.getOutputStream(), 1024), false);
        }
        catch (IOException ioe)
        {
          this.getExceptionHandler().reportException(new ProcessingException(ioe,getSymbolicName()));
        }

        while (outRecIter.hasNext())
        {
          outRec = (FlatRecord)outRecIter.next();
          OutputRecord.write(outRec.getData());
          OutputRecord.write(REPLY_RECORD_TERMINATOR);
          OutputRecord.flush();
        }
//        closeStream();
      }
      else
      {
        r = null;
      }
    }

    return r;
  }

 /**
  * Process the stream trailer. Get the file base name and open the transaction.
  *
  * @param r The current record we are working on
  * @return The prepared record
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {

    // Do the transaction level maintenance
    super.procTrailer(r);

    return r;
  }

  /**
   *
   * @throws IOException
   */
  private void initSocket()
                     throws IOException
  {
    if(OutputSocket == null || OutputSocket.isConnected() == false){
        OutputSocket = new Socket(this.HostName, this.ListenerPort);
        getPipeLog().info("Input Socket Initialized @ host: "+this.HostName+" port: " + this.ListenerPort);
    }
  }

  /**
   *
   * @return
   */
  private boolean isSocketValid(){
      int tmpTries = 0;
      while(tmpTries < MAX_SOCKET_RETRIES){
        try{
            initSocket();
            return true;
        }catch(IOException ex){
            tmpTries++;
            getPipeLog().info("Input Socket NOT Initialized @ host: "+this.HostName+" port: " + this.ListenerPort+" Try #: "+tmpTries);
        }

      }

      return false;
  }

  private void closeStream()
  {
    OutputRecord.close();
  }


  /**
  * Start transaction opens the transaction
  *
  * @param TransactionNumber The transaction we are working on
  * @return 0 if the transaction started OK
  */
  @Override
  public int startTransaction(int TransactionNumber)
  {
    return 0;
  }

 /**
  * Flush Transaction finishes the output of any existing records in the pipe.
  * Any errors or potential error conditions should be handled here, because
  * the commit/rollback should be guaranteed sucessful operations.
  *
  * @param TransactionNumber The transaction we are working on
  * @return 0 if everything flushed OK, otherwise -1
  */
  @Override
  public int flushTransaction(int TransactionNumber)
  {
    return 0;
  }

 /**
  * Commit Transaction closes the transaction status with success
  *
  * @param TransactionNumber The transaction we are working on
  */
  @Override
  public void commitTransaction(int TransactionNumber)
  {
    // Nothing needed
  }

 /**
  * Commit Transaction closes the transaction status with failure
  *
  * @param TransactionNumber The transaction we are working on
  */
  @Override
  public void rollbackTransaction(int TransactionNumber)
  {
    // Nothing needed
  }

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * @param transactionNumber The transaction we are working on
  */
  @Override
  public void closeTransaction(int transactionNumber)
  {
    // Nothing needed
  }

  /**
   *
   * @param TransactionNumber
   */
  @Override
  public void closeStream(int TransactionNumber)
  {
    // Nothing needed
  }

}
