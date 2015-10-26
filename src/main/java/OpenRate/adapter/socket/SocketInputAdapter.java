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
package OpenRate.adapter.socket;

import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Generic Socket Input InputAdapter.
 *
 * <p>
 * The basic function of this socket input adapter is to facilitate reading of
 * records from socket in batches, so that two pipelines can communicate each
 * other regardless where they are running and on which machine.
 *
 * The input adapter waits for record on socket, and when found, reads them and
 * turns them into batches to maintain the load on the pipeline.<br>
 *
 * Because this adapter uses the transactional parent class, the adapter is
 * transaction aware, and communicates with the parent transactional layer
 * communicates with the transaction manager to coordinate the processing.
 *
 * The transaction is initiated by receiving an input record with the HEADER
 * tag. The transaction is closed when a record is received with the TRAILER
 * tag. When the transaction is closed, the socket is closed as well, and must
 * be re-opened.
 *
 * <p>
 * Scanning and Processing<br>
 * -----------------------
 *
 * <p>
 * [to do]<br>
 *
 */
public abstract class SocketInputAdapter
        extends AbstractTransactionalInputAdapter
        implements IEventInterface {

  // Port of the Socket to listen on
  private int ListenerPort;
  
//Response in case the request was successful
  private String onSuccessfulResponse;
  
  // Response in case the request failed
  private String onFailedResponse;
  
  /*
   * Socket is initialized in the init() method and is kept open for loadBatch()
   * calls and then closed in cleanup().
   */
  private ServerSocket serverSocket;

  // Used to hold current communication
  private Socket InputSocket;

  // This is the current transaction number we are working on
  private int transactionNumber = 0;

  /**
   * used to track the status of the stream processing. This should normally
   * count the number of input records which have been processed.
   */
  protected int InputRecordNumber = 0;

  /**
   * Default Constructor
   */
  public SocketInputAdapter() {
    super();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of inherited Input Adapter functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * Initialise the module. Called during pipeline creation. initialize input
   * adapter.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    String ConfigHelper;

    // Register ourself with the client manager
    super.init(PipelineName, ModuleName);

    // Now we load the properties
    // Get the port number
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(PipelineName, ModuleName, "ListenerPort");

    if (ConfigHelper == null || ConfigHelper.equals("0")) {
      message = "Please set the port number to listen on using the ListenerPort property";
      throw new InitializationException(message, getSymbolicName());
    }

    // see if we can convert it
    try {
      ListenerPort = Integer.parseInt(ConfigHelper);
    } catch (NumberFormatException nfe) {
      // Could not use the value we got
      message = "Could not parse the ListenerPort value <" + ConfigHelper + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Get successful response 
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(PipelineName, ModuleName, "onSuccessfulResponse");

    if (ConfigHelper != null) {
      onSuccessfulResponse = ConfigHelper;
    }
    
    // Get failed response
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(PipelineName, ModuleName, "onFailedResponse");

    if (ConfigHelper != null) {
      onFailedResponse = ConfigHelper;
    }   
    
    // Check the file name scanning variables, throw initialisation exception
    // if something is wrong.
    try {
      initSocket();
    } catch (IOException nfe) {
      // Could not use the value we got
      message = "Unable to open socket at specified port <" + ListenerPort + ">";
      throw new InitializationException(message, getSymbolicName());
    }

  }

  /**
   * loadBatch() is called regularly by the framework to either process records
   * or to scan for work to do, depending on whether we are already processing
   * or not.
   *
   * The way this works is that we assign a batch of files to work on, and then
   * work our way through them. This minimizes the directory scans that we have
   * to do and improves performance.
   *
   * @return
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  protected Collection<IRecord> loadBatch()
          throws ProcessingException {
    Collection<IRecord> Outbatch;
    int ThisBatchCounter = 0;

    // The Record types we will have to deal with
    HeaderRecord tmpHeader;
    TrailerRecord tmpTrailer;
    FlatRecord tmpDataRecord;
    IRecord batchRecord;
    Outbatch = new ArrayList<>();
    int batchCount = 0;
    boolean InTrans = false;

    InputRecordNumber = 0;

    // contine with the open file
    try {
      checkSocket();

      // we don't want the socket to block
      serverSocket.setSoTimeout(100);
      serverSocket.setReuseAddress(true);

      try {
        InputSocket = serverSocket.accept();
      } catch (SocketTimeoutException ste) {
        // There was nothing to process - just go back
        return Outbatch;
      }

      BufferedReader inputRecordStream = new BufferedReader(new InputStreamReader(InputSocket.getInputStream()));

      // read from the socket and prepare the batch
      while (InputSocket.isClosed() == false && (ThisBatchCounter < batchSize)) {
        String inputRecord;
        if (inputRecordStream.ready()) {
          inputRecord = inputRecordStream.readLine();
        } else {
        	// set failed response message to be sent to the client 
          sendSocketResponse(onFailedResponse);	
          break;
        }

        // skip blank records
        if (inputRecord != null && inputRecord.length() == 0) {
          continue;
        }
        
        // Handle the header and trailer
        switch (inputRecord) {
          case "HEADER":
            // create the transaction
            transactionNumber = createNewTransaction();
            InTrans = true;
            getPipeLog().info("opening trans " + transactionNumber);
            // Inform the transactional layer that we have started processing
            setTransactionProcessing(transactionNumber);
            // Inject a stream header record into the stream
            tmpHeader = new HeaderRecord();
            tmpHeader.setStreamName("SocketInput_" + transactionNumber);
            tmpHeader.setTransactionNumber(transactionNumber);
            // Pass the header to the user layer for any processing that
            // needs to be done
            tmpHeader = procHeader(tmpHeader);
            Outbatch.add(tmpHeader);
            batchCount = 0;
            break;
          case "TRAILER":
            // Inject a stream trailer record into the stream
            tmpTrailer = new TrailerRecord();
            tmpTrailer.setStreamName("SocketInput_" + transactionNumber);
            tmpTrailer.setTransactionNumber(transactionNumber);
            // Pass the header to the user layer for any processing that
            // needs to be done. To allow for purging in the case of record
            // compression, we allow mutiple calls to procTrailer until the
            // trailer is returned
            batchRecord = procTrailer(tmpTrailer);
            while (!(batchRecord instanceof TrailerRecord)) {
              // the call the trailer returned a purged record. Add this
              // to the batch and refetch
              Outbatch.add(batchRecord);
              batchRecord = procTrailer(tmpTrailer);
            }
            Outbatch.add(batchRecord);
            ThisBatchCounter++;
            
            // set successful response message to be sent to the client 
            sendSocketResponse(onSuccessfulResponse);
            
            // Close the socket
            inputRecordStream.close();
            InputSocket.close();
            // Notify the transaction layer that we have finished
            setTransactionFlushed(transactionNumber);
            InTrans = false;
            getPipeLog().info("flushed trans " + transactionNumber);
            // Remove the transaction from the list
            transactionNumber = 0;
            getPipeLog().info("Recevive batch count " + batchCount);
            break;
          default:
            // All other records
            // see if we have to abort - for this we just skip records until the
            // end of the stream
            if (transactionAbortRequest(transactionNumber)) {
              // if so, clear down everything that is not a header or a trailer
              // so we don't keep filling the pipe
              int originalCount = Outbatch.size();
              int discardCount = 0;
              Iterator<IRecord> discardIterator = Outbatch.iterator();

              while (discardIterator.hasNext()) {
                IRecord tmpRecord = discardIterator.next();
                if (((tmpRecord instanceof HeaderRecord) | (tmpRecord instanceof TrailerRecord)) == false) {
                  discardIterator.remove();
                  discardCount++;
                }
              }

              // if so, clear down the outbatch, so we don't keep filling the pipe
              getPipeLog().warning("Pipe <" + getSymbolicName() + "> discarded <" + discardCount + "> of <" + originalCount + "> input records, because of pending abort.");
            } else {
              if (InTrans == false) {
                System.err.println("Record when no trans");
              }
              ThisBatchCounter++;
              tmpDataRecord = new FlatRecord(inputRecord, InputRecordNumber);

              // Call the user layer for any processing that needs to be done
              batchRecord = procValidRecord(tmpDataRecord);

              // Add the prepared record to the batch, because of record compression
              // we may receive a null here. If we do, don't bother adding it
              if (batchRecord != null) {
                InputRecordNumber++;
                Outbatch.add(batchRecord);
              }
            }
            batchCount++;
            break;
        }
      }
    } catch (IOException ioex) {
      getPipeLog().fatal("Error reading socket. Message <" + ioex.getMessage() + ">");
    }

    return Outbatch;
  }
  
  /**
   * Send response to client
   * @param message
   */
  private void sendSocketResponse(String message) throws IOException
  {
	  if (message != null)
	  {
		  PrintWriter response = new PrintWriter(InputSocket.getOutputStream(), true); 
		  // Return response to the client
	      response.println(message);
		  response.close();
	  }
  }

  /**
   * Closes down the input stream after all the input has been collected
   *
   * @param TransactionNumber The transaction number of the transaction to close
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream(int TransactionNumber)
          throws ProcessingException {
//    try
//    {
//      serverSocket.close();
//    }
//    catch (IOException ex)
//    {
//      getPipeLog().error("Application is unable to close the Socket: '" + TransactionNumber +
//                "' ");
//      throw new ProcessingException("Application is unable to close the Socket : '" +
//                                    TransactionNumber + "' ", ex);
//    }
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract IRecord procValidRecord(FlatRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract IRecord procErrorRecord(FlatRecord r) throws ProcessingException;
  
  /**
   * Allows any records to be purged at the end of a file
   *
   * @return The pending record
   */
  public IRecord purgePendingRecord() {
    // default - do nothing
    return null;
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------
  /**
   *
   * @throws IOException
   */
  private void initSocket() throws IOException {

    this.serverSocket = new ServerSocket(this.ListenerPort);

    getPipeLog().info("Input Socket Initialized @ port: " + this.ListenerPort);
    System.out.println(getSymbolicName() + " Input Socket Initialized @ port <" + this.ListenerPort + ">");
  }

  /**
   * Checks the socket state if its not open, open it
   *
   * @throws IOException
   */
  private void checkSocket() throws IOException {
    if (serverSocket.isClosed()) {
      initSocket();
    }
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
  public int flushTransaction(int TransactionNumber) {
    return 0;
  }

  /**
   * Commit Transaction closes the transaction status with success
   *
   * @param TransactionNumber The transaction we are working on
   */
  @Override
  public void commitTransaction(int TransactionNumber) {
    // Nothing needed
  }

  /**
   * Commit Transaction closes the transaction status with failure
   *
   * @param TransactionNumber The transaction we are working on
   */
  @Override
  public void rollbackTransaction(int TransactionNumber) {
    // Nothing needed
  }

  /**
   * Close Transaction is the trigger to clean up transaction related
   * information such as variables, status etc.
   *
   * @param transactionNumber The transaction we are working on
   */
  @Override
  public void closeTransaction(int transactionNumber) {
    // Nothing needed
  }
}
