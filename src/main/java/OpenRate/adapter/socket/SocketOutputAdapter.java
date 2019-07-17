
package OpenRate.adapter.socket;

import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
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
        implements IEventInterface {

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
  public SocketOutputAdapter() {
    super();
  }

  /**
   * Initialize the output adapter with the configuration that is to be used for
   * this instance of the adapter.
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
    // Get the host name
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(PipelineName, ModuleName, "HostName", "0");

    if (ConfigHelper.equals("0")) {
      message = "Please set the host name using the HostName property";
      throw new InitializationException(message, getSymbolicName());
    }
    this.HostName = ConfigHelper;
    // Get the port number
    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(PipelineName, ModuleName, "ListenerPort", "0");

    if (ConfigHelper.equals("0")) {
      message = "Please set the port number to listen on using the ListenerPort property";
      throw new InitializationException(message, getSymbolicName());
    }

    // see if we can convert it
    try {
      this.ListenerPort = Integer.parseInt(ConfigHelper);
    } catch (NumberFormatException nfe) {
      // Could not use the value we got
      message = "Could not parse the ListenerPort value <" + ConfigHelper + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Check the file name scanning variables, throw initialisation exception
    // if something is wrong.
    try {
      initSocket();
    } catch (IOException nfe) {
      // Could not use the value we got
      message = "Unable to open socket at host <" + HostName + "> with specified port <" + ListenerPort + ">";
      throw new InitializationException(message, getSymbolicName());
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
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException {
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
  public IRecord prepValidRecord(IRecord r) throws ProcessingException {
    Collection<IRecord> outRecCol;
    FlatRecord outRec;
    Iterator<IRecord> outRecIter;

    outRecCol = procValidRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      outRecIter = outRecCol.iterator();
      if (isSocketValid()) {
        try {
          OutputRecord = new PrintWriter(new BufferedOutputStream(OutputSocket.getOutputStream(), 1024), false);
        } catch (IOException ioe) {
          this.getExceptionHandler().reportException(new ProcessingException(ioe, getSymbolicName()));
        }
        while (outRecIter.hasNext()) {
          outRec = (FlatRecord) outRecIter.next();
          OutputRecord.write(outRec.getData());
          OutputRecord.write(REPLY_RECORD_TERMINATOR);
          OutputRecord.flush();
        }
//        closeStream();
      } else {
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
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException {
    Collection<IRecord> outRecCol;
    FlatRecord outRec;
    Iterator<IRecord> outRecIter;

    outRecCol = procErrorRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      outRecIter = outRecCol.iterator();
      if (isSocketValid()) {
        try {
          OutputRecord = new PrintWriter(new BufferedOutputStream(OutputSocket.getOutputStream(), 1024), false);
        } catch (IOException ioe) {
          this.getExceptionHandler().reportException(new ProcessingException(ioe, getSymbolicName()));
        }

        while (outRecIter.hasNext()) {
          outRec = (FlatRecord) outRecIter.next();
          OutputRecord.write(outRec.getData());
          OutputRecord.write(REPLY_RECORD_TERMINATOR);
          OutputRecord.flush();
        }
//        closeStream();
      } else {
        r = null;
      }
    }

    return r;
  }

  /**
   * Process the stream trailer. Get the file base name and open the
   * transaction.
   *
   * @param r The current record we are working on
   * @return The prepared record
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {

    // Do the transaction level maintenance
    super.procTrailer(r);

    return r;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here. Note that the result is a collection for the case that we
   * have to re-expand after a record compression input adapter has done
   * compression on the input stream.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<IRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<IRecord> procErrorRecord(IRecord r) throws ProcessingException;

  /**
   *
   * @throws IOException
   */
  private void initSocket()
          throws IOException {
    if (OutputSocket == null || OutputSocket.isConnected() == false) {
      OutputSocket = new Socket(this.HostName, this.ListenerPort);
      getPipeLog().info("Input Socket Initialized @ host: " + this.HostName + " port: " + this.ListenerPort);
    }
  }

  /**
   *
   * @return
   */
  private boolean isSocketValid() {
    int tmpTries = 0;
    while (tmpTries < MAX_SOCKET_RETRIES) {
      try {
        initSocket();
        return true;
      } catch (IOException ex) {
        tmpTries++;
        getPipeLog().info("Input Socket NOT Initialized @ host: " + this.HostName + " port: " + this.ListenerPort + " Try #: " + tmpTries);
      }

    }

    return false;
  }

  private void closeStream() {
    OutputRecord.close();
  }

  /**
   * Start transaction opens the transaction
   *
   * @param TransactionNumber The transaction we are working on
   * @return 0 if the transaction started OK
   */
  @Override
  public int startTransaction(int TransactionNumber) {
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

  /**
   *
   * @param TransactionNumber
   */
  @Override
  public void closeStream(int TransactionNumber) {
    // Nothing needed
  }

}
