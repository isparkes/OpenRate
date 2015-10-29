/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
package OpenRate.adapter.cassandra;

import OpenRate.adapter.jdbc.*;
import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.KeyValuePairRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=JDBC_Input_Adapter'>click
 * here</a> to go to wiki page.
 *
 * <p>
 * Abstract Cassandra InputAdapter.<br> Read records from a Cassandra input key
 * space, and process them.
 *
 * In order to read records from the key space, we have to have a strategy for
 * locating records and presenting them to the pipeline.
 */
public abstract class AbstractCassandraInputAdapter
        extends AbstractTransactionalInputAdapter {

  /**
   * The name of the queue we are scanning.
   */
  protected String cassandraIPAddr = null;

  /**
   * The name of the host where the queue is.
   */
  protected int cassandraPort = 0;

  /**
   * The port where the queue is.
   */
  protected String cassandraUserName = null;

  /**
   * The name of the queue we are scanning.
   */
  protected String cassandraPassword = null;

  // List of Services that this Client supports
  private static final String SERVICE_CASSANDRA_IP_ADDR = "CassandraIPAddress";
  private static final String SERVICE_CASSANDRA_PORT = "CassandraPort";
  private static final String SERVICE_CASSANDRA_USER_NAME = "CassandraUserName";
  private static final String SERVICE_CASSANDRA_PASSWORD = "CassandraPassword";

  // This tells us if we should look for new work or continue with something
  // that is going on at the moment
  private final boolean OutputStreamOpen = false;

  // The Cassandra transport
  private TTransport tr;

  // The Casasandra client
  private Cassandra.Client client;

  // This tells us if we should look for new work or continue with something
  // that is going on at the moment
  private boolean InputStreamOpen = false;

  // used to track the status of our transaction
  private int transactionNumber = 0;
  private int InputRecordNumber = 0;

  /**
   * Holds the time stamp for the transaction
   */
  protected String ORTransactionId = null;

  /**
   * Default Constructor
   */
  public AbstractCassandraInputAdapter() {
    super();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * Initialise the module. Called during pipeline creation.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    String ConfigHelper;

    super.init(PipelineName, ModuleName);

    // get the IP address of the SMSC
    ConfigHelper = initCassandraIPAddress();
    processControlEvent(SERVICE_CASSANDRA_IP_ADDR, true, ConfigHelper);

    // get the port for the SMSC
    ConfigHelper = initCassandraPort();
    processControlEvent(SERVICE_CASSANDRA_PORT, true, ConfigHelper);

    // get the user name
    ConfigHelper = initCassandraUserName();
    processControlEvent(SERVICE_CASSANDRA_USER_NAME, true, ConfigHelper);

    // get the user name
    ConfigHelper = initCassandraPassword();
    processControlEvent(SERVICE_CASSANDRA_PASSWORD, true, ConfigHelper);

    // initialise the connection as a test
    try {
      initCassandraConnection();
    } catch (Exception ex) {
      throw new InitializationException("Error opening Cassandra connection <" + ex.getMessage() + ">", getSymbolicName());
    }

    // Close it again
    closeCassandraConnection();
  }

  /**
   * Retrieve all the source records that should be processed by the pipeline.
   *
   * @return
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  protected Collection<IRecord> loadBatch() throws ProcessingException {
    Collection<IRecord> Outbatch;
    int ThisBatchCounter = 0;

    // The Record types we will have to deal with
    HeaderRecord tmpHeader;
    TrailerRecord tmpTrailer;
    KeyValuePairRecord tmpRecord;
    IRecord batchRecord;

    getPipeLog().debug("loadBatch()");
    Outbatch = new ArrayList<>();

    // This layer deals with opening the stream if we need to
    if (InputStreamOpen == false) {
      // Check to see if there is any new work to do
      if (canStartNewTransaction() && (getInputAvailable() > 0)) {
        // There is work to do, we execute the Init SQL so that we have the chance to
        // prepare the data for reading
        assignInput();

        // the renamed file provided by assignInput
        try {
          // Get the data we want to process

          // See if we get an empty result set
          // ToDo Just so we can compile it
          int numberOfRows = 0;

          if (numberOfRows > 0) {
            // Create the new transaction to hold the information. This is done in
            // The transactional layer - we just trigger it here
            // Create the transaction base name according to a simple counter
            transactionNumber = createNewTransaction();

            // This is the transaction identifier for all records in this stream
            ORTransactionId = getTransactionID(transactionNumber);

            // reset the cursor
            //ToDo: gotoFirstRow();
            InputStreamOpen = true;
            InputRecordNumber = 0;

            // Inform the transactional layer that we have started processing
            setTransactionProcessing(transactionNumber);

            // Inject a stream header record into the stream
            tmpHeader = new HeaderRecord();
            tmpHeader.setStreamName(ORTransactionId);
            tmpHeader.setTransactionNumber(transactionNumber);

            // Increment the stream counter
            incrementStreamCount();

            // Pass the header to the user layer for any processing that
            // needs to be done
            tmpHeader = procHeader(tmpHeader);
            Outbatch.add(tmpHeader);
          } else {
            message = "Select statement did not return rows in <" + getSymbolicName() + ">";
            getPipeLog().error(message);

            // No work to do - return the empty batch
            return Outbatch;
          }
        } catch (Exception ex) {
          message = "Select SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
          getPipeLog().error(message);

          // Stop any transactions that are open
          if (transactionNumber > 0) {
            cancelTransaction(transactionNumber);
          }

          // Close statement and connection
          //ToDo: closeSelectStatement();
          // report the exception
          throw new ProcessingException(message, getSymbolicName());
        }
      }
    }

    if (InputStreamOpen) {
      try {
        // ToDo: Get it to compile
        boolean isLast = false;

        while ((ThisBatchCounter < batchSize) & (!isLast)) {
          // get next row
          //ToDo: next();

          ThisBatchCounter++;

          // create the record
          tmpRecord = new KeyValuePairRecord();

          // Call the user layer for any processing that needs to be done
          batchRecord = procValidRecord(tmpRecord);

          // Add the prepared record to the batch, because of record compression
          // we may receive a null here. If we do, don't bother adding it
          if (batchRecord != null) {
            InputRecordNumber++;

            Outbatch.add(batchRecord);
          }
        }

        // see if we have to abort
        if (transactionAbortRequest(transactionNumber)) {
          // if so, clear down the out batch, so we don't keep filling the pipe
          getPipeLog().warning("Pipe <" + getSymbolicName() + "> discarded <" + Outbatch.size() + "> input records, because of pending abort.");
          Outbatch.clear();
        }

        // Keep track of the records
        updateRecordCount(transactionNumber, InputRecordNumber);
      } catch (Exception ex) {
        message = "Retrieve SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
        getPipeLog().error(message);
        throw new ProcessingException(message, getSymbolicName());
      }

      // See if we need to add a stream trailer record - this is done immediately
      // after the last real record of the stream
      try {
        // see the reason that we closed
        // ToDo: Get it to compile
        boolean isLast = false;
        if (isLast) {
          // we have finished
          InputStreamOpen = false;

          // Inject a stream header record into the stream
          tmpTrailer = new TrailerRecord();
          tmpTrailer.setStreamName(ORTransactionId);
          tmpTrailer.setTransactionNumber(transactionNumber);

          // Pass the header to the user layer for any processing that
          // needs to be done. To allow for purging in the case of record
          // compression, we allow multiple calls to procTrailer until the
          // trailer is returned
          batchRecord = procTrailer(tmpTrailer);

          while (!(batchRecord instanceof TrailerRecord)) {
            // the call the trailer returned a purged record. Add this
            // to the batch and fetch again
            Outbatch.add(batchRecord);
            batchRecord = procTrailer(tmpTrailer);
          }

          Outbatch.add(tmpTrailer);

          // Notify the transaction layer that we have finished
          setTransactionFlushed(transactionNumber);

          // Close the connection
          // Connection will be closed after commit or rollback
          //ToDo: closeSelectStatement();
        }
      } catch (Exception ex) {
        message = "Close SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
        getPipeLog().error(message);
        throw new ProcessingException(message, getSymbolicName());
      }
    }

    return Outbatch;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract IRecord procValidRecord(KeyValuePairRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract IRecord procErrorRecord(KeyValuePairRecord r) throws ProcessingException;
  
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
  // --------------- Start of overridable processing  functions ------------------
  // -----------------------------------------------------------------------------

  /**
   * Get the transaction id for the transaction. Intended to be overwritten in
   * the case that you want another transaction ID format.
   *
   * @param transactionNumber The number of the transaction
   * @return The calculated transaction id
   */
  public String getTransactionID(int transactionNumber) {
    return "" + new Date().getTime();
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * processControlEvent is the event processing hook for the External Control
   * Interface (ECI). This allows interaction with the external world, for
   * example turning the dumping on and off.
   *
   * @param Command The command that we are to work on
   * @param Init True if the pipeline is currently being constructed
   * @param Parameter The parameter value for the command
   * @return The result message of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init,
          String Parameter) {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_CASSANDRA_IP_ADDR)) {
      if (Init) {
        cassandraIPAddr = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return cassandraIPAddr;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_CASSANDRA_PORT)) {
      if (Init) {
        cassandraPort = Integer.parseInt(Parameter);
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return String.valueOf(cassandraPort);
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_CASSANDRA_USER_NAME)) {
      if (Init) {
        cassandraUserName = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return cassandraUserName;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_CASSANDRA_PASSWORD)) {
      if (Init) {
        cassandraPassword = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return cassandraPassword;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (ResultCode == 0) {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    } else {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

  /**
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CASSANDRA_IP_ADDR, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CASSANDRA_PORT, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CASSANDRA_USER_NAME, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CASSANDRA_PASSWORD, ClientManager.PARAM_MANDATORY);
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * Perform any processing that needs to be done when we are flushing the
   * transaction
   *
   * @param TransactionNumber
   * @return 0 if everything flushed OK, otherwise -1
   */
  @Override
  public int flushTransaction(int TransactionNumber) {
    try {
      // close the input stream
      closeStream();
    } catch (ProcessingException ex) {
      return -1;
    }

    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are committing the
   * transaction
   *
   * @param transactionNumber The transaction to commit
   */
  @Override
  public void commitTransaction(int transactionNumber) {
    try {
      CommitStream(transactionNumber);
    } catch (ProcessingException ex) {
      Logger.getLogger(JDBCInputAdapter.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  /**
   * Perform any processing that needs to be done when we are rolling back the
   * transaction;
   *
   * @param transactionNumber The transaction to roll back
   */
  @Override
  public void rollbackTransaction(int transactionNumber) {
    try {
      RollbackStream(transactionNumber);
    } catch (ProcessingException ex) {
      Logger.getLogger(JDBCInputAdapter.class.getName()).log(Level.SEVERE, null, ex);
    }
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

  // -----------------------------------------------------------------------------
  // ----------------- Start of inherited IAdapter functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * Close all statements and perform clean up
   */
  @Override
  public void cleanup() {
    getPipeLog().debug("JDBCInputAdapter running cleanup");

    // Close the statements and connections
    //ToDo: closeConnection();
    super.cleanup();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Custom connection management functions -------------------
  // -----------------------------------------------------------------------------
  /**
   * Tries to connect to Cassandra.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void openCassandraConnection() throws ProcessingException {
    tr = new TFramedTransport(new TSocket(cassandraIPAddr, 9160));
    TProtocol proto = new TBinaryProtocol(tr);
    client = new Cassandra.Client(proto);

    try {
      tr.open();
    } catch (TTransportException ex) {
      message = "Transport exception opening Cassandra transport";
      throw new ProcessingException(message, ex, getSymbolicName());
    }
  }

  /**
   * Tries to connect to Cassandra.
   */
  public void closeCassandraConnection() {
    tr.close();
  }

  /**
   * @return the client
   */
  public Cassandra.Client getClient() {
    return client;
  }

  /**
   * @param client the client to set
   */
  public void setClient(Cassandra.Client client) {
    this.client = client;
  }

  // -----------------------------------------------------------------------------
  // ----------------- Stream opening and closing functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * getInputAvailable performs the count query to see the number of records
   * that are candidates for processing
   */
  private int getInputAvailable() throws ProcessingException {
    int InputAvail = 0;

    try {
      // prepare the count query
      //ToDo: prepareCountStatement();

      //ToDo: if (stmtCountQuery.execute())
      {
        //ToDo: Trs = stmtCountQuery.getResultSet();

        //ToDo: if (next())
        {
          //ToDo. WorkingResult = Trs.getString(1);
          //ToDo: InputAvail = Integer.parseInt(WorkingResult);

          // Log what we have got
          if (InputAvail > 0) {
            OpenRate.getOpenRateStatsLog().info("Input  <" + getSymbolicName() + "> found <" + InputAvail + "> events for processing");
          }
        }

        //ToDo: Trs.close();
      }

      // close the statement
      //ToDo: closeCountStatement();
    } catch (Exception ex) {
      message = "Count SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      getPipeLog().error(message);
      throw new ProcessingException(message, getSymbolicName());
    }

    return InputAvail;
  }

  /**
   * assignInput performs the init query to mark the input records that we are
   * going to process. This means that any records that arrive after the
   * processing has started will not be included in this transaction, but
   * instead will have to wait for a later transaction
   */
  private void assignInput() throws ProcessingException {
    try {
      // prepare the statement
      //ToDo: prepareInitStatement();

      // Execute it
      //ToDo: stmtInitQuery.execute();
      // Close the statement
      //ToDo: closeInitStatement();
    } catch (Exception ex) {
      message = "Init SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      getPipeLog().error(message);
      throw new ProcessingException(message, getSymbolicName());
    }
  }

  /**
   * Closes down the input stream after all the input has been collected and
   * informs the transaction manager
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream()
          throws ProcessingException {
    // close down the result set now that we have read everything
    //ToDo: if (rs != null)
    {
      try {
        //ToDo: rs.close();
      } catch (Exception ex) {
        message = "SQL Exception closing resultset in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
        getPipeLog().error(message);
        throw new ProcessingException(message, getSymbolicName());
      }
    }
  }

  /**
   * Commit stream performs the commit query to fix the data
   *
   * @param TransactionNumber The transaction number we are working on
   * @throws ProcessingException
   */
  public void CommitStream(int TransactionNumber) throws ProcessingException {
    try {
      // prepare the statement
      //ToDo: prepareCommitRollbackStatement();

      // deinit the records so that we don't have to read them ever again
      //ToDo: if(stmtCommitQuery != null){
      //ToDo:   perfomCommit();
      //ToDo: }
      // Close down the connection and return to the pool
      //ToDo: closeCommitRollbackStatement();
    } catch (Exception ex) {
      message = "Commit SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      getPipeLog().error(message);
      throw new ProcessingException(message, getSymbolicName());
    }
  }

  /**
   * Rollback stream performs the rollback query to remove the data
   *
   * @param TransactionNumber The transaction number we are working on
   * @throws ProcessingException
   */
  public void RollbackStream(int TransactionNumber) throws ProcessingException {
    try {
      // prepare the statement
      //ToDo: prepareCommitRollbackStatement();

       // deinit the records so that we don't have to read them ever again
      //ToDo: if(stmtRollbackQuery != null){
      //ToDo:   perfomRollback();
      //ToDo: }
      // Close down the connection and return to the pool
      //ToDo: closeCommitRollbackStatement();
    } catch (Exception ex) {
      message = "Rollback SQL Exception in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      getPipeLog().error(message);
      throw new ProcessingException(message, getSymbolicName());
    }
  }

  /**
   * Overridable commit block for allowing the addition of parameters
   *
   * @throws SQLException
   */
  public void perfomCommit() throws SQLException {
    //ToDo: stmtCommitQuery.execute();
  }

  /**
   * Overrideable rollback block for allowing the addition of parameters
   *
   * @throws SQLException
   */
  public void perfomRollback() throws SQLException {
    //ToDo: stmtRollbackQuery.execute();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of custom initialisation functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * The initValidQueueName gets the name of the valid output queue.
   *
   * @return The query string
   * @throws OpenRate.exception.InitializationException
   */
  public String initCassandraIPAddress() throws InitializationException {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_CASSANDRA_IP_ADDR,
            "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None")) {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <" + SERVICE_CASSANDRA_IP_ADDR + "> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message, getSymbolicName());
    }

    return configHelper;
  }

  /**
   * The initValidQueueName gets the name of the valid output queue.
   *
   * @return The query string
   * @throws OpenRate.exception.InitializationException
   */
  public String initCassandraPort() throws InitializationException {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_CASSANDRA_PORT,
            "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None")) {
      message = "Output <" + getSymbolicName() + "> - config parameter <" + SERVICE_CASSANDRA_PORT + "> not found";
      getPipeLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // check it is numeric
    try {
      cassandraPort = Integer.parseInt(configHelper);
    } catch (NumberFormatException ex) {
      message = "Output <" + getSymbolicName() + "> - config parameter <" + SERVICE_CASSANDRA_PORT + "> not numeric";
      getPipeLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    return configHelper;
  }

  /**
   * The initSMSCUserName gets the user name for logging into the SMSC with.
   *
   * @return The query string
   * @throws OpenRate.exception.InitializationException
   */
  public String initCassandraUserName() throws InitializationException {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_CASSANDRA_USER_NAME,
            "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None")) {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <" + SERVICE_CASSANDRA_USER_NAME + "> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message, getSymbolicName());
    }

    return configHelper;
  }

  /**
   * The initSMSCPassword gets the password for logging into the SMSC with.
   *
   * @return The query string
   * @throws OpenRate.exception.InitializationException
   */
  public String initCassandraPassword() throws InitializationException {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_CASSANDRA_PASSWORD,
            "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None")) {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <" + SERVICE_CASSANDRA_USER_NAME + "> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message, getSymbolicName());
    }

    return configHelper;
  }

  /**
   * Tries to connect to Cassandra.
   *
   * @throws OpenRate.exception.InitializationException
   */
  public void initCassandraConnection() throws InitializationException {
    tr = new TFramedTransport(new TSocket(cassandraIPAddr, cassandraPort));
    TProtocol proto = new TBinaryProtocol(tr);
    client = new Cassandra.Client(proto);

    try {
      tr.open();
    } catch (TTransportException ex) {
      message = "Transport exception opening Cassandra transport";
      throw new InitializationException(message, ex, getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------------- Start of custom functions ----------------------------
  // -----------------------------------------------------------------------------
  /**
   * Create a byte buffer for insert
   *
   * @param value the string to convert
   * @return the converted byte buffer
   */
  private ByteBuffer toByteBuffer(String value) {
    try {
      return ByteBuffer.wrap(value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException ex) {
      // try without encoding
      return ByteBuffer.wrap(value.getBytes());
    }
  }

  private void insertColumnValue(String parentName, String id, String name, String value, long ts) throws TException, TimedOutException, UnavailableException, InvalidRequestException, UnsupportedEncodingException {
    ColumnParent parent = new ColumnParent(parentName);
    Column column = new Column(toByteBuffer(name));
    column.setValue(toByteBuffer(value));
    column.setTimestamp(ts);
    getClient().insert(toByteBuffer(id), parent, column, ConsistencyLevel.ONE);
  }
}
