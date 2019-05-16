/* ====================================================================
 * SNOCS Notification Framework
 * ====================================================================
 */
package OpenRate.adapter.cassandra;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.IRecord;
import OpenRate.record.KeyValuePairRecord;
import OpenRate.utils.PropertyUtils;
import java.util.Collection;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * <p>
 * ActiveMQ Output Adapter.<br>
 *
 * This module writes records into a Cassandra key space.
 */
public abstract class AbstractCassandraOutputAdapter
        extends AbstractTransactionalOutputAdapter {

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
  private boolean OutputStreamOpen = false;

  // The Cassandra transport
  private TTransport tr;

  // The Casasandra client
  private Cassandra.Client client;

  /**
   * Initialise the module. Called during pipeline creation. Initialise the
   * Logger, and load the SQL statements.
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
    registerClientManager();

    // Register ourself with the client manager
    setSymbolicName(ModuleName);

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

    // initialise the valid queue producer
    try {
      initCassandraConnection();
    } catch (Exception ex) {
      throw new InitializationException("Error opening Cassandra connection <" + ex.getMessage() + ">", getSymbolicName());
    }
  }

  /**
   * Prepare good records for writing to the defined output stream.
   *
   * @param r The current record we are working on
   * @return The prepared record
   * @throws ProcessingException
   */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException {
    try {
      // We perform the sending here
      procValidRecord(r);
    } catch (ProcessingException pe) {
      // Pass the exception up
      String Message = "Processing exception preparing valid record in module <"
              + getSymbolicName() + ">. Message <" + pe.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(pe, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (Exception ex) {
      // Not good. Abort the transaction
      String Message = "Unexpected Exception preparing valid record in module <"
              + getSymbolicName() + ">. Message <" + ex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(Message, ex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    return r;
  }

  /**
   * Prepare bad records for writing to the defined output stream.
   *
   * @param r The current record we are working on
   * @return The prepared record
   * @throws ProcessingException
   */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException {
    try {
      // We perform the sending here
      procErrorRecord(r);
    } catch (ProcessingException pe) {
      // Pass the exception up
      String Message = "Processing exception preparing valid record in module <"
              + getSymbolicName() + ">. Message <" + pe.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(pe, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (Exception ex) {
      // Not good. Abort the transaction
      String Message = "Unexpected Exception preparing valid record in module <"
              + getSymbolicName() + ">. Message <" + ex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(Message, ex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

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
  public abstract Collection<KeyValuePairRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<KeyValuePairRecord> procErrorRecord(IRecord r) throws ProcessingException;

  // -----------------------------------------------------------------------------
  // ------------------ Custom connection management functions -------------------
  // -----------------------------------------------------------------------------

  /*
   * closeStream() is called by the pipeline when no more information comes
   * down it. We must perform a transaction state change here to FLUSHED
   */
  @Override
  public void closeStream(int TransactionNumber) {
    if (OutputStreamOpen) {
      setTransactionFlushed(TransactionNumber);
      OutputStreamOpen = false;
    }
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
   * When a transaction is started, the transactional layer calls this method to
   * see if we have any reason to stop the transaction being started, and to do
   * any preparation work that may be necessary before we start.
   *
   * @param transactionNumber The transaction to start
   * @return
   */
  @Override
  public int startTransaction(int transactionNumber) {
    // We do not have any reason to inhibit the transaction start, so return
    // the OK flag
    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are flushing the
   * transaction;
   *
   * @param transactionNumber The transaction to flush
   * @return
   */
  @Override
  public int flushTransaction(int transactionNumber) {
    // close the input stream
    closeStream(transactionNumber);

    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are committing the
   * transaction;
   *
   * @param transactionNumber The transaction to commit
   */
  @Override
  public void commitTransaction(int transactionNumber) {
    // Nothing
  }

  /**
   * Perform any processing that needs to be done when we are rolling back the
   * transaction;
   *
   * @param transactionNumber The transaction to rollback
   */
  @Override
  public void rollbackTransaction(int transactionNumber) {
    // Nothing
  }

  /**
   * Close Transaction is the trigger to clean up transaction related
   * information such as variables, status etc.
   *
   * Close down the statements we opened. Because the commit and rollback
   * statements are optional, we check if they have been defined before we ry to
   * close them.
   *
   * @param transactionNumber The transaction we are working on
   */
  @Override
  public void closeTransaction(int transactionNumber) {
    // Nothing
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
}
