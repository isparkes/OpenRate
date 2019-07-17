package OpenRate.adapter.jms;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.IRecord;
import OpenRate.record.QueueMessageRecord;
import OpenRate.utils.PropertyUtils;
import java.util.Collection;
import java.util.Iterator;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * <p>ActiveMQ Output Adapter.<br>
 * 
 * This module writes records into an Active MQ Queue.
 */
public abstract class AbstractAMQOutputAdapter
  extends AbstractTransactionalOutputAdapter
{
  /**
   * The type of the queue we are scanning.
   */
  private String validQueueType = "";
  
  /**
   * The name of the queue we are scanning.
   */
  private String validQueueName = null;

  /**
   * The name of the host where the queue is.
   */
  private String validQueueHost = null;

  /**
   * The port where the queue is.
   */
  private String validQueuePort = null;

  /**
   * The name of the queue we are scanning.
   */
  private String errorQueueType = "";

  /**
   * The name of the queue we are scanning.
   */
  private String errorQueueName = null;

  /**
   * The name of the host where the queue is.
   */
  private String errorQueueHost = null;

  /**
   * The port where the queue is.
   */
  private String errorQueuePort = null;
  
  // javax.jms.Session
  private Session validSession;

  // javax.jms.MessageConsumer
  private MessageProducer validProducer;

  // The destination
  private ActiveMQDestination validDestination;

  // The connection factory
  private ActiveMQConnectionFactory validConnectionFactory;

  // and the connection
  private javax.jms.Connection validConnection;

  // javax.jms.Session
  private Session errorSession;

  // javax.jms.MessageConsumer
  private MessageProducer errorProducer;

  // The destination
  private ActiveMQDestination errorDestination;

  // The connection factory
  private ActiveMQConnectionFactory errorConnectionFactory;

  // and the connection
  private javax.jms.Connection errorConnection;

  // List of Services that this Client supports
  private static final String SERVICE_VALID_Q_TYPE  = "ValidQueueType";
  private static final String SERVICE_VALID_Q_NAME  = "ValidQueueName";
  private static final String SERVICE_VALID_Q_HOST  = "ValidQueueHost";
  private static final String SERVICE_VALID_Q_PORT  = "ValidQueuePort";
  private static final String SERVICE_ERROR_Q_TYPE  = "ErrorQueueType";
  private static final String SERVICE_ERROR_Q_NAME  = "ErrorQueueName";
  private static final String SERVICE_ERROR_Q_HOST  = "ErrorQueueHost";
  private static final String SERVICE_ERROR_Q_PORT  = "ErrorQueuePort";
  private final static String SERVICE_SINGLE_OUTPUT = "SingleOutputQueue";

  // The types of queues we are using
  private static final String SERVICE_Q_TYPE_QUEUE = "QUEUE";
  private static final String SERVICE_Q_TYPE_TOPIC = "TOPIC";

  // This tells us if we should look for new work or continue with something
  // that is going on at the moment
  private boolean OutputStreamOpen = false;
  
  // If we are using a single writer
  private boolean          singleWriter     = false;
  
  /**
   * Default constructor
   */
  public AbstractAMQOutputAdapter()
  {
    super();
  }

 /**
  * Initialise the module. Called during pipeline creation.
  * Initialise the Logger, and load the SQL statements.
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
    super.init(PipelineName, ModuleName);
    registerClientManager();

    // Register ourself with the client manager
    setSymbolicName(ModuleName);

    // get the queue type for the valid queue
    ConfigHelper = initValidQueueType();
    processControlEvent(SERVICE_VALID_Q_TYPE, true, ConfigHelper);

    // get the queue name for the valid queue
    ConfigHelper = initValidQueueName();
    processControlEvent(SERVICE_VALID_Q_NAME, true, ConfigHelper);

    // get the queue host for the valid queue
    ConfigHelper = initValidQueueHost();
    processControlEvent(SERVICE_VALID_Q_HOST, true, ConfigHelper);

    // get the queue port for the valid queue
    ConfigHelper = initValidQueuePort();
    processControlEvent(SERVICE_VALID_Q_PORT, true, ConfigHelper);

    // initialise the valid queue producer
    initValidQueueProducer();
    
    // See if we have an error queue to manage
    ConfigHelper = initSingleQueueOutput();
    processControlEvent(SERVICE_SINGLE_OUTPUT, true, ConfigHelper);
    
    if (singleWriter)
    {
      // Single writer defined
      String Message = "Using Single Output for Adapter <" + getSymbolicName() + ">";
      getPipeLog().info(Message);
    }
    else
    {
      // get the queue type for the error queue
      ConfigHelper = initErrorQueueType();
      processControlEvent(SERVICE_ERROR_Q_TYPE, true, ConfigHelper);

      // get the queue name for the error queue
      ConfigHelper = initErrorQueueName();
      processControlEvent(SERVICE_ERROR_Q_NAME, true, ConfigHelper);

      // get the queue host for the error queue
      ConfigHelper = initErrorQueueHost();
      processControlEvent(SERVICE_ERROR_Q_HOST, true, ConfigHelper);

      // get the queue port for the valid queue
      ConfigHelper = initErrorQueuePort();
      processControlEvent(SERVICE_ERROR_Q_PORT, true, ConfigHelper);
      
      // initialise the error queue producer
      initErrorQueueProducer();
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
  public IRecord prepValidRecord(IRecord r) throws ProcessingException
  {
    int i;
    Collection<QueueMessageRecord> outRecCol = null;
    QueueMessageRecord  outRec;
    Iterator<QueueMessageRecord>   outRecIter;

    try
    {
      outRecCol = procValidRecord(r);
    }
    catch (ProcessingException pe)
    {
      // Pass the exception up
      String Message = "Processing exception preparing valid record in module <" +
                       getSymbolicName() + ">. Message <" + pe.getMessage() +
                       ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(pe,getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }
    catch (Exception ex)
    {
      // Not good. Abort the transaction
      String Message = "Unexpected Exception preparing valid record in module <" +
                        getSymbolicName() + ">. Message <" + ex.getMessage() + 
                        ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(Message,ex,getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    // Null return means "do not bother to process"
    if (outRecCol != null)
    {
      outRecIter = outRecCol.iterator();
      while (outRecIter.hasNext())
      {
        outRec = outRecIter.next();
        
        try
        {  
          validProducer.send(outRec.getData());
        }
        catch (Exception ex)
        {
          // Not good. Abort the transaction
          String Message = "Unknown Exception inserting valid record in module <" +
                          getSymbolicName() + ">. Message <" + ex.getMessage() +
                          ">. Aborting transaction.";
          getPipeLog().fatal(Message);

          getExceptionHandler().reportException(new ProcessingException(Message,ex,getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        }
      }
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
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException
  {
    int i;
    Collection<QueueMessageRecord> outRecCol = null;
    QueueMessageRecord  outRec;
    Iterator<QueueMessageRecord>   outRecIter;

    try
    {
      outRecCol = procErrorRecord(r);
    }
    catch (ProcessingException pe)
    {
      // Pass the exception up
      String Message = "Processing exception preparing error record in module <" +
                       getSymbolicName() + ">. Message <" + pe.getMessage() +
                       ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(pe,getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      // Not good. Abort the transaction
      String Message = "Column Index preparing error record in module <" +
                       getSymbolicName() + ">. Message <" + ex.getMessage() +
                       ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(Message,ex,getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }
    catch (Exception ex)
    {
      // Not good. Abort the transaction
      String Message = "Unknown Exception preparing error record in module <" +
                        getSymbolicName() + ">. Message <" + ex.getMessage() +
                        ">. Aborting transaction.";
      getPipeLog().fatal(Message);
      getExceptionHandler().reportException(new ProcessingException(Message,ex,getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    // Null return means "do not bother to process"
    if (outRecCol != null)
    {
      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext())
      {
        outRec = outRecIter.next();

        try
        {
          if (singleWriter)
          {
            validProducer.send(outRec.getData());
          }
          else
          {
            errorProducer.send(outRec.getData());
          }
        }
        catch (Exception ex)
        {
          // Not good. Abort the transaction
          String Message = "Unknown Exception inserting error record in module <" +
                          getSymbolicName() + ">. Message <" + ex.getMessage() +
                          ">. Aborting transaction.";
          getPipeLog().fatal(Message);
          getExceptionHandler().reportException(new ProcessingException(Message,ex,getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        }
      }
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
  public abstract Collection<QueueMessageRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<QueueMessageRecord> procErrorRecord(IRecord r) throws ProcessingException;

  // -----------------------------------------------------------------------------
  // ------------------ Custom connection management functions -------------------
  // -----------------------------------------------------------------------------

  /*
   * closeStream() is called by the pipeline when no more information comes
   * down it. We must perform a transaction state change here to FLUSHED
   */
  @Override
  public void closeStream(int TransactionNumber)
  {
    if (OutputStreamOpen)
    {
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
                                    String Parameter)
  {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_VALID_Q_TYPE))
    {
      if (Init)
      {
        if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_QUEUE))
        {
          validQueueType = SERVICE_Q_TYPE_QUEUE;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_TOPIC))
        {
          validQueueType = SERVICE_Q_TYPE_TOPIC;
          ResultCode = 0;
        }
      }
      else
      {
        if (Parameter.equals(""))
        {
          return validQueueType;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERROR_Q_TYPE))
    {
      if (Init)
      {
        if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_QUEUE))
        {
          errorQueueType = SERVICE_Q_TYPE_QUEUE;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_TOPIC))
        {
          errorQueueType = SERVICE_Q_TYPE_TOPIC;
          ResultCode = 0;
        }
      }
      else
      {
        if (Parameter.equals(""))
        {
          return errorQueueType;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_VALID_Q_NAME))
    {
      if (Init)
      {
        setValidQueueName(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getValidQueueName();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERROR_Q_NAME))
    {
      if (Init)
      {
        setErrorQueueName(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getErrorQueueName();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_VALID_Q_HOST))
    {
      if (Init)
      {
        setValidQueueHost(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getValidQueueHost();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERROR_Q_HOST))
    {
      if (Init)
      {
        setErrorQueueHost(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getErrorQueueHost();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_VALID_Q_PORT))
    {
      if (Init)
      {
        setValidQueuePort(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getValidQueuePort();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERROR_Q_PORT))
    {
      if (Init)
      {
        setErrorQueuePort(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getErrorQueuePort();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_SINGLE_OUTPUT))
    {
      if (Init)
      {
        singleWriter = Boolean.parseBoolean(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return Boolean.toString(singleWriter);
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }
    
    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

  /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_VALID_Q_NAME,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_VALID_Q_HOST,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_VALID_Q_PORT,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERROR_Q_NAME,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERROR_Q_HOST,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERROR_Q_PORT,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_SINGLE_OUTPUT,ClientManager.PARAM_NONE);
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
  */
  @Override
  public int startTransaction(int transactionNumber)
  {
    // We do not have any reason to inhibit the transaction start, so return
    // the OK flag
    return 0;
  }

 /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction;
  * 
  * @param transactionNumber The transaction to flush
  */
  @Override
  public int flushTransaction(int transactionNumber)
  {
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
  public void commitTransaction(int transactionNumber)
  {
    // Nothing
  }

  /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction;
  * 
  * @param transactionNumber The transaction to rollback
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    // Nothing
  }

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * Close down the statements we opened. Because the commit and rollback
  * statements are optional, we check if they have been defined before we ry
  * to close them.
  * 
  * @param transactionNumber The transaction we are working on
  */
  @Override
  public void closeTransaction(int transactionNumber)
  {
    // Nothing
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of custom initialisation functions ---------------------
  // -----------------------------------------------------------------------------

 /**
   * Tries to connect to the queue for valid events, and create a producer to it.
   *
   * @return true if the queue was initialised correctly, otherwise false
   */
  private boolean initValidQueueProducer() throws InitializationException
  {
    String url = "tcp://"+getValidQueueHost()+":"+getValidQueuePort();
    getPipeLog().info("start message listener on <" + url + ">");

    validConnectionFactory = new ActiveMQConnectionFactory(url);

    try {
      validConnection = validConnectionFactory.createConnection();
    } catch (JMSException ex) {
      throw new InitializationException("Could not create connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      validConnection.start();
    } catch (JMSException ex) {
      throw new InitializationException("Could not start connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      setValidSession(validConnection.createSession(false, Session.AUTO_ACKNOWLEDGE));
    } catch (JMSException ex) {
      throw new InitializationException("Could not create session <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      // Set the destination
      switch (getValidQueueType()) {
        case SERVICE_Q_TYPE_QUEUE:
          validDestination = (ActiveMQQueue) getValidSession().createQueue(getValidQueueName());
          break;
        case SERVICE_Q_TYPE_TOPIC:
          validDestination = (ActiveMQTopic) getValidSession().createTopic(getValidQueueName());
          break;
      }      
    } catch (JMSException ex) {
      throw new InitializationException("Could not create queue destination <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      validProducer = getValidSession().createProducer(validDestination);
    } catch (JMSException ex) {
      throw new InitializationException("Could not create consumer <" + ex.getMessage() + ">",getSymbolicName());
    }

    return true;
  }
  
 /**
   * Tries to connect to the queue for error events, and create a producer to it.
   *
   * @return true if the queue was initialised correctly, otherwise false
   */
  private boolean initErrorQueueProducer() throws InitializationException
  {
    String url = "tcp://"+getErrorQueueHost()+":"+getErrorQueuePort();
    getPipeLog().info("start message listener on <" + url + ">");

    errorConnectionFactory = new ActiveMQConnectionFactory(url);

    try {
      errorConnection = errorConnectionFactory.createConnection();
    } catch (JMSException ex) {
      throw new InitializationException("Could not create connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      errorConnection.start();
    } catch (JMSException ex) {
      throw new InitializationException("Could not start connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      setErrorSession(errorConnection.createSession(false, Session.AUTO_ACKNOWLEDGE));
    } catch (JMSException ex) {
      throw new InitializationException("Could not create session <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      // Set the destination
      switch (getErrorQueueType()) {
        case SERVICE_Q_TYPE_QUEUE:
          errorDestination = (ActiveMQQueue) getValidSession().createQueue(getErrorQueueName());
          break;
        case SERVICE_Q_TYPE_TOPIC:
          errorDestination = (ActiveMQTopic) getValidSession().createTopic(getErrorQueueName());
          break;
      }
    } catch (JMSException ex) {
      throw new InitializationException("Could not create queue destination <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      errorProducer = getErrorSession().createProducer(errorDestination);
    } catch (JMSException ex) {
      throw new InitializationException("Could not create consumer <" + ex.getMessage() + ">",getSymbolicName());
    }

    return true;
  }
  
 /**
  * The initValidQueueType gets the type of the valid output queue. This may
  * be either SERVICE_Q_TYPE_QUEUE or SERVICE_Q_TYPE_TOPIC.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initValidQueueType() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_VALID_Q_TYPE,
                                                   SERVICE_Q_TYPE_QUEUE);

    return configHelper;
  }

 /**
  * The initValidQueueName gets the name of the valid output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initValidQueueName() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_VALID_Q_NAME,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_VALID_Q_NAME+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initValidQueueName gets the name of the valid output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initValidQueueHost() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_VALID_Q_HOST,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_VALID_Q_HOST+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initValidQueueName gets the port of the valid output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initValidQueuePort() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_VALID_Q_PORT,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_VALID_Q_PORT+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }
  
 /**
  * The initValidQueueType gets the type of the error output queue. This may
  * be either SERVICE_Q_TYPE_QUEUE or SERVICE_Q_TYPE_TOPIC.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initErrorQueueType() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_ERROR_Q_TYPE,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_ERROR_Q_TYPE+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initValidQueueName gets the name of the error output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initErrorQueueName() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_ERROR_Q_NAME,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_ERROR_Q_NAME+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initValidQueueName gets the name of the error output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initErrorQueueHost() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_ERROR_Q_HOST,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_ERROR_Q_HOST+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initValidQueueName gets the port of the error output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initErrorQueuePort() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_ERROR_Q_PORT,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_ERROR_Q_PORT+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * The initSingleQueueOutput gets the configuration if we have only a single 
  * output queue.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initSingleQueueOutput() throws InitializationException
  {
    String configHelper;

    // Get the init statement from the properties
    configHelper = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   SERVICE_SINGLE_OUTPUT,
                                                   "None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Output <" + getSymbolicName() + "> - config parameter <"+SERVICE_SINGLE_OUTPUT+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

  /**
   * @return the validSession
   */
  public Session getValidSession() {
    return validSession;
  }

  /**
   * @param validSession the validSession to set
   */
  public void setValidSession(Session validSession) {
    this.validSession = validSession;
  }

  /**
   * @return the errorSession
   */
  public Session getErrorSession() {
    if (singleWriter)
    {
      return validSession;
    }
    else
    {
      return errorSession;
    }
  }

  /**
   * @param errorSession the errorSession to set
   */
  public void setErrorSession(Session errorSession) {
    this.errorSession = errorSession;
  }

  /**
   * @return the validQueuePort
   */
  public String getValidQueuePort() {
    return validQueuePort;
  }

  /**
   * @param validQueuePort the validQueuePort to set
   */
  public void setValidQueuePort(String validQueuePort) {
    this.validQueuePort = validQueuePort;
  }

  /**
   * @return the errorQueueType
   */
  public String getErrorQueueType() {
    return errorQueueType;
  }

  /**
   * @param errorQueueType the errorQueueType to set
   */
  public void setErrorQueueType(String errorQueueType) {
    this.errorQueueType = errorQueueType;
  }

  /**
   * @return the errorQueueName
   */
  public String getErrorQueueName() {
    return errorQueueName;
  }

  /**
   * @param errorQueueName the errorQueueName to set
   */
  public void setErrorQueueName(String errorQueueName) {
    this.errorQueueName = errorQueueName;
  }

  /**
   * @return the errorQueueHost
   */
  public String getErrorQueueHost() {
    return errorQueueHost;
  }

  /**
   * @param errorQueueHost the errorQueueHost to set
   */
  public void setErrorQueueHost(String errorQueueHost) {
    this.errorQueueHost = errorQueueHost;
  }

  /**
   * @return the errorQueuePort
   */
  public String getErrorQueuePort() {
    return errorQueuePort;
  }

  /**
   * @param errorQueuePort the errorQueuePort to set
   */
  public void setErrorQueuePort(String errorQueuePort) {
    this.errorQueuePort = errorQueuePort;
  }

  /**
   * @return the validQueueType
   */
  public String getValidQueueType() {
    return validQueueType;
  }

  /**
   * @param validQueueType the validQueueType to set
   */
  public void setValidQueueType(String validQueueType) {
    this.validQueueType = validQueueType;
  }

  /**
   * @return the validQueueName
   */
  public String getValidQueueName() {
    return validQueueName;
  }

  /**
   * @param validQueueName the validQueueName to set
   */
  public void setValidQueueName(String validQueueName) {
    this.validQueueName = validQueueName;
  }

  /**
   * @return the validQueueHost
   */
  public String getValidQueueHost() {
    return validQueueHost;
  }

  /**
   * @param validQueueHost the validQueueHost to set
   */
  public void setValidQueueHost(String validQueueHost) {
    this.validQueueHost = validQueueHost;
  }
}
