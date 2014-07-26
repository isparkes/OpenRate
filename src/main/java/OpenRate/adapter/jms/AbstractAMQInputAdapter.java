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

package OpenRate.adapter.jms;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.QueueMessageRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * ActiveMQ Input Adapter - reads events from an ActiveMQ Queue or Topic. This 
 * adapter runs in a polling mode (as opposed to a listener mode), which works 
 * better to create a batch of records into a transaction. All records (up to 
 * the batch size limit) are read in one go, until no more records arrive within
 * the given timeout. In that case, the batch is processed.
 * 
 * The adapter can be configured to use either a Queue or a Topic.
 */
public abstract class AbstractAMQInputAdapter
  extends AbstractTransactionalInputAdapter
  implements IEventInterface, MessageListener

{
  /**
   * The name of the queue we are scanning.
   */
  private String queueName = null;

  /**
   * The name of the host where the queue is.
   */
  private String queueHost = null;

  /**
   * The port where the queue is.
   */
  private String queuePort = null;

 /**
  * used to track the status of the stream processing. This should normally
  * count the number of input records which have been processed.
  */
  protected int InputRecordNumber = 0;

  // This is the current transaction number we are working on
  private int      transactionNumber = 0;

  // List of Services that this Client supports
  private static final String SERVICE_Q_TYPE = "QueueType";
  private static final String SERVICE_Q_NAME = "QueueName";
  private static final String SERVICE_Q_HOST = "QueueHost";
  private static final String SERVICE_Q_PORT = "QueuePort";

  // The types of queues we are using
  private static final String SERVICE_Q_TYPE_QUEUE   = "QUEUE";
  private static final String SERVICE_Q_TYPE_TOPIC   = "TOPIC";
  private static final String SERVICE_Q_TYPE_DURABLE = "DURABLETOPIC";

  // Tells the the type of queue we are using, either SERVICE_Q_TYPE_QUEUE or SERVICE_Q_TYPE_TOPIC
  private String queueType = "";
  
  // javax.jms.Session
  private Session session;

  // javax.jms.MessageConsumer
  private MessageConsumer consumer;

  // The destination, cast to either queue or Topic
  private ActiveMQDestination destination;

  // The connection factory
  private ActiveMQConnectionFactory connectionFactory;

  // and the connection
  private Connection connection;

 /**
  * This tells us if we should look for a file to open or continue reading from
  * the one we have
  */
  protected boolean InputStreamOpen = false;

 /**
  * Holds the time stamp for the transaction
  */
  protected String ORTransactionId = null;

  /**
   * Default Constructor
   */
  public AbstractAMQInputAdapter()
  {
    super();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of inherited Input Adapter functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation.
  * Initialise input adapter.
  * sets the filename to use & initialises the file reader.
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

    // Now we load the properties and use the event interface to initialise
    // the adapter. Note that this architecture will change to be completely
    // event driven in the near future.
    ConfigHelper = initGetInputQueueType();
    processControlEvent(SERVICE_Q_TYPE, true, ConfigHelper);
    ConfigHelper = initGetInputQueueName();
    processControlEvent(SERVICE_Q_NAME, true, ConfigHelper);
    ConfigHelper = initGetInputQueueHost();
    processControlEvent(SERVICE_Q_HOST, true, ConfigHelper);
    ConfigHelper = initGetInputQueuePort();
    processControlEvent(SERVICE_Q_PORT, true, ConfigHelper);

    // initialise the queue consumer
    initConsumer();
  }

 /**
  * loadBatch() is called regularly by the framework to either process records
  * or to scan for work to do, depending on whether we are already processing
  * or not.
  *
  * The way this works is that we assign a batch of files to work on, and then
  * work our way through them. This minimises the directory scans that we have
  * to do and improves performance.
  */
  @Override
  protected Collection<IRecord> loadBatch() throws ProcessingException
  {
    String     baseName = null;
    Collection<IRecord> Outbatch;
    int        ThisBatchCounter = 0;
    boolean    consumerHasRecords = true;
    Message    msg = null;

    // The Record types we will have to deal with
    HeaderRecord  tmpHeader;
    TrailerRecord tmpTrailer;
    QueueMessageRecord tmpDataRecord;
    IRecord       batchRecord;
    Outbatch = new ArrayList<>();

    // Process records if we are not yet full, or we have files waiting
    while ((ThisBatchCounter < batchSize) & (consumerHasRecords))
    {
      // if we are not in a transaction, see if we are allowed to see if one
      // should be started. If we are already in a transaction we can just continue
      if (InputStreamOpen || ((InputStreamOpen==false) && (canStartNewTransaction())))
      {
        try {
          // get records, or wait 100mS trying
          msg = consumer.receive(100);
        } catch (JMSException ex) {
          getPipeLog().error("Error getting message <" + ex.getMessage() + ">");
          throw new ProcessingException("Error getting message <" + ex.getMessage() + ">",ex,getSymbolicName());
        }
      }

      // See if we have run out of records, in this case we close the transaction
      // we are are in one
      consumerHasRecords = (msg != null);

      // see if we can open a new file - we are not in a transaction but we have
      // files waiting, so open a file
      if (InputStreamOpen == false)
      {
        if (consumerHasRecords)
        {
          // Create the new transaction to hold the information. This is done in
          // The transactional layer - we just trigger it here
          // Create the transaction base name according to a simple counter
          transactionNumber = createNewTransaction();

          // This is the transaction identifier for all records in this stream
          ORTransactionId = getTransactionID(transactionNumber);

          // reset the record number
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
          tmpHeader = (HeaderRecord)procHeader((IRecord)tmpHeader);
          Outbatch.add(tmpHeader);

          // Set that the input stream is open
          InputStreamOpen = true;

          // put the payload into the record
          tmpDataRecord = new QueueMessageRecord(msg, InputRecordNumber);

          // Call the user layer for any processing that needs to be done
          batchRecord = procValidRecord((IRecord) tmpDataRecord);

          // Add the prepared record to the batch, because of record compression
          // we may receive a null here. If we do, don't bother adding it
          if (batchRecord != null)
          {
            // We got a record to work on
            ThisBatchCounter++;
            InputRecordNumber++;
            Outbatch.add(batchRecord);
          }
        }
      }
      else
      {
        if (consumerHasRecords)
        {
          // Continue with the open batch
          tmpDataRecord = new QueueMessageRecord(msg, InputRecordNumber);

          // Call the user layer for any processing that needs to be done
          batchRecord = procValidRecord((IRecord) tmpDataRecord);

          // Add the prepared record to the batch, because of record compression
          // we may receive a null here. If we do, don't bother adding it
          if (batchRecord != null)
          {
            // We got a record to work on
            ThisBatchCounter++;
            InputRecordNumber++;
            Outbatch.add(batchRecord);
          }

          // set the scheduler
          getPipeline().setSchedulerHigh();
        }
        else
        {
          // set the scheduler
          getPipeline().setSchedulerHigh();

          // we have finished
          InputStreamOpen = false;

          // get any pending records that are in the input handler
          batchRecord = purgePendingRecord();

          // Add the prepared record to the batch, because of record compression
          // we may receive a null here. If we do, don't bother adding it
          if (batchRecord != null)
          {
            InputRecordNumber++;
            Outbatch.add(batchRecord);
          }

          // Inject a stream trailer record into the stream
          tmpTrailer = new TrailerRecord();
          tmpTrailer.setStreamName(baseName);
          tmpTrailer.setTransactionNumber(transactionNumber);

          // Pass the header to the user layer for any processing that
          // needs to be done. To allow for purging in the case of record
          // compression, we allow multiple calls to procTrailer until the
          // trailer is returned
          batchRecord = procTrailer((IRecord)tmpTrailer);

          // This allows us to purge out records from the input adapter
          // before the trailer
          while (!(batchRecord instanceof TrailerRecord))
          {
            // the call the trailer returned a purged record. Add this
            // to the batch and fetch again
            Outbatch.add(batchRecord);
            batchRecord = procTrailer((IRecord)tmpTrailer);
          }

          Outbatch.add(batchRecord);
          ThisBatchCounter++;


          // Notify the transaction layer that we have finished
          setTransactionFlushed(transactionNumber);

          // Reset the transaction number
          transactionNumber = 0;
        }
      }
    }

    return Outbatch;
  }

 /**
  * Get the transaction id for the transaction. Intended to be overwritten
  * in the case that you want another transaction ID format.
  *
  * @param transactionNumber The number of the transaction
  * @return The calculated transaction id
  */
  public String getTransactionID(int transactionNumber)
  {
    return ""+new Date().getTime();
  }

  /**
   * Closes down the input stream after all the input has been collected
   *
   * @param TransactionNumber The transaction number of the transaction to close
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream(int TransactionNumber)
    throws ProcessingException
  {
    // Nothing
  }

 /**
  * Allows any records to be purged at the end of a file
  *
  * @return The pending record
  */
  @Override
  public IRecord purgePendingRecord()
  {
    // default - do nothing
    return null;
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

  /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction;
  *
  * @param transactionNumber The transaction to flush
  * @return 0 if the transaction was closed OK, otherwise -1
  */
  @Override
  public int flushTransaction(int transactionNumber)
  {
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
    // nothing
  }

 /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction;
  * @param transactionNumber The transaction to rollback
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    // nothing
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
    // Nothing
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
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

    if (Command.equalsIgnoreCase(SERVICE_Q_TYPE))
    {
      if (Init)
      {
        if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_QUEUE))
        {
          queueType = SERVICE_Q_TYPE_QUEUE;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_TOPIC))
        {
          queueType = SERVICE_Q_TYPE_TOPIC;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase(SERVICE_Q_TYPE_DURABLE))
        {
          queueType = SERVICE_Q_TYPE_DURABLE;
          ResultCode = 0;
        }
      }
      else
      {
        if (Parameter.equals(""))
        {
          return queueType;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_Q_NAME))
    {
      if (Init)
      {
        setQueueName(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getQueueName();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equals(SERVICE_Q_HOST))
    {
      if (Init)
      {
        setQueueHost(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getQueueHost();
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_Q_PORT))
    {
      if (Init)
      {
        setQueuePort(Parameter);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return getQueuePort();
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_Q_TYPE, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_Q_NAME, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_Q_HOST, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_Q_PORT, ClientManager.PARAM_NONE);
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------

  /**
  * The initGetQueueType gets the type of the error input queue. This may
  * be either SERVICE_Q_TYPE_QUEUE, SERVICE_Q_TYPE_TOPIC or SERVICE_Q_TYPE_DURABLETOPIC.
  */
  private String initGetInputQueueType() throws InitializationException
  {
    String tmpType;
    tmpType = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(),getSymbolicName(),SERVICE_Q_TYPE, SERVICE_Q_TYPE_QUEUE);

    if ((tmpType.equals(SERVICE_Q_TYPE_QUEUE) || tmpType.equals(SERVICE_Q_TYPE_TOPIC) || tmpType.equals(SERVICE_Q_TYPE_DURABLE)) == false)
    {
      message = "Parameter <QueueType> must be one of " + SERVICE_Q_TYPE_QUEUE + ", " + SERVICE_Q_TYPE_TOPIC + " or " + SERVICE_Q_TYPE_DURABLE + " but received <"+ tmpType +">";
      throw new InitializationException(message,getSymbolicName());
    }
    
    return tmpType;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputQueueName() throws InitializationException
  {
    String configHelper;
    configHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(),getSymbolicName(),SERVICE_Q_NAME,"None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Input <" + getSymbolicName() + "> - config parameter <"+SERVICE_Q_NAME+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputQueueHost()
                              throws InitializationException
  {
    String configHelper;
    configHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(),getSymbolicName(),SERVICE_Q_HOST,"None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Input <" + getSymbolicName() + "> - config parameter <"+SERVICE_Q_HOST+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputQueuePort()
                             throws InitializationException
  {
    String configHelper;
    configHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(),getSymbolicName(),SERVICE_Q_PORT,"None");

    if ((configHelper == null) || configHelper.equalsIgnoreCase("None"))
    {
      String Message = "Input <" + getSymbolicName() + "> - config parameter <"+SERVICE_Q_PORT+"> not found";
      getPipeLog().error(Message);
      throw new InitializationException(Message,getSymbolicName());
    }

    return configHelper;
  }

 /**
   * Tries to connect to the queue, and create a consumer of it.
   *
   * @return true if the queue was initialised correctly, otherwise false
   */
  private boolean initConsumer() throws InitializationException
  {
    String url = "tcp://"+getQueueHost()+":"+getQueuePort();
    getPipeLog().info("start message listener on <" + url + "> with  queue type to <"+queueType+">");
    
    connectionFactory = new ActiveMQConnectionFactory(url);

    try {
      connection = connectionFactory.createConnection();
      connection.setClientID(getPipeName()+"."+getSymbolicName());
    } catch (JMSException ex) {
      throw new InitializationException("Could not create connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      connection.start();
    } catch (JMSException ex) {
      throw new InitializationException("Could not start connection <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    } catch (JMSException ex) {
      throw new InitializationException("Could not create session <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      // Set the destination
      switch (queueType) {
        case SERVICE_Q_TYPE_QUEUE:
          destination = (ActiveMQQueue) session.createQueue(getQueueName());
          break;
        case SERVICE_Q_TYPE_TOPIC:
        case SERVICE_Q_TYPE_DURABLE:
          destination = (ActiveMQTopic) session.createTopic(getQueueName());
          break;
        default:
          throw new InitializationException("<QueueType> is not <QUEUE> or <TOPIC>",getSymbolicName());
      }
      
    } catch (JMSException ex) {
      throw new InitializationException("Could not create queue destination <" + ex.getMessage() + ">",getSymbolicName());
    }
    
    try {
      // Set the consumer
      switch (queueType) {
        case SERVICE_Q_TYPE_QUEUE:
        case SERVICE_Q_TYPE_TOPIC:
          consumer = session.createConsumer(destination);
          break;
        case SERVICE_Q_TYPE_DURABLE:
           consumer = session.createDurableSubscriber((ActiveMQTopic)destination, getPipeName()+"."+getSymbolicName());
          break;
      }
    } catch (JMSException ex) {
      throw new InitializationException("Could not create consumer <" + ex.getMessage() + ">",getSymbolicName());
    }

    return true;
  }

  // -----------------------------------------------------------------------------
  // ---------------------- Start stream handling functions ----------------------
  // -----------------------------------------------------------------------------

  /**
   * Triggered if a message is received and we were in listener mode. Not used
   * for polling mode.
   * 
   * @param msg The message that was received
   */
  @Override
  public void onMessage(Message msg) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * @return the queueName
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * @param queueName the queueName to set
   */
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * @return the queueHost
   */
  public String getQueueHost() {
    return queueHost;
  }

  /**
   * @param queueHost the queueHost to set
   */
  public void setQueueHost(String queueHost) {
    this.queueHost = queueHost;
  }

  /**
   * @return the queuePort
   */
  public String getQueuePort() {
    return queuePort;
  }

  /**
   * @param queuePort the queuePort to set
   */
  public void setQueuePort(String queuePort) {
    this.queuePort = queuePort;
  }
}
