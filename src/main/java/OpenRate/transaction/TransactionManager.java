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

package OpenRate.transaction;

import OpenRate.IPipeline;
import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the pipeline wide transaction manager, used for coordinating the processing
 * across multiple modules. The idea is that each module registers itself as a
 * client to the transaction manager, and using the client number and the
 * transaction number, it tells the transaction manager about the status of
 * the transaction from its point of view, and the records it has processed.
 *
 * The transaction manager module then calculates an overall transaction status
 * and this is used to control the pipeline. All client modules are obliged to
 * read the overall transaction status at opportune moments to understand how
 * the transaction processing should progress.
 *
 * This version of the Transaction Manager is for the asynchronous pipeline,
 * using buffers between the modules and therefore has a simplified handshaking
 * of the transaction:
 *
 * NONE         - Transaction opened but not initialised
 * PROCESSING   - transaction processing input stream
 * FLUSHED      - All input done
 * FINISHED_OK  - Transaction committed
 * FINISHED_ERR - Transaction rolled back or cancelled
 * COMPLETE     - All finalisation processing completed
 *
 * Notification to the client modules is triggered on FLUSHED status so that
 * the input and output modules can complete the persistent storage calculation
 * and in this case, the output module gets to work first, then the input module
 * as there may be the need to cancel due to output problems more probably than
 * input problems.
 *
 * Notification also happens in the case of an ABORT request.
 *
 * For the transaction from FLUSHED and FINISHED, the check of the status is
 * initiated by the pipe controller, which avoids re-entry into the TM at very
 * little overhead cost.
 *
 * ToDo:
 *   Make the modules wait for notifications of transaction finishing?
 */
public class TransactionManager implements ITransactionManager, IEventInterface
{
  // This is the pipeline that we are in, used for logging and property retrieval
  private IPipeline pipeline;

  // Tells us whether we are in a transaction
  private boolean  TMStarted               = false;

  private ConcurrentHashMap<Integer, TransactionInfo> transactionList;  // This is the map of the transactions in progress
  private static int   nextTransactionNumber   = 0;           // The transaction number sequence generator

  private int          numberOfClients         = 0;           // The number of clients we are dealing with
  private ITMClient[]  clients;                               // This is the map of the client objects
  private int[]        clientTypeArray;                       // This is the map of the client objects
  private int[]        clientTransNumber;                     // The transaction number the client is working on

  // This is the count of the transactions we have open right now
  private int     activeTransactionCount = 0;

  // Wheter we are allowed to create new transactions or not
  private boolean newTransactionAllowed  = true;

  // This holds the maximum number of transactions that this TM can have open
  // at any one time. Defaults to 1, but can be set to any other value.
  private int maxTransactions = 1;

  // This defines if we should abort all transactions that are open if one aborts
  private boolean abortConcurrentTransactions = false;

  // List of Services that this Client supports
  private final static String SERVICE_ABORT = "SetAbort";
  private final static String SERVICE_ABORT_CONCURRENT_TRANS = "AbortConcurrentTransactions";
  private final static String SERVICE_ALLOWTRANS = "AllowTransaction";
  private final static String SERVICE_TRANSCOUNT = "TransactionCount";
  private final static String SERVICE_CLIENT_STATUS = "ClientStatus";
  private final static String SERVICE_FLUSH_STATUS = "FlushStatus";
  private final static String SERVICE_MAX_TRANSACTIONS = "MaxTransactions";
  private final static String SERVICE_ABORT_HARD = "AbortHard";

  // module symbolic name: set during initialisation
  private String symbolicName = "TransactionManager";

  // Common Definitions for the transaction manager
  private TMDefs TMD = TMDefs.getTMDefs();

  // used to simplify logging and exception handling
  public String message;
  
  // Transaction Flusher Thread
  TransactionFlusher tmf;

  /**
   * Constructor
   */
  public TransactionManager()
  {
  }

 /**
  * Initialise the transaction manager
  *
  * @param pipelineName The name of the pipeline that this TM is working for
  * @throws OpenRate.exception.InitializationException
  */
  public void init(String pipelineName) throws InitializationException
  {
    OpenRate.getOpenRateFrameworkLog().info("Starting TransactionManager initialisation for pipeline <" +
               pipelineName + ">");

    // store the pipe we are in - used for pipeline level logging and exception handling
    setPipeline(OpenRate.getPipelineFromMap(pipelineName));

    // The list of current transactions
    transactionList   = new ConcurrentHashMap<>(50);

    // The list of clients
    clients           = new ITMClient[50];

    // Tells us what type of client a client is (Input, processing, output)
    clientTypeArray   = new int[50];

    // Tells us the transaction a client is in (it can't always find out by itself when we are overlaying transactions)
    clientTransNumber = new int[50];

    // Set the initial status of the transaction manager
    requestTMStart();

    // Start the TM Flusher thread
    tmf = new TransactionFlusher();
    ThreadGroup flusher = new ThreadGroup("TransactionFlusher");

    Thread flusherThread = new Thread(flusher, tmf, "TransFlusher."+pipelineName+"-Inst-0");
    tmf.setTMReference(this);
    tmf.setPipelineName(pipelineName);
    tmf.setLogger(getPipeLog());
    flusherThread.setDaemon(true);
    flusherThread.start();

    System.out.println("    Started 1 Transaction Flusher Thread for pipeline <"+pipelineName+">");

    OpenRate.getOpenRateFrameworkLog().info("TransactionManager initialised");
  }

  /**
   * Creates a new transaction instance, ready accept transaction information
   * Sets the transaction start time
   *
   * @param pipeline The name of the pipeline that opened the transaction
   * @return The transaction number
   */
  @Override
  public synchronized int openTransaction(String pipeline)
  {
    int tmpTransactionNumber;
    if (TMStarted)
    {
      tmpTransactionNumber            = getNextTransactionNumber();
      TransactionInfo CachedTrans     = new TransactionInfo();
      CachedTrans.setTransactionStart(System.currentTimeMillis());
      CachedTrans.setTransactionNumber(tmpTransactionNumber);
      transactionList.put(tmpTransactionNumber, CachedTrans);
      message = "Opened transaction <" + tmpTransactionNumber + "> for pipeline <" + pipeline + ">";
      getPipeLog().info(message);
      OpenRate.getOpenRateStatsLog().info(message);

      // Maintain the count
      activeTransactionCount++;

      return tmpTransactionNumber;
    }
    else
    {
      getPipeLog().error("Transaction Manager halted. Cannot open new transactions.");

      return -1;
    }
  }

 /**
  * Commit the transaction and print the transaction information to the log.
  *
  * @param transNumber The transaction number to close
  */
  @Override
  public synchronized void closeTransaction(int transNumber)
  {
    TransactionInfo CachedTrans;

    CachedTrans = transactionList.get(transNumber);

    CachedTrans.setTransactionEnd(System.currentTimeMillis());
    long TransactionTime = (CachedTrans.getTransactionEnd() - CachedTrans.getTransactionStart());

    // Deal with the case that we have a zero transaction time
    if (TransactionTime == 0)
    {
      TransactionTime = 1;
    }

    getPipeLog().info("Transaction <" + transNumber + "> closed");
    OpenRate.getOpenRateStatsLog().info("Transaction <" + transNumber + "> closed");
    OpenRate.getOpenRateStatsLog().info("Statistics: Records  <" + CachedTrans.getTransactionRecords() + ">");
    OpenRate.getOpenRateStatsLog().info("            Duration <" +
      (CachedTrans.getTransactionEnd() - CachedTrans.getTransactionStart()) + "> ms");
    OpenRate.getOpenRateStatsLog().info("            Speed    <" +
      ((CachedTrans.getTransactionRecords() * 1000) / TransactionTime) + "> records /sec");

    // remove the old transaction
    transactionList.remove(transNumber);
    getPipeLog().debug("Removed transaction <" + transNumber + ">");

    // Maintain the count
    activeTransactionCount--;
 }

 /**
  * Signal that the transaction number should be aborted at the first possible
  * opportunity
  *
  * @param transNumber The transaction number to abort
  */
  @Override
  public void requestTransactionAbort(int transNumber)
  {
    TransactionInfo tmpCachedTrans;
    TransactionInfo CachedTrans;

    CachedTrans = transactionList.get(transNumber);

    CachedTrans.setAbortRequested(true);
    getPipeLog().info("Request Abort for Transaction <" + transNumber + ">");

    // if we should abort concurrent transactions do so
    if (abortConcurrentTransactions)
    {
      Iterator<Integer> TransactionIterator = transactionList.keySet().iterator();

      while (TransactionIterator.hasNext())
      {
        Integer tmpTransactionKey = TransactionIterator.next();
        tmpCachedTrans = transactionList.get(tmpTransactionKey);
        tmpCachedTrans.setAbortRequested(true);
        getPipeLog().info("Request Subordinate Abort for Transaction <" + tmpTransactionKey + ">");
      }
    }
  }

 /**
  * Find if the transaction has been aborted
  *
  * @param transNumber The transaction number to abort
  * @return true if the transaction has been aborted, otherwise false
  */
  public boolean getTransactionAborted(int transNumber)
  {
    if (transactionList.containsKey(transNumber))
    {
      return transactionList.get(transNumber).isAbortRequested();
    }
    else
    {
      // This is a closed transaction, we didn't abort it
      OpenRate.getOpenRateFrameworkLog().warning("No trans info found for trans<" + transNumber + ">");
      return false;
    }
  }

 /**
  * Stop the Transaction Manager, so that new transactions cannot be started
  */
  public void requestTMStop()
  {
    if (TMStarted)
    {
      getPipeLog().info("Request Stop for Transaction manager. Will stop after current transaction is closed.");
      TMStarted = false;
    }
    else
    {
      getPipeLog().error("Transaction manager already stopped. No Change made.");
    }
  }

 /**
  * Start the Transaction Manager, so that new transactions can be started
  */
  public void requestTMStart()
  {
    if (TMStarted)
    {
      getPipeLog().error("Transaction manager already started. No Change made.");
    }
    else
    {
      getPipeLog().info("Request Start for Transaction manager.");
      TMStarted = true;
    }
  }

 /**
  * Get the status of the Transaction Manager
  *
  * @return If the TM is started
  */
  public boolean getTMStarted()
  {
    return TMStarted;
  }

 /**
  * Return the number of registered Transaction Manager clients
  *
  * @return The number of clients attached to this TM
  */
  public int getNumberOfClients()
  {
    return numberOfClients;
  }

 /**
  * Cancel (delete, remove) a given transaction.
  *
  * @param transNumber The transaction number to cancel
  */
  @Override
  public void cancelTransaction(int transNumber)
  {
    getPipeLog().info("Cancel Transaction <" + transNumber + ">");
    transactionList.remove(transNumber);
    activeTransactionCount--;
  }

  /**
   * Register a module as a client of the transaction manager. This is required
   * so that we can track when all clients have finished their work, meaning
   * that the entire transaction was finished. There are three types of clients:
   * Input module
   * Processing module
   * Output module
   *
   * The transaction can be closed when the last module in the pipe returns a status
   * of TM_FINISHED.
   *
   * @param clientType The type of the client (input, output, processing)
   * @param clientReference The object reference
   * @return Client index
   */
  public int registerClient(int clientType, ITMClient clientReference)
  {
    // Need to add something here to track the status of the transaction, and
    // perhaps the clients
    numberOfClients++;
    clients[numberOfClients] = clientReference;
    clientTypeArray[numberOfClients] = clientType;

    getPipeLog().debug("Registered client <" + clientReference.toString() +"> as client type <" + clientType + "> as client number <" + numberOfClients + ">");

    return numberOfClients;
  }

  /**
   * Update the transaction status from the current client. This will have the
   * effect of recalculating the new overall status and will return it immediately
   *
   * The transaction can be closed when the last module in the pipe returns a status
   * of TM_FINISHED.
   *
   * @param transNumber The transaction we are working on
   * @param clientNumber The client number
   * @param newStatus The new client status to set
   */
  public synchronized void setClientStatus(int transNumber, int clientNumber, int newStatus)
  {
    try
    {
      // Update the client status
      transactionList.get(transNumber).setClientStatus(clientNumber, newStatus);

      // Print something to the log, so we can understand the state changes
      getPipeLog().debug("Client <" + clientNumber+ "> set status <" + newStatus + "> " +
                " for transaction <" + transNumber + ">");

      // Store away the transaction number
      clientTransNumber[clientNumber] = transNumber;

      // Set the transaction into the "finish transaction list" (if it is ready)
      if ((clientNumber==numberOfClients) && (newStatus==TMD.TM_FLUSHED))
      {
        tmf.addTransactionToFlushList(transactionList.get(transNumber));
      }
    }
    catch (NullPointerException npe)
    {
      message = "Error setting client status <" + newStatus +
                       "> for client <" + clientNumber + "> in transaction <" +
                       transNumber + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
    }
  }


  int getClientCount()
  {
    return numberOfClients;
  }

 /**
  * The the client reference.
  *
  * @param i The index of the client to get
  * @return The client reference
  */
  public ITMClient getClient(int i)
  {
    return clients[i];
  }

 /**
  * Return the transaction a client is working on. This is required because the
  * management of overlaid transactions means that clients are not able to track
  * this correctly.
  *
  * @param clientNumber The client who is requesting the information
  * @return The current transaction number
  */
  public int getTransactionNumber(int clientNumber)
  {
    return clientTransNumber[clientNumber];
  }

  /**
   * Get the next transaction number. This is a synchronised method to ensure
   * that we never return the same transaction number twice, nor leave a gap.
   *
   * @return the nextTransactionNumber
   */
  public synchronized static int getNextTransactionNumber() {
    nextTransactionNumber++;
    return nextTransactionNumber;
  }

 /**
  * Calculates the new overall status for this transaction
  *
  * @param transNumber The transaction we are working on
  * @param cachedTrans The information object for the transaction
  * @return the new overall status
  */
  public synchronized int getOverallStatus(int transNumber, TransactionInfo cachedTrans)
  {
    int     newOverallStatus;
    boolean AllFinished      = true;
    boolean AllClosed        = true;
    boolean ErrFlag          = false;
    int     i;

    // Calculate the new overall status for this transaction. This will be:
    // The maximum value of statuses up to TM_PROCESSING
    // TM_PROCESSING until the last client goes to TM_FLUSHED
    // TM_ABORT_REQUEST if any client requests an abort
    // we should report the lowest overall status, so we initialise high
    // and go down
    newOverallStatus                         = TMD.TM_CLOSED;
    for (i = 1; i <= numberOfClients; i++)
    {
      int tmpStatus = cachedTrans.getClientStatus(i);
      AllFinished   = (AllFinished &
                     ((tmpStatus == TMD.TM_FINISHED_OK) ||
                      (tmpStatus == TMD.TM_FINISHED_ERR)));
      AllClosed     = (AllClosed & (tmpStatus == TMD.TM_CLOSING));
      ErrFlag       = (ErrFlag | (cachedTrans.getClientStatus(i) == TMD.TM_FINISHED_ERR));

      if (tmpStatus < newOverallStatus)
      {
        newOverallStatus = tmpStatus;
      }
    }

    // We have to be a little bit clever with the status FINISHED, because
    // we have to calculate either TM_FINISHED_OK if *ALL* the clients report
    // OK, or else TM_FINISHED_ERR if any one of the clients had an error
    if ((ErrFlag) | (cachedTrans.isAbortRequested()))
    {
      cachedTrans.setTransactionErrored(true);
    }

    if ((newOverallStatus == TMD.TM_FINISHED_OK) | (newOverallStatus == TMD.TM_FINISHED_ERR))
    {
      if (cachedTrans.isTransactionErrored())
      {
        newOverallStatus = TMD.TM_FINISHED_ERR;
      }
      else
      {
        newOverallStatus = TMD.TM_FINISHED_OK;
      }
    }

    // Mark if we had a state change
    cachedTrans.setStateChange(cachedTrans.getTransactionStatus() != newOverallStatus);

    // Store it in the transaction
    cachedTrans.setTransactionStatus(newOverallStatus);

    // log if we need to
    if (getPipeLog().isDebugEnabled())
    {
      logTransactionStatus(transNumber);
    }

    // return it
    return newOverallStatus;
  }

 /**
  * Put an overview of the transaction status into the log. This is a costly
  * operation and should be used with care for performance reasons.
  *
  * @param transNumber the transaction number
  */
  private void logTransactionStatus(int transNumber)
  {
    int i;
    String  ClientType       = "";
    String  ClientStatus     = "";

    if (transactionList.containsKey(transNumber))
    {
      TransactionInfo CachedTrans = transactionList.get(transNumber);

      for (i = 1; i <= numberOfClients; i++)
      {
        ClientType += Integer.toString(clientTypeArray[i]);
        ClientStatus += Integer.toString(CachedTrans.getClientStatus(i));
      }

      getPipeLog().debug("New Status for transaction <" + transNumber + ">:");
      ClientStatus = ClientStatus + " --> " + CachedTrans.getTransactionStatus();

      if (CachedTrans.isStateChange())
      {
        ClientStatus += "(*)";
      }

      getPipeLog().debug("  " + ClientType);
      getPipeLog().debug("  " + ClientStatus);
    }
  }

 /**
  * Get the current status of a transaction client
  *
  * @param transNumber The transaction number
  * @param clientNumber The client number
  * @return The current status of the client
  */
  public int getClientStatus(int transNumber, int clientNumber)
  {
    return transactionList.get(transNumber).getClientStatus(clientNumber);
  }

  /**
   * Get the overall transaction status. This is the processed summation of the
   * statuses of the individual client processes, processed to give the current
   * processing status of the transaction
   *
   * @param transNumber The number of the transaction
   * @return The current status
   */
  @Override
  public int getTransactionStatus(int transNumber)
  {
    return transactionList.get(transNumber).getTransactionStatus();
  }

 /**
  * Update the number of records that have been processed
  *
  * @param transNumber The transaction
  * @param clientNumber The client
  * @param recordCount The new record count
  */
  public synchronized void updateClientRecordCount(int transNumber, int clientNumber,
    int recordCount)
  {
    TransactionInfo CachedTrans;

    CachedTrans = transactionList.get(transNumber);

    transactionList.get(transNumber).setRecordCount(clientNumber, recordCount);

    // update the overall transaction record count
    if (clientTypeArray[clientNumber] == TMD.CT_CLIENT_INPUT)
    {
      CachedTrans.setTransactionRecords(recordCount);
    }
  }

  /**
   * Return the count of the active transactions. This is used primarily in
   * pipeline scheduling.
   *
   * @return The number of active transactions
   */
  @Override
  public int getActiveTransactionCount()
  {
    return activeTransactionCount;
  }
  
  /**
   * Return the count of the transactions waiting to be flushed.
   *
   * @return The number of transactions waiting to be flushed
   */
  @Override
  public int getFlushedTransactionCount()
  {
    return tmf.getFlushedTransactionCount();
  }

  /**
   * Return whether the input adapter for this pipe is allowed to start new
   * transactions, based on the fact that the transaction manager is enabled,
   * and that the maximum number of concurrent transactions has not been
   * reached.
   *
   * @return True if new transactions can be started
   */
  public boolean getNewTransactionAllowed()
  {
      return newTransactionAllowed & (activeTransactionCount < maxTransactions);
  }

  /**
   * Return whether the input adapter for this pipe is allowed to start new
   * transactions
   *
   * @param newState True if new transactions are to be allowed, otherwise false
   */
  public void setNewTransactionAllowed(boolean newState)
  {
      newTransactionAllowed = newState;
  }

  /**
   * Get the maximum number of transactions allowed
   *
   * @return the max transactions value
   */
  public int getMaxTransactions()
  {
    return maxTransactions;
  }

  /**
   * Set the maximum number of transactions allowed
   *
   * @param newMaxValue the new max transactions value
   */
  public void setMaxTransactions(int newMaxValue)
  {
    maxTransactions = newMaxValue;
  }

  /**
  * Perform any close down activities that are needed, the inverse of the
  * init() procedure
  */
  public void close()
  {
    // clean up the clients
    resetClients();
  }


// -----------------------------------------------------------------------------
// ------------- Start of inherited IEventInterface functions ------------------
// -----------------------------------------------------------------------------

  /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
   *
   */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.getClientManager().registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ABORT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ALLOWTRANS, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_TRANSCOUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CLIENT_STATUS, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_FLUSH_STATUS, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_MAX_TRANSACTIONS, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ABORT_CONCURRENT_TRANS, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ABORT_HARD, ClientManager.PARAM_DYNAMIC);
  }

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {
    int ResultCode = -1;
    int i;
    String result = "";

    if (Command.equalsIgnoreCase(SERVICE_ABORT))
    {
        if (Parameter.equalsIgnoreCase("true"))
        {
          // abort all open transactions
          Iterator<Integer> transIter = transactionList.keySet().iterator();
          while (transIter.hasNext())
          {
            requestTransactionAbort(transIter.next());
          }
          ResultCode=0;
        }
        if (Parameter.equalsIgnoreCase("false"))
        {
          ResultCode=0;
        }
        if (Parameter.equalsIgnoreCase(""))
        {
          return "false";
        }
    }

    if (Command.equalsIgnoreCase(SERVICE_ABORT_CONCURRENT_TRANS))
    {
        if (Parameter.equalsIgnoreCase("true"))
        {
          abortConcurrentTransactions = true;
          ResultCode=0;
        }
        if (Parameter.equalsIgnoreCase("false"))
        {
          abortConcurrentTransactions = false;
          ResultCode=0;
        }
        if (Parameter.equalsIgnoreCase(""))
        {
          if (abortConcurrentTransactions)
          {
            return "true";
          }
          else
          {
            return "false";
          }
        }
    }

    if (Command.equalsIgnoreCase(SERVICE_ALLOWTRANS))
    {
      // put the transaction manager on hold
      if (Parameter.equalsIgnoreCase("true"))
      {
        setNewTransactionAllowed(true);
        ResultCode=0;
      }

      // release it
      if (Parameter.equalsIgnoreCase("false"))
      {
        setNewTransactionAllowed(false);
        ResultCode=0;
      }

      // Query current status
      if (Parameter.equalsIgnoreCase(""))
      {
        if (newTransactionAllowed)
        {
          return "true";
        }
        else
        {
          return "false";
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_TRANSCOUNT))
    {
      return Integer.toString(activeTransactionCount);
    }

    if (Command.equalsIgnoreCase(SERVICE_CLIENT_STATUS))
    {
      int tmpTransNumber;

      try
      {
        tmpTransNumber = Integer.parseInt(Parameter);
      }
      catch (NumberFormatException nfe)
      {
        result = "Parameter <" + Parameter + "> is not numeric";
        return result;
      }

      // Show the client status list
      TransactionInfo CachedTrans = transactionList.get(tmpTransNumber);

      if (CachedTrans == null)
      {
        result = "No transaction";
        return result;
      }

      // Get the status for each client
      for (i = 1; i <= getClientCount(); i++)
      {
        result = result + "Client <"+i+"> status for transaction <" + tmpTransNumber + "> is <" +
            CachedTrans.getClientStatus(i) + ">\n";
      }
      result = result + "New Status for transaction <" + tmpTransNumber + "> is <" +
          CachedTrans.getTransactionStatus() + ">";

      return result;
    }

    if (Command.equalsIgnoreCase(SERVICE_FLUSH_STATUS))
    {
      result = "Flusher has <" + tmf.getFlushedTransactionCount() +
               "> transactions waiting";

      return result;
    }

    // Set the maximum number of transactions
    if (Command.equalsIgnoreCase(SERVICE_MAX_TRANSACTIONS))
    {
      if (Parameter.equals(""))
      {
        return String.valueOf(getMaxTransactions());
      }
      else
      {
        try
        {
          setMaxTransactions(Integer.parseInt(Parameter));
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for batch size. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    // Set the maximum number of transactions
    if (Command.equalsIgnoreCase(SERVICE_ABORT_HARD))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        int transactionsChanged = 0;

        Iterator<Integer> transIter = transactionList.keySet().iterator();

        while (transIter.hasNext())
        {
          Integer transNumber = transIter.next();

          // Get the transaction
          TransactionInfo CachedTrans = transactionList.get(transNumber);

          // Abort it
          CachedTrans.setAbortRequested(true);
          transactionsChanged++;

          // and unblock the clients
          for (i = 1; i <= getClientCount(); i++)
          {
            CachedTrans.setClientStatus(i,TMDefs.getTMDefs().TM_FLUSHED);
          }
        }

        return "Aborted <" + transactionsChanged + "> transactions";
      }
    }

    // Return the response
    if (ResultCode == 0)
    {
        String logStr = "Command " + Command + " handled";
        getPipeLog().debug(logStr);
        return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

 /**
  * return the symbolic name
  *
  * @return The symbolic name for this plugin
  */
  public String getSymbolicName()
  {
      return symbolicName;
  }

 /**
  * set the symbolic name
  *
  * @param Name The new symbolic name for this plug in
  */
  public void setSymbolicName(String Name)
  {
      symbolicName=Name;
  }

 /**
  * Test support - not normally needed in real life: Resets the clients between
  * tests: Allows us to use a generic test set for different modules. If we
  * don't do this, we collect clients, but during tests we don't maintain them
  * correctly.
  */
  @Override
  public void resetClients() {
    numberOfClients = 0;
  }
  
  // -----------------------------------------------------------------------------
  // -------------------- Standard getter/setter functions -----------------------
  // -----------------------------------------------------------------------------

    /**
     * @return the pipeName
     */
    public String getPipeName() {
      return pipeline.getSymbolicName();
    }

    /**
     * @return the pipeline
     */
    public IPipeline getPipeline() {
      return pipeline;
    }

 /**
  * Set the pipeline reference so the input adapter can control the scheduler
  *
  * @param pipeline the Pipeline to set
  */
  public void setPipeline(IPipeline pipeline)
  {
    this.pipeline = pipeline;
  }

   /**
    * Return the pipeline logger.
    * 
    * @return The logger
    */
    protected ILogger getPipeLog() {
      return pipeline.getPipeLog();
    }

   /**
    * Return the exception handler.
    * 
    * @return The exception handler
    */
    protected ExceptionHandler getExceptionHandler() {
      return pipeline.getPipelineExceptionHandler();
    }
}
