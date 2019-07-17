package OpenRate.adapter;

import OpenRate.exception.InitializationException;
import OpenRate.transaction.ITMClient;
import OpenRate.transaction.TMDefs;
import OpenRate.transaction.TransactionManager;
import OpenRate.transaction.TransactionManagerFactory;


/**
 * This transactional layer for the Input adapter works slightly differently
 * to the other transactional modules in the system, because the input adapter
 * has the special role of being the transaction driver. The output and
 * plug in transactional layers are slaves of the state changes created in the
 * Input adapter.
 */
public abstract class AbstractTransactionalInputAdapter
  extends AbstractInputAdapter
  implements ITMClient
{
  // Get the Transaction Manager
  private TransactionManager TM;

  // used to update the status of this client instance
  private int TMClientNumber = 0;

  /** Creates a new instance of AbstractTransactionalInputAdapter */
  public AbstractTransactionalInputAdapter()
  {
    super();
  }

 /**
  * Initialize the module. Called during pipeline creation.
  * Initialize the transactional layer - get the TM client number
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    super.init(PipelineName, ModuleName);

    // now that we are up and running, register with Transaction Manager
    TM = TransactionManagerFactory.getTransactionManager(PipelineName);
    TMClientNumber = TM.registerClient(TMDefs.getTMDefs().CT_CLIENT_INPUT, this);
  }

 /**
  * See if there are any transactions in progress at the moment. We do this by
  * interrogating the transaction manager to see how many transactions are open
  * right now. If any are open, we return that we have work in progress.
  *
  * @return true if we are working on a transaction, otherwise false
  */
  protected boolean getTransactionInWork()
  {
    return (TM.getActiveTransactionCount() > 0);
  }

 /**
  * See if there are any transactions in progress at the moment. We do this by
  * interrogating the transaction manager to see how many transactions are open
  * right now. If any are open, we return that we have work in progress.
  *
  * @return true if we can start a transaction, otherwise false
  */
  protected boolean canStartNewTransaction()
  {
    return TM.getNewTransactionAllowed();
  }

 /**
  * See if the transactions we are working on has been aborted. .
  *
  * @param transactionNumber the transaction to check
  * @return true if the transaction has been aborted, otherwise false
  */
  protected boolean transactionAbortRequest(int transactionNumber)
  {
    return TM.getTransactionAborted(transactionNumber);
  }

 /**
  * Set the transaction into the abort state
  *
  * @param transactionNumber The transaction to set
  */
  protected void setTransactionAbort(int transactionNumber)
  {
    TM.requestTransactionAbort(transactionNumber);
    getPipeLog().warning("Pipe <" + getPipeName() + "> requested abort for transaction <" + transactionNumber + ">");
  }

 /**
  * createNewTransaction communicates with the transaction manager to open a
  * new internal transaction object
  *
  * @return The assigned transaction number
  */
  protected int createNewTransaction()
  {
    // create the new transaction
    int currentTransactionNumber = TM.openTransaction(getPipeName());

    // Update the status of the transaction in the TM
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_NONE);

    return currentTransactionNumber;
  }

 /**
  * Set the status of the current transaction to "Processing".
  *
  * @param transactionNumber The transaction to start
  */
  protected void setTransactionProcessing(int transactionNumber)
  {
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);
  }

 /**
  * Set the status of the current transaction to "Flushed".
  *
  * @param transactionNumber The transaction to flush
  */
  protected void setTransactionFlushed(int transactionNumber)
  {
    // Set the fact that we have flushed
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);
  }

 /**
  * Update the transaction record count for the statistics
  *
  * @param transactionNumber The transaction to update
  * @param newCount The new count that should be communicated to the trans
  * manager
  */
  protected void updateRecordCount(int transactionNumber, int newCount)
  {
    TM.updateClientRecordCount(transactionNumber, TMClientNumber, newCount);
  }

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the flush phase
  *
  * @param transactionNumber The number of the transaction to update
  * @return true if the flush was ok, otherwise false if there was an error
  */
  @Override
  public synchronized boolean updateTransactionStatusFlush(int transactionNumber)
  {
    flushTransaction(transactionNumber);

    return true;
  }

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the commit/rollback phase
  * and the transaction was processed correctly
  *
  * @param transactionNumber The number of the transaction to update
  */
  @Override
  public synchronized void updateTransactionStatusCommit(int transactionNumber)
  {
    // Call the finalisation of the processing. Commit transaction is not
    // allowed to fail, so all sensitive work should be done in the flush.
    commitTransaction(transactionNumber);
  }

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the close phase
  * and the transaction was processed with an error
  *
  * @param transactionNumber The number of the transaction to update
  */
  @Override
  public synchronized void updateTransactionStatusRollback(int transactionNumber)
  {
      // Call the finalisation of the processing. Rollback transaction is not
      // allowed to fail, so all sensitive work should be done in the flush.
      rollbackTransaction(transactionNumber);
  }

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the close phase
  *
  * @param transactionNumber The number of the transaction to update
  */
  @Override
  public synchronized void updateTransactionStatusClose(int transactionNumber)
  {
    // Call the cleanup for the processing. This is post transaction, and is
    // used for clean up etc.
    closeTransaction(transactionNumber);
  }

 /**
  * createNewTransaction communicates with the transaction manager to open a
  * new internal transaction object
  *
   * @param transactionNumber The transaction to cancel
  */
  protected void cancelTransaction(int transactionNumber)
  {
    // Update the status of the transaction in the TM
    TM.cancelTransaction(transactionNumber);
  }

 /**
  * This method returns the transaction number that we are currently working on.
  * If not transaction is in processing, returns 0.
  *
  * @return The current transaction number
  */
  public int getTransactionNumber()
  {
    return TM.getTransactionNumber(TMClientNumber);
  }

// -----------------------------------------------------------------------------
// --------------- Start of transactional layer functions ----------------------
// -----------------------------------------------------------------------------

 /**
  * To be able to deal with transactions, we must add some layers of interaction
  * for the preparation for closing, committing and rolling back of
  * transaction. These must be handled in the final implementation class.
  *
  * Note that there is no need for the StartTransacton method here, because
  * recall that the InputAdapter is driving the process.
  */

 /**
  * Flush Transaction finishes the output of any existing records in the pipe.
  * Any errors or potential error conditions should be handled here, because
  * the commit/rollback should be guaranteed successful operations.
  *
  * @param transactionNumber The transaction we are working on
  * @return 0 if everything flushed OK, otherwise -1
  */
  public abstract int flushTransaction(int transactionNumber);

 /**
  * Commit Transaction closes the transaction status with success
  *
  * @param transactionNumber The transaction we are working on
  */
  public abstract void commitTransaction(int transactionNumber);

 /**
  * Rollback Transaction closes the transaction status with failure
  *
  * @param transactionNumber The transaction we are working on
  */
  public abstract void rollbackTransaction(int transactionNumber);

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * @param transactionNumber The transaction we are working on
  */
  public abstract void closeTransaction(int transactionNumber);
}
