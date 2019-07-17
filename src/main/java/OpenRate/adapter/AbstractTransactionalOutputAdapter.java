package OpenRate.adapter;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.HeaderRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.transaction.ITMClient;
import OpenRate.transaction.TMDefs;
import OpenRate.transaction.TransactionManager;
import OpenRate.transaction.TransactionManagerFactory;

/**
 * This module adds the transactional elements to the output adapter.
 *
 * @author Ian
 */
public abstract class AbstractTransactionalOutputAdapter
        extends AbstractOutputAdapter
        implements ITMClient {

  // Get the Transaction Manager

  private TransactionManager TM;

  // used to update the status of this client instance
  private int TMClientNumber = 0;

  /**
   * Creates a new instance of AbstractTransactionalInputAdapter
   */
  public AbstractTransactionalOutputAdapter() {
    super();
  }

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
    super.init(PipelineName, ModuleName);

    // Register as a Transaction Manager client
    TM = TransactionManagerFactory.getTransactionManager(PipelineName);
    TMClientNumber = TM.registerClient(TMDefs.getTMDefs().CT_CLIENT_OUTPUT, this);
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * Mark the transaction as started when we get the start of stream header
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException {
    int currentTransactionNumber = r.getTransactionNumber();

    // Inform the client that we are opening the transaction
    startTransaction(currentTransactionNumber);

    // Mark that we have started a stream
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);

    return r;
  }

  /**
   * Process the stream trailer. Get the file base name and open the
   * transaction.
   *
   * @param r The record we are working on
   * @return The processed record
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    // recover the transaction number
    int currentTransactionNumber = r.getTransactionNumber();

    // Mark that we have finished the stream
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);

    return r;
  }

// -----------------------------------------------------------------------------
// --------------- Start of transactional layer functions ----------------------
// -----------------------------------------------------------------------------
  /**
   * This method returns the transaction number that we are currently working
   * on. If not transaction is in processing, returns 0.
   *
   * @return The current transaction number we are working on
   */
  public int getTransactionNumber() {
    return TM.getTransactionNumber(TMClientNumber);
  }

  /**
   * Set the transaction into the processing status
   *
   * @param transactionNumber The transaction to set
   */
  protected void setTransactionProcessing(int transactionNumber) {
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);
  }

  /**
   * Set the transaction into the flushed status
   *
   * @param transactionNumber The transaction to set
   */
  protected void setTransactionFlushed(int transactionNumber) {
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);
  }

  /**
   * Set the transaction into the abort state
   *
   * @param transactionNumber The transaction to set
   */
  protected void setTransactionAbort(int transactionNumber) {
    TM.requestTransactionAbort(transactionNumber);
  }

  /**
   * Do any non-record level processing required to finish this batch cycle.
   *
   * @return the number of records in the outbound buffer
   */
  @Override
  public int getOutboundRecordCount() {
    return super.getOutboundRecordCount();
  }

  /**
   * This is used to inform the client that an update has taken place to the
   * status of the transaction, and that we are now in the flush phase
   *
   * @param transactionNumber The number of the transaction to update
   * @return true if the flush was ok, otherwise false if there was an error
   */
  @Override
  public boolean updateTransactionStatusFlush(int transactionNumber) {
    // We have no work to do on flushing
    int retCode = flushTransaction(transactionNumber);

    return (retCode == 0);
  }

  /**
   * This is used to inform the client that an update has taken place to the
   * status of the transaction, and that we are now in the commit/rollback phase
   * and the transaction was processed correctly
   *
   * @param transactionNumber The number of the transaction to update
   */
  @Override
  public void updateTransactionStatusCommit(int transactionNumber) {
    // Call the finalisation of the processing
    commitTransaction(transactionNumber);
  }

  /**
   * This is used to inform the client that an update has taken place to the
   * status of the transaction, and that we are now in the close phase and the
   * transaction was processed with an error
   *
   * @param transactionNumber The number of the transaction to update
   */
  @Override
  public void updateTransactionStatusRollback(int transactionNumber) {
    // Call the finalisation of the processing
    rollbackTransaction(transactionNumber);
  }

  /**
   * This is used to inform the client that an update has taken place to the
   * status of the transaction, and that we are now in the close phase
   *
   * @param transactionNumber The number of the transaction to update
   */
  @Override
  public void updateTransactionStatusClose(int transactionNumber) {
    // Call the cleanup for the processing. This is post transaction, and is
    // used for clean up etc.
    closeTransaction(transactionNumber);
  }

  /**
   * See if there are any transactions in progress at the moment. We do this by
   * interrogating the transaction manager to see how many transactions are open
   * right now. If any are open, we return that we have work in progress.
   *
   * @param transactionNumber The number of the transaction to check for
   * @return true if we can start a transaction, otherwise false
   */
  protected boolean getTransactionAborted(int transactionNumber) {
    return TM.getTransactionAborted(transactionNumber);
  }

  /**
   * To be able to deal with transactions, we must add some layers of
   * interaction for the preparation for closing, committing and rolling back of
   * transaction. These must be handled in the final implementation class.
   */
  /**
   * Start transaction opens the transaction
   *
   * @param transactionNumber The transaction we are working on
   * @return 0 if the transaction started OK
   */
  public abstract int startTransaction(int transactionNumber);

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
   * Close Transaction is the trigger to clean up transaction related
   * information such as variables, status etc.
   *
   * @param transactionNumber The transaction we are working on
   */
  public abstract void closeTransaction(int transactionNumber);
}
