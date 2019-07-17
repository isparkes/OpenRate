
package OpenRate.process;

import OpenRate.exception.InitializationException;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.transaction.ITMClient;
import OpenRate.transaction.TMDefs;
import OpenRate.transaction.TransactionManager;
import OpenRate.transaction.TransactionManagerFactory;

/**
 * AbstractTransactionalPlugIn adds the interaction with the Transaction Manager
 * for the plug ins that wish to be transactional. This layer is intended to
 * encapsulate completely the interaction with the TM, leaving derived classes
 * free of the burden of dealing with transactionality, which is fairly complex.
 *
 * Derived classes may be derived from this class, or the parent class
 * AbstractNotificationPlugIn and should behave in the same way, simplifying
 * greatly the derivation for transactional and non-transactional processing
 * plug ins.
 */
public abstract class AbstractTransactionalPlugIn
        extends AbstractPlugIn
        implements ITMClient {

  // The Transaction Manager

  private TransactionManager TM;

  // used to update the status of this client instance
  private int TMClientNumber = 0;

  /**
   * Initialise the module. Called during pipeline creation to initialise: -
   * Configuration properties that are defined in the properties file. - The
   * references to any cache objects that are used in the processing - The
   * symbolic name of the module
   *
   * This layer must only register itself with the Transaction Manager so that
   * we are able to interact with it to control the state of the work we are
   * doing.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The name of this module in the pipeline
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException {
    super.init(PipelineName, ModuleName);

    // Register as a Transaction Manager client
    TM = TransactionManagerFactory.getTransactionManager(PipelineName);
    TMClientNumber = TM.registerClient(TMDefs.getTMDefs().CT_CLIENT_PROC, this);
  }

  /**
   * This layer has to deal with updating the status of the transaction, so that
   * we can control the overall status of the processing.
   *
   */
  @Override
  public void process() {
    // Push the records through the pipeline
    super.process();
  }

  /**
   * Do any non-record level processing required to finish this batch cycle.
   *
   * @return The number of events in the output FIFO buffer
   */
  @Override
  public int getOutboundRecordCount() {
    // Maintain the transaction
    //CheckTransStatus();

    return super.getOutboundRecordCount();
  }

  /**
   * Mark the transaction as started when we get the start of stream header
   *
   * @param r The header record
   * @return The unmodified header record
   */
  @Override
  public IRecord procHeader(IRecord r) {
    HeaderRecord tmpHeader;

    // recover the transaction number
    tmpHeader = (HeaderRecord) r;
    int currentTransactionNumber = tmpHeader.getTransactionNumber();

    // Inform the client that we are opening the transaction
    startTransaction(currentTransactionNumber);

    // Mark that we have started a stream
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);

    return r;
  }

  /**
   * The update status is called when the overall transaction status changes.
   * this is then used to trigger transaction level processing when we arrive at
   * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
   */
  @Override
  public boolean updateTransactionStatusFlush(int transactionNumber) {
    int FlushResult;

    FlushResult = flushTransaction(transactionNumber);

    if (FlushResult == 0) {
      // Mark that we have finished, and we should commit
      //TM.setClientStatus(TransactionNumber,TMClientNumber,TM.TM_FINISHED_OK);
      return true;
    } else {
      // Mark that we have finished, and we should rollback
      //TM.setClientStatus(TransactionNumber,TMClientNumber,TM.TM_FINISHED_ERR);
      return false;
    }
  }

  /**
   * The update status is called when the overall transaction status changes.
   * this is then used to trigger transaction level processing when we arrive at
   * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
   */
  @Override
  public void updateTransactionStatusCommit(int transactionNumber) {
    // Call the finalisation of the processing
    commitTransaction(transactionNumber);
  }

  /**
   * The update status is called when the overall transaction status changes.
   * this is then used to trigger transaction level processing when we arrive at
   * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
   */
  @Override
  public void updateTransactionStatusRollback(int transactionNumber) {
    // Call the finalisation of the processing
    rollbackTransaction(transactionNumber);
  }

  /**
   * The update status is called when the overall transaction status changes.
   * this is then used to trigger transaction level processing when we arrive at
   * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
   */
  @Override
  public void updateTransactionStatusClose(int transactionNumber) {
    // Call the cleanup for the processing. This is post transaction, and is
    // used for clean up etc.
    closeTransaction(transactionNumber);
  }

  /**
   * Mark the transaction as flushed when we get the end of stream trailer
   *
   * @param r The trailer record
   * @return The unmodified trailer record
   */
  @Override
  public IRecord procTrailer(IRecord r) {
    TrailerRecord tmpTrailer;

    // recover the transaction number
    tmpTrailer = (TrailerRecord) r;
    int tmpCurrentTransactionNumber = tmpTrailer.getTransactionNumber();

    // Mark that we have finished the stream
    TM.setClientStatus(tmpCurrentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);

    return r;
  }

  /**
   * This method returns the transaction number that we are currently working
   * on. If not transaction is in processing, returns 0.
   *
   * @return The current transaction number
   */
  public int getTransactionNumber() {
    return TM.getTransactionNumber(TMClientNumber);
  }

// -----------------------------------------------------------------------------
// --------------- Start of transactional layer functions ----------------------
// -----------------------------------------------------------------------------
  /**
   * To be able to deal with transactions, we must add some layers of
   * interaction for the preparation for closing, committing and rolling back of
   * transaction. These must be handled in the final implementation class.
   */
  /**
   * See if we can start the transaction
   *
   * @param transactionNumber The number of the transaction
   * @return 0 if the transaction can start
   */
  public abstract int startTransaction(int transactionNumber);

  /**
   * See if the transaction was flushed correctly
   *
   * @param transactionNumber The number of the transaction
   * @return 0 if the transaction was flushed OK
   */
  public abstract int flushTransaction(int transactionNumber);

  /**
   * Do the work that is needed to commit the transaction
   *
   * @param transactionNumber The number of the transaction
   */
  public abstract void commitTransaction(int transactionNumber);

  /**
   * Do the work that is needed to roll the transaction back
   *
   * @param transactionNumber The number of the transaction
   */
  public abstract void rollbackTransaction(int transactionNumber);

  /**
   * Do the work that is needed to close the transaction after commit/rollback.
   * This usually is used for cleaning up variables, states etc.
   *
   * @param transactionNumber The number of the transaction
   */
  public abstract void closeTransaction(int transactionNumber);

  /**
   * Request that the transaction be aborted
   *
   * @param transactionNumber The number of the transaction
   */
  public void abortTransaction(int transactionNumber) {
    TM.requestTransactionAbort(transactionNumber);
  }
}
