package OpenRate.transaction;

/**
 * The ITMClient interface registers a class as a client of
 * the transaction manager, meaning that the processing is conducted by the
 * centralised transaction manager class, to allow processing to be committed
 * or rolled back based on the overall status of the processing.
 *
 * The interface defines a number of control variables, and the methods that
 * must be hooked to perform the interaction with the TM.
 */
public interface ITMClient
{
 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the flush phase
  *
  * @param transactionNumber The number of the transaction to update
  * @return true if the flush was ok, otherwise false if there was an error
  */
  public boolean updateTransactionStatusFlush(int transactionNumber);

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the commit/rollback phase
  * and the transaction was processed correctly
  *
  * @param transactionNumber The number of the transaction to update
  */
  public void updateTransactionStatusCommit(int transactionNumber);

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the close phase
  * and the transaction was processed with an error
  *
  * @param transactionNumber The number of the transaction to update
  */
  public void updateTransactionStatusRollback(int transactionNumber);

 /**
  * This is used to inform the client that an update has taken place to the
  * status of the transaction, and that we are now in the close phase
  *
  * @param transactionNumber The number of the transaction to update
  */
  public void updateTransactionStatusClose(int transactionNumber);
}
