

package OpenRate.transaction;

/**
 * The Transaction Manager coordinates the processing of files and
 * serves as a central point for synchronising the state of the
 * pipeline modules.
 */
public interface ITransactionManager
{
  /**
   * The Key used to get the CacheFactory from the
   * configuration settings.
   */
  public static final String KEY = "Resource.TransactionManager";

  /**
   * Create a new transaction
   *
   * @param pipeline The name of the pipeline that opened the transaction
   * @return The number of the new transaction
   */
  public int openTransaction(String pipeline);

 /**
  * Close the transaction
  *
  * @param TransactionNumber The number of the transaction to close
  */
  public void closeTransaction(int TransactionNumber);

 /**
  * Ask other clients to abort transaction
  *
  * @param TransactionNumber The number of the transaction to abort
  */
  public void requestTransactionAbort(int TransactionNumber);

 /**
  * Cancel and Close the transaction
  *
  * @param TransactionNumber The number of the transaction to cancel
  */
  public void cancelTransaction(int TransactionNumber);

  /**
   * Return the count of the active transactions. This is used primarily in
   * pipeline scheduling.
   *
   * @return The number of active transactions
   */
  public int getActiveTransactionCount();

  /**
   * Return the count of the flushed transactions. This is used primarily in
   * pipeline scheduling.
   *
   * @return The number of flushed transactions awaiting closure
   */
  public int getFlushedTransactionCount();

  /**
   * Get the overall transaction status. This is the processed summation of the
   * statuses of the individual client processes, processed to give the current
   * processing status of the transaction
   *
   * @param transNumber The number of the transaction
   * @return The current status
   */
  public int getTransactionStatus(int transNumber);

 /**
  * Test support - not normally needed in real life: Resets the clients between
  * tests: Allows us to use a generic test set for different modules. If we
  * don't do this, we collect clients, but during tests we don't maintain them
  * correctly.
  */
  public void resetClients();
}
