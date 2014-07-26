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

package OpenRate.adapter;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
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
  implements ITMClient
{
  // Get the Transaction Manager
  private TransactionManager TM;

  // used to update the status of this client instance
  private int TMClientNumber = 0;

  /** Creates a new instance of AbstractTransactionalInputAdapter */
  public AbstractTransactionalOutputAdapter()
  {
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
            throws InitializationException
  {
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
  public IRecord procHeader(IRecord r) throws ProcessingException
  {
    HeaderRecord tmpHeader;

    tmpHeader = (HeaderRecord)r;
    int currentTransactionNumber = tmpHeader.getTransactionNumber();

    // Inform the client that we are opening the transaction
    startTransaction(currentTransactionNumber);

    // Mark that we have started a stream
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);

    return r;
  }

 /**
  * Process the stream trailer. Get the file base name and open the transaction.
  *
  * @param r The record we are working on
  * @return The processed record
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    TrailerRecord tmpTrailer;

    // recover the transaction number
    tmpTrailer = (TrailerRecord)r;
    int currentTransactionNumber = tmpTrailer.getTransactionNumber();

    // Mark that we have finished the stream
    TM.setClientStatus(currentTransactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);

    return r;
  }

// -----------------------------------------------------------------------------
// --------------- Start of transactional layer functions ----------------------
// -----------------------------------------------------------------------------

 /**
  * This method returns the transaction number that we are currently working on.
  * If not transaction is in processing, returns 0.
  *
  * @return The current transaction number we are working on
  */
  public int getTransactionNumber()
  {
    return TM.getTransactionNumber(TMClientNumber);
  }

 /**
  * Set the transaction into the processing status
  *
  * @param transactionNumber The transaction to set
  */
  protected void setTransactionProcessing(int transactionNumber)
  {
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_PROCESSING);
  }

 /**
  * Set the transaction into the flushed status
  *
  * @param transactionNumber The transaction to set
  */
  protected void setTransactionFlushed(int transactionNumber)
  {
    TM.setClientStatus(transactionNumber, TMClientNumber, TMDefs.getTMDefs().TM_FLUSHED);
  }

 /**
  * Set the transaction into the abort state
  *
  * @param transactionNumber The transaction to set
  */
  protected void setTransactionAbort(int transactionNumber)
  {
    TM.requestTransactionAbort(transactionNumber);
  }

 /**
  * Do any non-record level processing required to finish this
  * batch cycle.
  */
  @Override
  public int getOutboundRecordCount()
  {
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
  public boolean updateTransactionStatusFlush(int transactionNumber)
  {
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
  public void updateTransactionStatusCommit(int transactionNumber)
  {
    // Call the finalisation of the processing
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
  public void updateTransactionStatusRollback(int transactionNumber)
  {
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
  public void updateTransactionStatusClose(int transactionNumber)
  {
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
  protected boolean getTransactionAborted(int transactionNumber)
  {
    return TM.getTransactionAborted(transactionNumber);
  }

 /**
  * To be able to deal with transactions, we must add some layers of interaction
  * for the preparation for closing, committing and rolling back of
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
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * @param transactionNumber The transaction we are working on
  */
  public abstract void closeTransaction(int transactionNumber);
}
