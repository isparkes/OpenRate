/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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
 * AbstractTransactionalPlugIn adds the interaction with the
 * Transaction Manager for the plug ins that wish to be transactional. This
 * layer is intended to encapsulate completely the interaction with the TM,
 * leaving derived classes free of the burden of dealing with transactionality,
 * which is fairly complex.
 *
 * Derived classes may be derived from this class, or the parent class
 * AbstractNotificationPlugIn and should behave in the same way, simplifying
 * greatly the derivation for transactional and non-transactional processing
 * plug ins.
 */
public abstract class AbstractTransactionalPlugIn
       extends AbstractPlugIn
       implements ITMClient
{
  // The Transaction Manager
  private TransactionManager TM;

  // used to update the status of this client instance
  private int TMClientNumber = 0;

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
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
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    super.init(PipelineName,ModuleName);

    // Register as a Transaction Manager client
    TM = TransactionManagerFactory.getTransactionManager(PipelineName);
    TMClientNumber  = TM.registerClient(TMDefs.getTMDefs().CT_CLIENT_PROC,this);
  }

 /**
  * This layer has to deal with updating the status of the transaction, so
  * that we can control the overall status of the processing.
  *
  */
  @Override
  public void process()
  {
    // Push the records through the pipeline
    super.process();
  }

 /**
  * Do any non-record level processing required to finish this batch cycle.
  *
  * @return The number of events in the output FIFO buffer
  */
  @Override
  public int getOutboundRecordCount()
  {
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
  public IRecord procHeader(IRecord r)
  {
    HeaderRecord tmpHeader;

    // recover the transaction number
    tmpHeader = (HeaderRecord)r;
    int currentTransactionNumber = tmpHeader.getTransactionNumber();

    // Inform the client that we are opening the transaction
    startTransaction(currentTransactionNumber);

    // Mark that we have started a stream
    TM.setClientStatus(currentTransactionNumber,TMClientNumber,TMDefs.getTMDefs().TM_PROCESSING);

    return r;
  }

 /**
  * The update status is called when the overall transaction status changes.
  * this is then used to trigger transaction level processing when we arrive at
  * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
  */
  @Override
  public boolean updateTransactionStatusFlush(int transactionNumber)
  {
    int FlushResult;

    FlushResult = flushTransaction(transactionNumber);

    if (FlushResult == 0)
    {
      // Mark that we have finished, and we should commit
      //TM.setClientStatus(TransactionNumber,TMClientNumber,TM.TM_FINISHED_OK);
      return true;
    }
    else
    {
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
  public void updateTransactionStatusCommit(int transactionNumber)
  {
    // Call the finalisation of the processing
    commitTransaction(transactionNumber);
  }

 /**
  * The update status is called when the overall transaction status changes.
  * this is then used to trigger transaction level processing when we arrive at
  * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
  */
  @Override
  public void updateTransactionStatusRollback(int transactionNumber)
  {
    // Call the finalisation of the processing
    rollbackTransaction(transactionNumber);
  }

 /**
  * The update status is called when the overall transaction status changes.
  * this is then used to trigger transaction level processing when we arrive at
  * "FLUSHED" or "FINISHED_OK" or "FINISHED_ERR".
  */
  @Override
  public void updateTransactionStatusClose(int transactionNumber)
  {
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
  public IRecord procTrailer(IRecord r)
  {
    TrailerRecord tmpTrailer;

    // recover the transaction number
    tmpTrailer = (TrailerRecord)r;
    int tmpCurrentTransactionNumber = tmpTrailer.getTransactionNumber();

    // Mark that we have finished the stream
    TM.setClientStatus(tmpCurrentTransactionNumber,TMClientNumber,TMDefs.getTMDefs().TM_FLUSHED);

    return r;
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
  public void abortTransaction(int transactionNumber)
  {
    TM.requestTransactionAbort(transactionNumber);
  }
}
