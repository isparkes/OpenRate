

package OpenRate.transaction;

import OpenRate.logging.ILogger;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is responsible for closing flushed transactions. Flushed
 * transactions have finished traversing the pipeline, and need only to have the
 * final commit and closing done on them. This is externalised into a separate
 * thread for performance reasons.
 *
 * @author tgdspia1
 */
public class TransactionFlusher implements Runnable
{
  // used for managing overlaid transactions
  private ReentrantReadWriteLock clientLock = new ReentrantReadWriteLock();

  // The list of the transactions we are closing
  private ArrayList<TransactionInfo>transFlushedList   = new ArrayList<>();

  // Common Definitions for the transaction manager
  private TMDefs TMD = new TMDefs();

  // Callback to the TM
  TransactionManager TM;

  // The name of the pipe we are working for
  private String pipelineName;

  // Our logger
  private ILogger pipeLog;

  // Scheduler
  private int sleepTime;

  @Override
  public void run()
  {
    while(true)
    {
      if (getFlushedTransactionCount() > 0)
      {
        updateTransactionStatus();
        sleepTime = 0;
      }
      else
      {
        sleepTime = 1000;
      }
      /*try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex)
      {
        // Ignore
      }*/

      // If not marked for shutdown, wait for notification from the
      // suppler that new records are available for processing.
      try
      {
        synchronized (this)
        {
          wait();
        }
      }
      catch (InterruptedException e)
      {
        // ignore interrupt exceptions
      }
    }
  }

 /**
  * Put a transaction into the flush list
  *
  * @param trans
  */
  public synchronized void addTransactionToFlushList(TransactionInfo trans)
  {
    transFlushedList.add(trans);
    pipeLog.debug("Added transaction <"+trans.getTransactionNumber()+"> to flusher for pipe <"+pipelineName+">");
    this.notify();
  }

 /**
  * Update the overall status and in the case that we have a state change (for
  * example during the asynchronous closing portion of the transaction) deal
  * with the state change. In the future we might put this into a separate
  * thread.
  */
  public void updateTransactionStatus()
  {	  
    int     i;
    int     NewOverallStatus;
    TransactionInfo cachedTrans;
    Integer transNumber;

    if (clientLock.isWriteLocked())
    {
      return;
    }

    try
    {
      // Get the lock
      clientLock.writeLock().lock();

      // Check the status of the transactions
      while (transFlushedList.size() > 0)
      {
        cachedTrans = transFlushedList.get(0);
        transNumber = cachedTrans.getTransactionNumber();

        // Calculate the new status
        NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);

        // If we had a state change, inform the clients if there was an overall state change
        while (transNumber > 0)
        {
          if (NewOverallStatus == TMD.TM_FLUSHED)
          {
            // inform each of the clients in turn
            for (i = 1; i <= TM.getClientCount(); i++)
            {
              if (TM.getClient(i).updateTransactionStatusFlush(transNumber))
              {
                // Set the overall status for this client to OK
                cachedTrans.setClientStatus(i, TMD.TM_FINISHED_OK);
              }
              else
              {
                // Set the overall status for this client to OK
                cachedTrans.setClientStatus(i, TMD.TM_FINISHED_ERR);
              }

              // Update the status
              NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);
            }
          }
          else if (NewOverallStatus == TMD.TM_FINISHED_OK)
          {
            // inform each of the clients in turn
            for (i = 1; i <= TM.getClientCount(); i++)
            {
              TM.getClient(i).updateTransactionStatusCommit(transNumber);

              // Set the overall status for this client to OK
              cachedTrans.setClientStatus(i, TMD.TM_CLOSING);

              // Update the status
              NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);
            }
          }
          else if (NewOverallStatus == TMD.TM_FINISHED_ERR)
          {
            // inform each of the clients in turn
            for (i = 1; i <= TM.getClientCount(); i++)
            {
              TM.getClient(i).updateTransactionStatusRollback(transNumber);

              // Set the overall status for this client to closing
              cachedTrans.setClientStatus(i, TMD.TM_CLOSING);

              // Update the status
              NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);
            }
          }
          else if (NewOverallStatus == TMD.TM_CLOSING)
          {
            // inform each of the clients in turn
            for (i = 1; i <= TM.getClientCount(); i++)
            {
              TM.getClient(i).updateTransactionStatusClose(transNumber);

              // Set the overall status for this client to all done
              cachedTrans.setClientStatus(i, TMD.TM_CLOSED);

              // Update the status
              NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);
            }
          }
          else if (NewOverallStatus == TMD.TM_CLOSED)
          {
            // Finish up and remove the transaction
            TM.closeTransaction(transNumber);
            transFlushedList.remove(0);
            pipeLog.debug(transFlushedList.size() + " transactions to flush for pipe <"+pipelineName+">");
            transNumber = 0;
          }
          else
          {
            // Update the status
            NewOverallStatus = TM.getOverallStatus(transNumber,cachedTrans);
          }
        }
      }
    }
    finally
    {
      // Release the lock
      clientLock.writeLock().unlock();
    }
  }

 /**
  * Get the number of transactions which have been flushed but not yet
  * finalised.
  *
  * @return the count.
  */
  public int getFlushedTransactionCount()
  {
    return transFlushedList.size();
  }

 /**
  * Set the reference for sending updates back to the transaction manager.
  *
  * @param newTM The TM we are serving.
  */
  public void setTMReference(TransactionManager newTM)
  {
    TM = newTM;
  }

 /**
  * Set the pipeline name for use in the log.
  *
  * @param newPipelineName The pipeline name
  */
  void setPipelineName(String newPipelineName)
  {
    pipelineName = newPipelineName;
  }

 /**
  * Set the logger we are using for this flusher.
  *
  * @param newPipeLog The pipe log we are logging to
  */
  void setLogger(ILogger newPipeLog)
  {
    pipeLog = newPipeLog;
  }

}
