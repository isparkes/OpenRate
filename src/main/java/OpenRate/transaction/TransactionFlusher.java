/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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

  // Longest time the flusher sleeps when there is nothing queued. A notify()
  // from addTransactionToFlushList() normally wakes it well before this.
  private static final long IDLE_WAIT_MS = 1000;

  // Short retry used when transactions are still queued but could not be
  // completed on this pass, for example because the client lock was held.
  private static final long PENDING_RETRY_MS = 50;

  @Override
  public void run()
  {
    while(true)
    {
      if (getFlushedTransactionCount() > 0)
      {
        updateTransactionStatus();
      }

      // Wait for notification from the supplier that new transactions are
      // available for processing. This wait must always be bounded:
      //
      //  - addTransactionToFlushList() may notify() between the check above
      //    and the wait below. That notification is lost, and an untimed
      //    wait() would leave the flusher asleep with work still queued.
      //  - updateTransactionStatus() returns without doing anything while the
      //    client lock is held, so queued work has to be picked up on a later
      //    pass rather than waiting for a notification that may never come.
      //
      // Either case previously hung the flusher permanently.
      try
      {
        synchronized (this)
        {
          wait(transFlushedList.isEmpty() ? IDLE_WAIT_MS : PENDING_RETRY_MS);
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
    // The transaction manager re-evaluates flush readiness on every client
    // status change, so guard against queueing the same transaction twice.
    if (transFlushedList.contains(trans))
    {
      return;
    }

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
  public synchronized int getFlushedTransactionCount()
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
