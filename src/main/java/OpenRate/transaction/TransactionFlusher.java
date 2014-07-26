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
