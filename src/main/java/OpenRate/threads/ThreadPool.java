/* ====================================================================
 * Limited Evaluation License:
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

package OpenRate.threads;

import java.util.ArrayList;

/**
 * Custom IThreadPool Implementation. The IThreadPool is started with predefined
 * set of WorkerThreads which actually perform the work.
 *
 * This IThreadPool is suitable for scenarios work is split across
 * mutiple threads and main thread waits for all the worker threads to
 * finish and assigns work to all (or part) of worker threads.
 *
 * The IThreadPool can be asked to find a worked thread and assign work to it using
 * execute method. Using join() method the caller will wait for all the worker
 * threads to finish.
 *
 * IMPORTANT NOTE(S):
 *
 * 1. This IThreadPool is suitable for use by a single thread, not by mutliple
 *    threads/requests    requesting IThreadPool to execute work.
 * 2. It is the responsibility of the caller to make sure that it requests at
 *    maximum 'poolsize' amount of work to be performed by the IThreadPool.
 *    If no workers are free an RuntimeException is thrown.
 */
public class ThreadPool implements IThreadPool
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: ThreadPool.java,v $, $Revision: 1.18 $, $Date: 2013-05-13 18:12:13 $";

  // maintain a pool of worker threads.
  ArrayList<WorkerThread> pool = null;

  // pool size
  int poolSize = 1;

  /**
     * Constructor - constructs a IThreadPool of given size. Constructs
     * returns after all the threads have been created and started.
     *
     *
     * @param size - size of the pool
     */
  public ThreadPool(int size)
  {
    poolSize   = size;
    pool       = new ArrayList<WorkerThread>(size);

    for (int i = 0; i < size; i++)
    {
      WorkerThread wThread = new WorkerThread(this);
      wThread.setName("WorkerThread_" + i);
      wThread.start();
      pool.add(wThread);
    }
  }

  /**
   * Executes a given 'work'. Finds a free worker thread to execute the
   * work - sets the work and notifies the worked thread to wake up
   * and perform the work.
   *
   * @param work - work to be performed.
   * @throws RuntimeException
   */
  @Override
  public void execute(Runnable work) throws RuntimeException
  {
    boolean allWorkersBusy = true;

    for (int i = 0; i < poolSize; i++)
    {
      WorkerThread wThread = pool.get(i);

      if (wThread.noWork())
      {
        wThread.setWork(work);

        synchronized (wThread)
        {
          wThread.notify();
        }

        allWorkersBusy = false;

        break;
      }
    }

    if (allWorkersBusy)
    {
      throw new RuntimeException(
        "All Workers are busy could not perform the work.!");
    }
  }

  /**
   * Wait for all the worker threads to finish. If there are workers still
   * performing the work, sleep for a second and then check again.
   */
  @Override
  public void join()
  {
    while (true)
    {
      boolean allWorkersFree = true;

      for (int i = 0; i < poolSize; i++)
      {
        WorkerThread wThread = pool.get(i);

        if (!wThread.noWork() && wThread.isAlive())
        {
          try
          {
            allWorkersFree = false;
            Thread.sleep(1000);
          }
          catch (InterruptedException e)
          {
            // ignore exception and continue.
          }

          break; // come out of worker thread loop.
        }
      }

      if (allWorkersFree)
      {
        // enough of waiting, all finished, return.
        break;
      }
    }
  }

  /**
   * Mark all threads to shutdown and notify them, just in case they are
   * sleeping.
   */
  @Override
  public void close()
  {
    for (int i = 0; i < poolSize; i++)
    {
      WorkerThread wThread = pool.get(i);
      wThread.markForShutDown();

      synchronized (wThread)
      {
        wThread.notify();
      }
    }
  }
}
