

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
    pool       = new ArrayList<>(size);

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
