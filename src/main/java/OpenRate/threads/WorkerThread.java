

package OpenRate.threads;

/**
 * Worker Threads performs a given work and sleeps after the work is finished.
 *
 * The thread is managed by a IThreadPool and is used in conjuction with a
 * IThreadPool.
 *
 * The threads sleeps as long as there is no work to be performed.
 * IThreadPool assigns work to the thread and wakes up the WorkerThread to
 * perform the given work. Once the thread finishes the work it sets itself
 * to as having no work.
 */
public class WorkerThread extends Thread
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: WorkerThread.java,v $, $Revision: 1.18 $, $Date: 2013-05-13 18:12:13 $";

  // assigned work
  private Runnable work = null;

  // indicates if the thread is marked for shutdown by the IThreadPool.
  private boolean shutDown = false;

  /**
   * Constructor - constructs a WorkerThread
   *
   * @param pool - reference to the managing IThreadPool
   */
  public WorkerThread(IThreadPool pool)
  {
    setDaemon(true);
  }

  /**
   * Perform the following steps, in a loop, until asked to shutdown:
   *  1. If there is no work then go to sleep ( wait ).
   *  2. If there is work:
   *      - perform the work
   *      - reset work to null
   *  3. Exit the loop, if asked to shutdown.
   */
  @Override
  public void run()
  {
    while (true)
    {
      if (noWork())
      {
        try
        {
          synchronized (this)
          {
            wait();
          }
        }
        catch (InterruptedException e)
        {
          // ignore
        }
      }
      else
      {
        work.run();
        setNoWork();
      }

      if (shutDown)
      {
        break;
      }
    }
  }

  /**
   * return true is there is no work to be performed.
   *
   * @return boolean
   */
  public boolean noWork()
  {
    return work == null;
  }

  /**
   * assign no work to the worked thread.
   */
  public void setNoWork()
  {
    work = null;
  }

  /**
   * mark the thread to be shutdown.
   */
  public void markForShutDown()
  {
    shutDown = true;
  }

  /**
   * Sets the work to be performed by the thread.
   *
   * @param action - action to be performed by the thread
   */
  public void setWork(Runnable action)
  {
    work = action;
  }
}
