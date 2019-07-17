

package OpenRate.threads;


/**
 * Interface for IThreadPool Implementations.
 */
public interface IThreadPool
{
  /**
   * Executes the given work. Typical IThreadPool implementations
   * will use a worker thread or some sort to execute the work.
   *
   *
   * @param work - work to be executed by thread pool.
   */
  public void execute(Runnable work);

  /**
   * Wait for all the worker thread to finish, essentially join all
   * threads in the pool..
   */
  public void join();

  /**
     * Closes the IThreadPool. Typically, implementing classes will terminate
     * all the worker threads and IThreadPool becomes un-usable.
     */
  public void close();
}
