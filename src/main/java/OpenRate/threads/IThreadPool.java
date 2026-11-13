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
