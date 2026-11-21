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
 * Lets a pipeline worker wait for more work without losing a wakeup.
 *
 * The pattern this replaces, which every batch worker in the framework had,
 * was:
 *
 * <pre>
 *   records = buffer.pull(batchSize);
 *
 *   if (records.isEmpty()) {
 *     if (shutdownFlag) break;
 *     synchronized (this) { wait(); }
 *   }
 * </pre>
 *
 * with the upstream module calling notifyAll() on the same object when it
 * pushed, and markForShutdown() doing the same.
 *
 * Nothing held a lock across the pull and the wait, so a push, or a
 * shutdown, that landed in the gap between them notified a thread that was
 * not waiting yet - and the worker then waited anyway, although there was
 * something for it. In mid stream the next push papers over it. At the end
 * of a stream, or against markForShutdown(), there is no next push: the
 * worker waits for a notify that has already been and gone, and the pipeline
 * stops dead with records still in it.
 *
 * Here the worker arms the latch before it goes looking for work, so a
 * signal arriving from that moment on is still recorded when the worker
 * decides whether to wait.
 *
 * @author ian
 */
public class WakeupLatch
{
  // Set by signal(), cleared by arm(). Guarded by this.
  private boolean signalled = false;

 /**
  * Called by the worker immediately before it looks for work. Any signal
  * from this point on will stop the following await() from blocking.
  */
  public synchronized void arm()
  {
    signalled = false;
  }

 /**
  * Called by whoever has given the worker something to do, or has told it to
  * stop.
  */
  public synchronized void signal()
  {
    signalled = true;

    notifyAll();
  }

 /**
  * Called by the worker when it looked for work and found none. Returns at
  * once if anything has signalled since arm(), otherwise waits to be
  * signalled.
  *
  * As with the plain wait() it replaces, a return does not promise that
  * there is work - the caller loops round and looks again.
  */
  public synchronized void await()
  {
    if (signalled)
    {
      return;
    }

    try
    {
      wait();
    }
    catch (InterruptedException ex)
    {
      // Put the flag back for whoever is above us, and let the caller round
      // the loop again to find out whether it should stop.
      Thread.currentThread().interrupt();
    }
  }
}
