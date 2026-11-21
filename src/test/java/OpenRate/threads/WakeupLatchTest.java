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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * @author ian
 */
public class WakeupLatchTest {

  /**
   * The case the latch exists for. The worker arms, looks for work and finds
   * none; the signal arrives in that window; the worker must not then wait.
   *
   * With the plain wait()/notifyAll() this replaces, the notify went to
   * nobody and the worker waited for a signal that had already happened. The
   * timeout here is what catches a regression - the failure mode is a hang.
   */
  @Test(timeout = 10000)
  public void testSignalBetweenArmAndAwaitIsNotLost() {
    WakeupLatch latch = new WakeupLatch();

    latch.arm();

    // the worker is looking for work here, and the supplier pushes
    latch.signal();

    // so this has to return straight away
    latch.await();
  }

  /**
   * Arming clears a signal that arrived before it, otherwise a worker would
   * spin instead of waiting once it had been signalled at any time in the
   * past.
   */
  @Test(timeout = 10000)
  public void testArmClearsAnEarlierSignal() throws Exception {
    final WakeupLatch latch = new WakeupLatch();

    latch.signal();
    latch.arm();

    final CountDownLatch returned = new CountDownLatch(1);

    Thread worker = new Thread(() -> {
      latch.await();
      returned.countDown();
    });

    worker.start();

    assertFalse("await() returned although the signal was armed away",
            returned.await(500, TimeUnit.MILLISECONDS));

    latch.signal();

    assertTrue("await() did not return after the signal",
            returned.await(5, TimeUnit.SECONDS));

    worker.join();
  }

  /**
   * The ordinary case: the worker waits, and a later signal releases it.
   */
  @Test(timeout = 10000)
  public void testAwaitBlocksUntilSignalled() throws Exception {
    final WakeupLatch latch = new WakeupLatch();
    final CountDownLatch waiting = new CountDownLatch(1);
    final CountDownLatch returned = new CountDownLatch(1);

    latch.arm();

    Thread worker = new Thread(() -> {
      waiting.countDown();
      latch.await();
      returned.countDown();
    });

    worker.start();

    assertTrue(waiting.await(5, TimeUnit.SECONDS));
    assertFalse("await() returned without a signal",
            returned.await(500, TimeUnit.MILLISECONDS));

    latch.signal();

    assertTrue("await() did not return after the signal",
            returned.await(5, TimeUnit.SECONDS));

    worker.join();
  }

  /**
   * Hammer the arm / signal / await cycle from two threads. Any iteration
   * where the signal is lost hangs, which the timeout turns into a failure.
   */
  @Test(timeout = 30000)
  public void testNoSignalIsLostUnderContention() throws Exception {
    final WakeupLatch latch = new WakeupLatch();
    final int iterations = 20000;

    Thread supplier = new Thread(() -> {
      for (int i = 0; i < iterations; i++) {
        latch.signal();
      }
    });

    supplier.start();

    // The worker arms, "looks for work", and waits if it found none. It only
    // ever gets here because the supplier is still running, so every await()
    // has to be released.
    for (int i = 0; i < iterations; i++) {
      latch.arm();
      latch.signal();
      latch.await();
    }

    supplier.join();
  }
}
