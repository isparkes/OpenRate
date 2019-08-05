package OpenRate.buffer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import ExampleApplications.SimpleApplication.SimpleRecord;
import OpenRate.record.IRecord;

public class LinkedListQueueBufferTest {

  private class Consumer implements Runnable {

    private final LinkedListQueueBuffer innerBuffer;


    Consumer(LinkedListQueueBuffer buffer) {
      this.innerBuffer = buffer;
    }

    @Override
    public void run() {
      while ((output.size() + 1) < MAX_RECORDS) {
        output.addAll(innerBuffer.pull(5));
      }
    }
  }

  private class Producer implements Runnable {

    private final LinkedListQueueBuffer innerBuffer;


    Producer(LinkedListQueueBuffer buffer) {
      this.innerBuffer = buffer;
    }

    @Override
    public void run() {
      while (idCounter < MAX_RECORDS) {
        Collection<IRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          IRecord record = new SimpleRecord();
          record.setRecordID(idCounter++);
          records.add(record);
        }

        innerBuffer.push(records);
      }
    }
  }


  private static final int MAX_RECORDS = 1_000_000;

  public boolean stopped;
  private int idCounter;
  private List<IRecord> output;
  private LinkedListQueueBuffer buffer;


  @Before
  public void init() {
    stopped = false;
    idCounter = 0;
    output = new ArrayList<>();
    buffer = new LinkedListQueueBuffer();
  }

  @Test
  public void testConcurrentPushAndPull() throws Exception {

    Thread consumerThread = new Thread(new Consumer(buffer));
    Thread producerThread = new Thread(new Producer(buffer));

    consumerThread.start();
    producerThread.start();

    producerThread.join();
    consumerThread.join();

    assertEquals(idCounter, output.size());
    assertEquals(idCounter, MAX_RECORDS);

    for (int i = 0; i < MAX_RECORDS; i++) {
      assertEquals(i, output.get(i).getRecordID());
    }
  }

}
