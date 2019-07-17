
package OpenRate.adapter.realTime;

import OpenRate.adapter.objectInterface.AbstractTeeAdapter;
import OpenRate.exception.ExceptionHandler;
import OpenRate.logging.AstractLogger;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.logging.Level;

/**
 * This module is the conversion bridge between real time processing and batch
 * processing. This is useful for example for making batch updates in order
 * to persist real time results into a database. Socket connections are used to
 * perform the communication to the batch pipeline.
 *
 * @author ian
 */
public class TeeBatchConverter implements Runnable
{
  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected AstractLogger PipeLog = null;

  // Used for reporting exceptions up to the pipe manager
  private ExceptionHandler handler;

  ArrayList<IRecord> outputBatch;

  // the current batch size
  private int outputCounter = 0;

  // the target batch size - we purge a batch when it gets this big
  private int targetBatchSize = 5000;

  // the default time we sleep for
  private int sleepTime = 5000;

  private AbstractTeeAdapter ParentAdapter;

 /**
  * Constructor
  */
  public void BatchConverter()
  {
  }

 /**
  * Add a RT time record to the pending batch output. In the case that we
  * have a low record volume, we can choose to purge the batch by setting
  * closeBatch to true.
  *
  * @param tmpRecord The record to add
  * @param closeBatch closes the batch for purging if true
  */
  public synchronized void addRecordToOutputBatch(IRecord tmpRecord, boolean closeBatch)
  {
    String transID = "";

    if (closeBatch)
    {
      // we are tired of waiting - purge
      TrailerRecord tmpTrailer = new TrailerRecord();
      tmpTrailer.setStreamName(transID);
      outputBatch.add(tmpTrailer);
      outputCounter = 0;

      // Push the completed batch
      ParentAdapter.pushTeeBatch(outputBatch);

      // reset the batch
      outputBatch = null;
    }
    else
    {
      // Push into the batch output
      if (outputCounter == 0)
      {
        // we are starting a new batch
        transID = ""+Calendar.getInstance().getTimeInMillis();
        HeaderRecord tmpHeader = new HeaderRecord();
        tmpHeader.setStreamName(transID);
        outputBatch = new ArrayList<>();
        outputBatch.add(tmpHeader);
      }

      outputBatch.add(tmpRecord);
      outputCounter++;

      if (outputCounter >= targetBatchSize)
      {

        TrailerRecord tmpTrailer = new TrailerRecord();
        tmpTrailer.setStreamName(transID);
        outputBatch.add(tmpTrailer);
        outputCounter = 0;

        // Push the completed batch
        ParentAdapter.pushTeeBatch(outputBatch);

        // reset the batch
        outputBatch = null;
      }
    }
  }

 /**
  * This thread purges the buffered records to the output stream, either when
  * we have reached the target batch size, or when we just get bored of waiting.
  */
  @Override
  public void run()
  {
    boolean threadActive = true;
    int sleepCounter = 0;

    while (threadActive)
    {
      try
      {
        // if we had no work to do, think about a sleep
        if (outputCounter == 0)
        {
          Thread.sleep(sleepTime);

          sleepCounter = 0;
        }
        else
        {
          Thread.sleep(100);
          sleepCounter += 100;

          if (sleepCounter > sleepTime)
          {
            // set the batch for purge
            addRecordToOutputBatch(null, true);
          }
        }
      }
      catch (InterruptedException ex)
      {
        java.util.logging.Logger.getLogger(TeeBatchConverter.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }

 /**
  * Set the log location for this thread
  *
  * @param newPipeLog
  */
  void setPipelineLog(AstractLogger newPipeLog)
  {
    this.PipeLog = newPipeLog;
  }

  void setHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }

  void setBatchSize(int newBatchSize)
  {
    this.targetBatchSize = newBatchSize;
  }

  int getBatchSize()
  {
    return this.targetBatchSize;
  }

  void setPurgeTime(int newPurgeTime)
  {
    this.sleepTime = newPurgeTime;
  }

  int getPurgeTime()
  {
    return this.sleepTime;
  }

  /**
   *
   *
   * @param aThis
   */
  public void setParentAdapter(AbstractTeeAdapter aThis)
  {
    this.ParentAdapter = aThis;
  }
}
