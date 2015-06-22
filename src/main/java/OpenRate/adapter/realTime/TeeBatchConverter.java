/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
