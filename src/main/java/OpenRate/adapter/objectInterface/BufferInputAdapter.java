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

package OpenRate.adapter.objectInterface;

import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.buffer.ISupplier;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.resource.LinkedBufferCache;
import OpenRate.resource.ResourceContext;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * The buffer input adapter allows us to link pipelines together using buffers.
 * In particular, this is useful for adding persistence to a real time pipeline
 * where the writing to a table or file should happen after real time processing
 * has happened.
 *
 * This module tees into a Real Time pipeline and takes a feed of the events
 * for putting into a batch pipeline. This is usually used for persistence
 * of RT events in a batch mode, however, it can also be used for balance
 * updates in a batch pipeline.
 *
 * Buffer Input Adapter
 * --------------------
 * The output of the buffer tee adapter allows you to "sniff" events out of
 * a pipeline (real time or batch) and put them into another pipeline (batch)
 * for persistence or further processing. This converts synchronous events into
 * asynchronous events. The coupling between the two pipelines is a normal FIFO
 * buffer. The buffer is accessed using a "LinkedBufferCache" which allows the
 * FIFO to be set up and accessed by name.
 *
 * Input >->->- Pipeline 1 ->->->- Buffer Tee Adapter ->->-> Output
 *                                     |
 *   +------------- Buffer ------------+
 *   |
 *   +-> Buffer Input Adapter >->- Pipeline 2 ->->->-> Output
 *
 */
public abstract class BufferInputAdapter
  extends AbstractTransactionalInputAdapter
{
  /**
   * Used to track the status of the number of the record we are processing
   */
  protected int InputRecordNumber = 0;

  // This is our linked FIFO buffer
  private ISupplier linkedInputBuffer;

  // This tells us if we should look for new work or continue with something
  // that is going on at the moment
  private boolean InputStreamOpen = false;

 /**
  * Holds the time stamp for the transaction
  */
  protected String ORTransactionId = null;

  private int TransactionCounter = 0;

  /**
   * Default Constructor
   */
  public BufferInputAdapter()
  {
    super();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of inherited Input Adapter functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * It is necessary in this class to have a local buffer to collects records
  * between the start of the processing and the push of the records into the
  * normal processing chain. This is achieved with a local buffer of the
  * default buffer type.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    // Register ourself with the client manager
    super.init(PipelineName, ModuleName);

    // Find the buffer supplier from the buffer list
    String ConfigHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(), "CacheName", "NONE");

    if (ConfigHelper == null || ConfigHelper.equals("NONE"))
    {
      throw new InitializationException ("Please set the name of the buffer cache");
    }

    // get the buffer reference
    ResourceContext ctx    = new ResourceContext();

    // get the reference to the buffer cache
    LinkedBufferCache LBC = (LinkedBufferCache) ctx.get(ConfigHelper);

    if (LBC == null)
    {
      String Message = "Could not find cache entry for <" + ConfigHelper + ">";
      throw new InitializationException(Message);
    }

    ConfigHelper = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(), "BufferName", "NONE");

    if (ConfigHelper == null || ConfigHelper.equals("NONE"))
    {
      throw new InitializationException ("Please set the name of the module to link to");
    }

    linkedInputBuffer = LBC.getSupplier(ConfigHelper);

    // Check to see if we have the naughty batch size of 0. this is usually
    // because someone has overwritten the init() without calling the parent
    // init
    if (BatchSize == 0)
    {
      throw new InitializationException("Batch size is zero");
    }

  }

 /**
  * loadBatch() is called regularly by the framework to either process records
  * or to scan for work to do, depending on whether we are already processing
  * or not.
  *
  * The way this works is that we assign a batch of files to work on, and then
  * work our way through them. This minimizes the directory scans that we have
  * to do and improves performance.
  */
  @Override
  protected Collection<IRecord> loadBatch() throws ProcessingException
  {
    HeaderRecord tmpHeader;
    TrailerRecord tmpTrailer;

    // iterator for running through the events
    Iterator<IRecord> iter;

    // processing list for batch events
    Collection<IRecord> in;

    // Print the thread startup message
    StatsLog.debug("PlugIn <" + Thread.currentThread().getName() +
                   "> started, pulling from buffer <" + linkedInputBuffer.toString() +
                   ">, pushing to buffer <" + getBatchOutboundValidBuffer().toString() + ">");

    // The Record types we will have to deal with
    ArrayList<IRecord> Outbatch = new ArrayList<>();

    // This layer deals with opening the stream if we need to
    if (linkedInputBuffer.getEventCount()>0 )
    {
      if (InputStreamOpen == false)
      {
        if (canStartNewTransaction())
        {
          // we can start a new transaction
          in = linkedInputBuffer.pull(BatchSize);

          // Active loop
          iter = in.iterator();

          // Process each of the block of records and trigger the processing
          // functions for each type (header, trailer, valid and error)
          while (iter.hasNext())
          {
            // Get the formatted information from the record
            IRecord r = iter.next();

            // Trigger the correct user level functions according to the state of
            // the record
            if (r.isValid())
            {
              r = procValidRecord(r);
            }
            else
            {
              if (r.isErrored())
              {
                r = procErrorRecord(r);
              }
              else
              {
                if (r instanceof HeaderRecord)
                {
                  InputStreamOpen = true;

                  // This is the transaction identifier for all records in this stream
                  ORTransactionId = ""+new Date().getTime();

                  // Create a new transaction
                  TransactionCounter = createNewTransaction();

                  // Inform the transactional layer that we have started processing
                  setTransactionProcessing(TransactionCounter);

                  // Inject a stream header record into the stream
                  tmpHeader = (HeaderRecord) r;
                  tmpHeader.setStreamName(ORTransactionId);
                  tmpHeader.setTransactionNumber(TransactionCounter);

                  // send for user processing
                  r = procHeader(r);
                }

                if (r instanceof TrailerRecord)
                {
                  tmpTrailer = (TrailerRecord) r;
                  tmpTrailer.setStreamName(ORTransactionId);
                  ORTransactionId = "";
                  tmpTrailer.setTransactionNumber(TransactionCounter);

                  // Send the record for user processing
                  r = procTrailer(r);

                  // Notify the transaction layer that we have finished
                  setTransactionFlushed(TransactionCounter);

                  InputStreamOpen = false;
                }
              }
            }

            // put the record in the out batch
            Outbatch.add(r);
          }
        }
        else
        {
          // we are not allowed to start a new transaction at the moment
          return Outbatch;
        }
      }
      else
      {
        // we are already in a stream, continue
        in = linkedInputBuffer.pull(BatchSize);

        // Active loop
        iter = in.iterator();

        // Process each of the block of records and trigger the processing
        // functions for each type (header, trailer, valid and error)
        while (iter.hasNext())
        {
          // Get the formatted information from the record
          IRecord r = iter.next();

          // Trigger the correct user level functions according to the state of
          // the record
          if (r.isValid())
          {
            r = procValidRecord(r);
          }
          else
          {
            if (r.isErrored())
            {
              r = procErrorRecord(r);
            }
            else
            {
              if (r instanceof HeaderRecord)
              {
                InputStreamOpen = true;

                // This is the transaction identifier for all records in this stream
                ORTransactionId = ""+new Date().getTime();

                // Create a new transaction
                TransactionCounter = createNewTransaction();

                // Inform the transactional layer that we have started processing
                setTransactionProcessing(TransactionCounter);

                // Inject a stream header record into the stream
                tmpHeader = (HeaderRecord) r;
                tmpHeader.setStreamName(ORTransactionId);
                tmpHeader.setTransactionNumber(TransactionCounter);

                // send for user processing
                r = procHeader(r);
              }

              if (r instanceof TrailerRecord)
              {
                tmpTrailer = (TrailerRecord) r;
                tmpTrailer.setStreamName(ORTransactionId);
                ORTransactionId = "";
                tmpTrailer.setTransactionNumber(TransactionCounter);

                // Send the record for user processing
                r = procTrailer(r);

                // Notify the transaction layer that we have finished
                setTransactionFlushed(TransactionCounter);

                InputStreamOpen = false;
              }
            }
          }

          // put the record in the out batch
          Outbatch.add(r);
        }
      }
    }

    return Outbatch;
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction.
  *
  * @param TransactionNumber The number of the transaction we are starting
  * @return 0 if the transaction was closed OK, otherwise -1
  */
  @Override
  public int flushTransaction(int TransactionNumber)
  {
    return 0;
  }

 /**
  * Perform any processing that needs to be done when we are committing the
  * transaction.
  *
  * @param TransactionNumber The number of the transaction we are starting
  */
  @Override
  public void commitTransaction(int TransactionNumber)
  {
    // Nothing to do
  }

 /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction.
  *
  * @param TransactionNumber The number of the transaction we are starting
  */
  @Override
  public void rollbackTransaction(int TransactionNumber)
  {
    // Nothing to do
  }

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * @param transactionNumber The transaction we are working on
  */
  @Override
  public void closeTransaction(int transactionNumber)
  {
    // Nothing needed
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
  *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    if (ResultCode == 0)
    {
      PipeLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), pipeName, Command, Parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

 /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    //ClientManager.RegisterClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);
  }
}
