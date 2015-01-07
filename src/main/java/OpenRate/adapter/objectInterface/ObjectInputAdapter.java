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

package OpenRate.adapter.objectInterface;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.buffer.IBuffer;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Generic Object InputAdapter.
 *
 * The basic function of this input adapter is to read a collection of objects
 * that are presented to an input "listener" function - in reality it does not
 * listen, but instead accepts a series of records that presented as a
 * collection to the input adapter.
 *
 * Because this adapter uses the transactional parent class, the adapter is
 * transaction aware, and communicates with the parent transactional layer
 * communicates with the transaction manager to coordinate the file processing.
 *
 *   Processing
 *   ----------
 *
 * The basic processing loop looks like this:
 * - When a new batch of records is presented to the input adapter, then are
 *   read in the block size that is defined in the input adapter. For real-time
 *   work, it is normally better to choose smaller block sizes to reduce the
 *   latency through the processing pipe.
 * - Read the records in from the stream, creating a basic "FlatRecord" for
 *   each record we have read
 * - When we have finished reading the batch (either because we have reached
 *   the batch limit or because there are no more records to read) call the
 *   abstract transformInput(), which  is a user definable method in the
 *   implementation class which transforms the generic FlatRecord read from
 *   the file into a record for the processing
 * - See if there are any more records to process. If there are no more,
 *   this is the end of the stream. If it is the end of the stream, we do:
 *   - Inject a trailer record into the stream
 *   - Call the transaction layer to set the transaction status to "Flushed"
 *
 * The transaction closing is performed by the transaction layer, which is
 * informed by the transaction manager of changes in the overall status of the
 * transaction. When a change is made, the updateTransactionStatus() method
 * is called.
 *  - When all of the modules down the pipe have reported that they have FLUSHED
 *    the records, the flushTransaction() method is called, causing the input
 *    stream to be closed and the state of the transaction to be set to either
 *    FINISHED_OK or FINISHED_ERR
 *  - If the state went to FINISHED_OK, we commit the transaction, and rename
 *    the input file to have the final "done" name, else it is renamed to the
 *    "Err" name.
 *
 * The input adapter is also able to process more than one file at a time. This
 * is to allow the efficient operation of long pipelines, where a commit might
 * not arrive until a long time after the input adapter has finished processing
 * the input file. In this case, successive transactions can be opened before
 * the preceeding transaction is closed.
 */
public abstract class ObjectInputAdapter
  extends AbstractTransactionalInputAdapter
  implements IEventInterface
{
  /**
   * Used to track the status of the number of the record we are processing
   */
  protected int InputRecordNumber = 0;

  // This is our local FIFO buffer
  private IBuffer LocalBuffer;

  /**
   * Default Constructor
   */
  public ObjectInputAdapter()
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

    // Get a copy of our local consumer as the local buffer
    Class BufferClass = null;
    try
    {
      BufferClass = Class.forName(CommonConfig.DEFAULT_BUFFER_TYPE);
    }
    catch (ClassNotFoundException ex)
    {
      getPipeLog().error("Unable to find buffer class <" + CommonConfig.DEFAULT_BUFFER_TYPE + ">");
    }
    try
    {
      LocalBuffer = (IBuffer) BufferClass.newInstance();
    }
    catch (InstantiationException ex)
    {
      getPipeLog().error("Unable to instantiate buffer class <" + CommonConfig.DEFAULT_BUFFER_TYPE + ">");
    }
    catch (IllegalAccessException ex)
    {
      getPipeLog().error("Unable to access buffer class <" + CommonConfig.DEFAULT_BUFFER_TYPE + ">");
    }
  }

 /**
  * loadBatch() in this adapter simply gets the records that have been
  * prepared and pushes them into the pipeline. We process the records as they
  * are read, and put them into a local FIFO. This clears out the FIFO by
  * passing the results to the rest of the processing.
  *
  * @return A collection of the records which have been accepted
  * @throws ProcessingException
  */
  @Override
  protected Collection<IRecord> loadBatch()
                          throws ProcessingException
  {
    Collection<IRecord> Outbatch;

    // Create the empty batch.
    Outbatch = new ArrayList<>();

    // this is a temporary test harness to do away with the need to have a
    // program designed to source records. To be removed after initial testing
    if (Outbatch.isEmpty() )
    {
      triggerSomething();
    }

    // Get a batch of records
    Outbatch = LocalBuffer.pull(batchSize);

    // Pass the batch back
    return Outbatch;
  }

 /**
  * This function accepts records from the external iterator, and performs
  * pre-processing on them. The records that have been processed are placed in
  * a local FIFO, awaiting them to be taken away by the loadBatch() process.
  *
   * @param Events The stream of objects to be converted into records
   * @throws ProcessingException
  */
  protected void acceptRecords (Iterator <Object>Events) throws ProcessingException
  {
    String     baseName = null;
    int        ThisBatchCounter = 0;
    int        tmpTransNumber;

    // The Record types we will have to deal with
    HeaderRecord  tmpHeader;
    TrailerRecord tmpTrailer;
    IRecord    tmpDataRecord;
    IRecord       batchRecord;
    Object        tmpObject;

    Collection<IRecord> tmpBatch;

    // Create the empty batch.
    tmpBatch = new ArrayList<>();

    while (canStartNewTransaction() == false)
    {
      getPipeLog().info("Waiting to be able to start new transaction");
      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException ex)
      {
        // Ignore
      }
    }

    tmpTransNumber = createNewTransaction();

    // Inform the transactional layer that we have started processing
    setTransactionProcessing(tmpTransNumber);

    // Inject a stream header record into the stream
    tmpHeader = new HeaderRecord();
    tmpHeader.setStreamName(Integer.toString(tmpTransNumber));
    tmpHeader.setTransactionNumber(tmpTransNumber);

    // Pass the header to the user layer for any processing that
    // needs to be done
    tmpHeader = (HeaderRecord)procHeader((IRecord)tmpHeader);
    tmpBatch.add(tmpHeader);

    // contine with the iterator
    while ((Events.hasNext()) & (ThisBatchCounter < batchSize))
    {
      // Get the raw object
      tmpObject = Events.next();

      // Convert the object to a record - we don't need to know any more about the
      // type of record here
      tmpDataRecord = mapObjectToRecord(tmpObject);

      // Call the user layer for any processing that needs to be done on the record
      batchRecord = procValidRecord((IRecord) tmpDataRecord);

      // Add the prepared record to the batch, because of record compression
      // we may receive a null here. If we do, don't bother adding it
      if (batchRecord != null)
      {
        InputRecordNumber++;
        tmpBatch.add(batchRecord);
      }
    }

    // Update the statistics with the number of COMPRESSED final records
    updateRecordCount(tmpTransNumber,InputRecordNumber);

    // Inject a stream header record into the stream
    tmpTrailer = new TrailerRecord();
    tmpTrailer.setStreamName(baseName);
    tmpTrailer.setTransactionNumber(tmpTransNumber);

    // Pass the header to the user layer for any processing that
    // needs to be done. To allow for purging in the case of record
    // compression, we allow mutiple calls to procTrailer until the
    // trailer is returned
    batchRecord = procTrailer((IRecord)tmpTrailer);

    while (!(batchRecord instanceof TrailerRecord))
    {
      // the call the trailer returned a purged record. Add this
      // to the batch and refetch
      tmpBatch.add(batchRecord);
      batchRecord = procTrailer((IRecord)tmpTrailer);
    }

    tmpBatch.add(tmpTrailer);
    ThisBatchCounter++;

    // Push the collected records
    LocalBuffer.push(tmpBatch);

    // Notify the transaction layer that we have finished
    setTransactionFlushed(tmpTransNumber);
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

    // The input file name can be changed
    /*if (Command.equalsIgnoreCase(SERVICE_I_NAME))
    {
      if (Init)
      {
        // Set the file name and the input strategy
        InputFileName = Parameter;
        InputFileStrategy = 1;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          ResultCode = 0;

          return InputFileName;
        }
        else
        {
          InputFileName = Parameter;
          InputFileStrategy = 1;
          ResultCode = 0;
        }
      }
    } */

    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

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
    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------


  // -----------------------------------------------------------------------------
  // ---------------------- Start stream handling functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * This function allows the mapping of an object into a record
  *
  * @param tmpObject The object to map
  * @return The input object mapped as a record
  */
  public abstract IRecord mapObjectToRecord(Object tmpObject);

 /**
  * Used as a test harness for the moment
  */
  public abstract void triggerSomething();

}
