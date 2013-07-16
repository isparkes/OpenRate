/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

package OpenRate.adapter.file;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.oro.io.GlobFilenameFilter;
import org.apache.oro.text.GlobCompiler;

/**
 *
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Binary_File_Input_Adapter'>click here</a> to go to wiki page.
 * <br>
 * <p>Generic Binary File InputAdapter.<br>The basic function of this binary file
 * input adapter is to facilitate a reading of a binary file in a single go
 * after which a parser is called to create the individual records.
 *
 * The file input adapter scans for files, and when found, opens them, reads
 * them and turns them into batches to maintain the load on the pipeline.<br>
 *
 * Because this adapter uses the transactional parent class, the adapter is
 * transaction aware, and communicates with the parent transactional layer
 * communicates with the transaction manager to coordinate the file processing.
 *
 * <p>  Scanning and Processing<br>
 *   -----------------------
 *
 * <p>The basic scanning and processing loop looks like this:<br>
 * - The loadBatch() method is called regularly by the execution model,
 *   regardless of if there is work in progress or not.<br>
 * - If we are not processing a file, we are allowed to scan for a new file to
 *   process, BUT NOT if we are in the process of closing a previous transaction
 *   which we detect by seeing if a transaction is open at the moment<br>
 * - If we are allowed to look for a new file to process, we do this:<br>
 *   - getInputAvailable() Scan to see if there is any work to do<br>
 *   - assignInput() marks the file as being in processing if we find work to do
 *     open the input stream and creates a new transaction object in the
 *     Transaction Manager layer<br>
 *   - Calculate the file names from the base name<br>
 *   - Open the file reader<br>
 *   - Call the transaction layer to set the transaction status to "Processing"<br>
 *   - Inject the synthetic HeaderRecord into the stream as the first record
 *     to synchronise the processing down the pipe
 *
 * <p>- If we are processing a stream, we do:<br>
 *   - Read the records in from the stream, creating a basic "FlatRecord" for
 *     each record we have read<br>
 *   - When we have finished reading the batch (either because we have reached
 *     the batch limit or because there are no more records to read) call the
 *     abstract transformInput(), which  is a user definable method in the
 *     implementation class which transforms the generic FlatRecord read from
 *     the file into a record for the processing<br>
 *   - See if the file reader has run out of records. It it has, this is the
 *     end of the stream. If it is the end of the stream, we do:<br>
 *     - Inject a trailer record into the stream<br>
 *     - Call the transaction layer to set the transaction status to "Flushed"<br>
 *
 * <p>The transaction closing is performed by the transaction layer, which is
 * informed by the transaction manager of changes in the overall status of the
 * transaction. When a change is made, the updateTransactionStatus() method
 * is called.<br>
 *  - When all of the modules down the pipe have reported that they have FLUSHED
 *    the records, the flushTransaction() method is called, causing the input
 *    stream to be closed and the state of the transaction to be set to either
 *    FINISHED_OK or FINISHED_ERR<br>
 *  - If the state went to FINISHED_OK, we commit the transaction, and rename
 *    the input file to have the final "done" name, else it is renamed to the
 *    "Err" name.<br>
 *
 * <p>The input adapter is also able to process more than one file at a time. This
 * is to allow the efficient operation of long pipelines, where a commit might
 * not arrive until a long time after the input adapter has finished processing
 * the input file. In this case, successive transactions can be opened before
 * the preceding transaction is closed.
 */
public abstract class BinaryFileInputAdapter
  extends AbstractTransactionalInputAdapter
  implements IEventInterface
{
  /**
   * The path of the directory in which we are scanning for input files. This
   * can either be a relative or an absolute path.
   */
  protected String InputFilePath = null;

  /**
   * The path of the directory in which we will place done input files. This
   * can either be a relative or an absolute path.
   */
  protected String DoneFilePath = null;

  /**
   * The path of the directory in which we will place errored input files. This
   * can either be a relative or an absolute path.
   */
  protected String ErrFilePath = null;

  /**
   * The prefix for which we will be scanning for input files. The file must
   * have this prefix to be considered for processing.
   */
  protected String InputFilePrefix = null;

  /**
   * The prefix which we will place on done files. The input prefix will be
   * replaced with this value on the done file.
   */
  protected String DoneFilePrefix = null;

  /**
   * The prefix which we will place on errored files. The input prefix will be
   * replaced with this value on the error file.
   */
  protected String ErrFilePrefix = null;

  /**
   * The suffix for which we will be scanning for input files. The file must
   * have this suffix to be considered for processing.
   */
  protected String InputFileSuffix = null;

  /**
   * The suffix which we will place on done files. The input suffix will be
   * replaced with this value on the done file.
   */
  protected String DoneFileSuffix = null;

  /**
   * The suffix which we will place on errored files. The input suffix will be
   * replaced with this value on the error file.
   */
  protected String ErrFileSuffix = null;

 /**
  * This tells us if we should look for a file to open or continue reading from
  * the one we have
  */
  protected boolean InputStreamOpen = false;

 /**
  * used to track the status of the stream processing. This should normally
  * count the number of input records which have been processed.
  */
  protected int InputRecordNumber = 0;

 /**
  * Used as the processing prefix. This is prepended to the file name as soon
  * as the framework takes the file for processing. This is way of marking the
  * file as being "in processing" and the property of the framework.
  */
  protected String ProcessingPrefix;

  // This is used for queueing up files ready for processing
  private ArrayList<Integer> FileTransactionNumbers = new ArrayList<>();

  // This is the current transaction number we are working on
  private int      transactionNumber = 0;

  /*
   * Reader is initialized in the init() method and is kept open for loadBatch()
   * calls and then closed in cleanup(). This facilitates batching of input.
   */
  private RandomAccessFile reader;

  // Used to iterate through the results of the parse in batches of BatchSize
  private Iterator<IRecord> recordListIterator = null;

  // List of Services that this Client supports
  private static final String SERVICE_I_PATH = "InputFilePath";
  private static final String SERVICE_D_PATH = "DoneFilePath";
  private static final String SERVICE_E_PATH = "ErrFilePath";
  private static final String SERVICE_I_PREFIX = "InputFilePrefix";
  private static final String SERVICE_D_PREFIX = "DoneFilePrefix";
  private static final String SERVICE_E_PREFIX = "ErrFilePrefix";
  private static final String SERVICE_I_SUFFIX = "InputFileSuffix";
  private static final String SERVICE_D_SUFFIX = "DoneFileSuffix";
  private static final String SERVICE_E_SUFFIX = "ErrFileSuffix";
  private static final String SERVICE_PROCPREFIX = "ProcessingPrefix";

 /**
  * This method calls a parser to parse the binary input which has been read
  * out of the input file. The parse is responsible for splitting and preparing
  * the contents of the file for processing.
  *
  * @param fileContents The binary contents of the file we have read
  * @return A collection of records parsed out of the binary input
  */
  public abstract ArrayList<IRecord> parseBinaryFileContents(byte[] fileContents);

  // This is used to hold the calculated file names
  private class TransControlStructure
  {
    String ProcFileName;
    String DoneFileName;
    String ErrorFileName;
    String BaseName;
  }

  // This holds the file names for the files that are in processing at any
  // given moment
  private HashMap <Integer, TransControlStructure> CurrentFileNames;

  /**
   * Default Constructor
   */
  public BinaryFileInputAdapter()
  {
    super();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of inherited Input Adapter functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation.
  * Initialise input adapter.
  * sets the filename to use & initialises the file reader.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;

    // Register ourself with the client manager
    super.init(PipelineName, ModuleName);

    // Now we load the properties and use the event interface to initialise
    // the adapter. Note that this architecture will change to be completely
    // event driven in the near future.
    ConfigHelper = initGetInputFilePath();
    processControlEvent(SERVICE_I_PATH, true, ConfigHelper);
    ConfigHelper = initGetDoneFilePath();
    processControlEvent(SERVICE_D_PATH, true, ConfigHelper);
    ConfigHelper = initGetErrFilePath();
    processControlEvent(SERVICE_E_PATH, true, ConfigHelper);
    ConfigHelper = initGetInputFilePrefix();
    processControlEvent(SERVICE_I_PREFIX, true, ConfigHelper);
    ConfigHelper = initGetDoneFilePrefix();
    processControlEvent(SERVICE_D_PREFIX, true, ConfigHelper);
    ConfigHelper = initGetErrFilePrefix();
    processControlEvent(SERVICE_E_PREFIX, true, ConfigHelper);
    ConfigHelper = initGetInputFileSuffix();
    processControlEvent(SERVICE_I_SUFFIX, true, ConfigHelper);
    ConfigHelper = initGetDoneFileSuffix();
    processControlEvent(SERVICE_D_SUFFIX, true, ConfigHelper);
    ConfigHelper = initGetErrFileSuffix();
    processControlEvent(SERVICE_E_SUFFIX, true, ConfigHelper);
    ConfigHelper = initGetProcPrefix();
    processControlEvent(SERVICE_PROCPREFIX, true, ConfigHelper);

    // Check the file name scanning variables, throw initialisation exception
    // if something is wrong.
    initFileName();

    // create the structure for storing filenames
    CurrentFileNames = new HashMap <>(10);
  }

 /**
  * loadBatch() is called regularly by the framework to either process records
  * or to scan for work to do, depending on whether we are already processing
  * or not.
  *
  * The way this works is that we assign a batch of files to work on, and then
  * work our way through them. This minimises the directory scans that we have
  * to do and improves performance.
  *
  * In contrast to the flat file adapter, this adapter loads all of the file
  * into memory, parses it in one go (using an implementation level parser) and
  * then pumps the records into the pipeline. Thus, a single call to load batch
  * will load the whole file.
  */
  @Override
  protected Collection<IRecord> loadBatch()
                          throws ProcessingException
  {
    String              baseName = null;
    Collection<IRecord> Outbatch;
    int                 ThisBatchCounter = 0;

    // The Record types we will have to deal with
    HeaderRecord      tmpHeader;
    TrailerRecord     tmpTrailer;
    IRecord           tmpDataRecord;
    IRecord           batchRecord;
    byte[]            bytes;

    Outbatch = new ArrayList<>();

    // Check to see if there is any work to do, and if the transaction
    // manager can accept the new work (if it can't, no files will be assigned
    ArrayList<Integer> fileNames = assignInput();
    if (fileNames.size() > 0)
    {
      // There is a file available, so open it and rename it to
      // show that we are doing something
      FileTransactionNumbers.addAll(fileNames);
    }

    // This layer deals with opening the stream if we need to
    if (FileTransactionNumbers.size() > 0)
    {
      // we have something to do
      // See if we are trying to finish off a file that is already in process
      if (InputStreamOpen == false)
      {
        // Open the stream
        // we don't have anything open, so get something from the head of the
        // waiting list
        transactionNumber = FileTransactionNumbers.get(0);

        // Now that we have the file name, try to open it from
        // the renamed file provided by assignInput
        try
        {
          reader = new RandomAccessFile(getProcName(transactionNumber),"r");
          InputStreamOpen = true;
          InputRecordNumber = 0;

          // Inform the transactional layer that we have started processing
          setTransactionProcessing(transactionNumber);

          // Inject a stream header record into the stream
          tmpHeader = new HeaderRecord();
          tmpHeader.setStreamName(getBaseName(transactionNumber));
          tmpHeader.setTransactionNumber(transactionNumber);

          // Increment the stream counter
          incrementStreamCount();

          // Pass the header to the user layer for any processing that
          // needs to be done
          tmpHeader = (HeaderRecord)procHeader((IRecord)tmpHeader);
          Outbatch.add(tmpHeader);

          // now load the file into a memory buffer - it's difficult to know
          // where to split up binary files, so we don't attempt to, and let the
          // parser work this out
          int fileLength = (int)reader.length();
          bytes = new byte[fileLength];
          reader.readFully(bytes);

          // call the parser to process the binary contents
          Collection<IRecord> recordList = parseBinaryFileContents(bytes);

          // Prepare the iterator for loading the records
          recordListIterator = recordList.iterator();
        }
        catch (FileNotFoundException exFileNotFound)
        {
          getPipeLog().error(
                "Application is not able to read file <" + getProcName(transactionNumber) + ">");
          throw new ProcessingException("Application is not able to read file <" +
                                        getProcName(transactionNumber) + ">", 
                                        exFileNotFound, 
                                        getSymbolicName());
        }
        catch (IOException ioex)
        {
          getPipeLog().error(
                "Application is not able to read file : '" + getProcName(transactionNumber) +
                "' ");
          throw new ProcessingException("Application is not able to read file: <" +
                                        getProcName(transactionNumber) + "> ",
                                        ioex,
                                        getSymbolicName());
        }
      }

      // read from the file and prepare the batch
      while ((recordListIterator.hasNext()) & (ThisBatchCounter < batchSize))
      {
        tmpDataRecord = recordListIterator.next();

        // skip blank records
        if (tmpDataRecord == null)
        {
          continue;
        }

        // Call the user layer for any processing that needs to be done
        batchRecord = procValidRecord(tmpDataRecord);
        ThisBatchCounter++;

        // Add the prepared record to the batch, because of record compression
        // we may receive a null here. If we do, don't bother adding it
        if (batchRecord != null)
        {
          InputRecordNumber++;
          Outbatch.add(batchRecord);
        }
      }

      // see if we have to abort
      if (transactionAbortRequest(transactionNumber))
      {
        // if so, clear down everything that is not a header or a trailer
        // so we don't keep filling the pipe
        int originalCount = Outbatch.size();
        int discardCount = 0;
        Iterator<IRecord> discardIterator = Outbatch.iterator();

        while (discardIterator.hasNext())
        {
          IRecord tmpRecord = discardIterator.next();
          if (((tmpRecord instanceof HeaderRecord) | (tmpRecord instanceof TrailerRecord)) == false)
          {
            discardIterator.remove();
            discardCount++;
          }
        }

        // if so, clear down the outbatch, so we don't keep filling the pipe
        getPipeLog().warning("Pipe <"+ getSymbolicName() + "> discarded <" + discardCount + "> of <" + originalCount + "> input records, because of pending abort.");
      }

      // Update the statistics with the number of COMPRESSED final records
      updateRecordCount(transactionNumber,InputRecordNumber);

      // see the reason that we closed
      if (recordListIterator.hasNext() == false)
      {
        // we have finished
        InputStreamOpen = false;

        // get any pending records that are in the input handler
        batchRecord = purgePendingRecord();

        // Add the prepared record to the batch, because of record compression
        // we may receive a null here. If we do, don't bother adding it
        if (batchRecord != null)
        {
          InputRecordNumber++;
          Outbatch.add(batchRecord);
        }

        // Inject a stream trailer record into the stream
        tmpTrailer = new TrailerRecord();
        tmpTrailer.setStreamName(baseName);
        tmpTrailer.setTransactionNumber(transactionNumber);

        // Pass the header to the user layer for any processing that
        // needs to be done. To allow for purging in the case of record
        // compression, we allow multiple calls to procTrailer until the
        // trailer is returned
        batchRecord = procTrailer((IRecord)tmpTrailer);

        while (!(batchRecord instanceof TrailerRecord))
        {
          // the call the trailer returned a purged record. Add this
          // to the batch and refetch
          Outbatch.add(batchRecord);
          batchRecord = procTrailer((IRecord)tmpTrailer);
        }

        Outbatch.add(tmpTrailer);
        ThisBatchCounter++;

        // Close the reader
        try
        {
          // close the input stream
          closeStream(transactionNumber);
        }
        catch (ProcessingException ex)
        {
          getPipeLog().error("Error flushing transaction in module <" + getSymbolicName() + ">. Message <" + ex.getMessage() + ">");
        }

        // Notify the transaction layer that we have finished
        setTransactionFlushed(transactionNumber);

        // Remove the transaction from the list
        FileTransactionNumbers.remove(0);
        transactionNumber = 0;

        // Clean up the iterator and the byte array
        recordListIterator = null;
      }
    }

    return Outbatch;
  }

  /**
   * Closes down the input stream after all the input has been collected
   *
   * @param TransactionNumber The transaction number of the transaction to close
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream(int TransactionNumber)
    throws ProcessingException
  {
    try
    {
      reader.close();
    }
    catch (IOException exFileNotFound)
    {
      getPipeLog().error("Application is not able to close file <" + getProcName(TransactionNumber) + ">");
      throw new ProcessingException("Application is not able to read file <" +
                                    getProcName(TransactionNumber) + "> ", 
                                    exFileNotFound,
                                    getSymbolicName());
    }
  }

  /**
   * Provides reader created during init()
   *
   * @return The buffered Reader to use
   */
  public RandomAccessFile getFileReader()
  {
    return reader;
  }

 /**
  * Allows any records to be purged at the end of a file
  *
  * @return The pending record
  */
  @Override
  public IRecord purgePendingRecord()
  {
    // default - do nothing
    return null;
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

  /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction;
   *
   * @return 0 if the transaction was closed OK, otherwise -1
   */
  @Override
  public int flushTransaction(int transactionNumber)
  {
    return 0;
  }

  /**
  * Perform any processing that needs to be done when we are committing the
  * transaction;
  */
  @Override
  public void commitTransaction(int transactionNumber)
  {
    shutdownStreamProcessOK(transactionNumber);
  }

  /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction;
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    shutdownStreamProcessERR(transactionNumber);
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
    // nothing needed
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

    if (Command.equalsIgnoreCase(SERVICE_I_PATH))
    {
      if (Init)
      {
        InputFilePath = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return InputFilePath;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equals(SERVICE_D_PATH))
    {
      if (Init)
      {
        DoneFilePath = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return DoneFilePath;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_PATH))
    {
      if (Init)
      {
        ErrFilePath = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return ErrFilePath;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_I_PREFIX))
    {
      if (Init)
      {
        InputFilePrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return InputFilePrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_D_PREFIX))
    {
      if (Init)
      {
        DoneFilePrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return DoneFilePrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_PREFIX))
    {
      if (Init)
      {
        ErrFilePrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return ErrFilePrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_I_SUFFIX))
    {
      if (Init)
      {
        InputFileSuffix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return InputFileSuffix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_D_SUFFIX))
    {
      if (Init)
      {
        DoneFileSuffix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return DoneFileSuffix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_SUFFIX))
    {
      if (Init)
      {
        ErrFileSuffix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return ErrFileSuffix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_PROCPREFIX))
    {
      if (Init)
      {
        ProcessingPrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return ProcessingPrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_I_PATH, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_D_PATH, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_E_PATH, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_I_PREFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_D_PREFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_E_PREFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_I_SUFFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_D_SUFFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_E_SUFFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputFilePath() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_I_PATH);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetDoneFilePath() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_D_PATH);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetErrFilePath() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_E_PATH);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputFilePrefix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_I_PREFIX);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetDoneFilePrefix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_D_PREFIX);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetErrFilePrefix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_E_PREFIX);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetInputFileSuffix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_I_SUFFIX);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetDoneFileSuffix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_D_SUFFIX);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetErrFileSuffix() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(),getSymbolicName(),SERVICE_E_SUFFIX);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetProcPrefix() throws InitializationException
  {
    String tmpProcPrefix;
    tmpProcPrefix = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                                  SERVICE_PROCPREFIX,
                                                                  "tmp");

    return tmpProcPrefix;
  }

 /**
   * Checks the file name from the input parameters.
   * Refactored from init() into a method of its own so that derived classes
   * can still reuse most of the functionality provided by this adapter and
   * selectively change only the logic to pickup file for processing.
   *
   * The method checks for validity of the input parameters that have been
   * configured, for example if the directory does not exist, an exception will
   * be thrown.
   *
   * Two methods of finding the file are supported:
   * 1) You can specify a file name and only that file will be read
   * 2) You can specify a file path and a regex prefix and suffix
   */
  private void initFileName() throws InitializationException
  {
    String  message;
    File    dir;

    /*
     * Validate the inputs we have received. We must end up with three
     * distinct paths for input done and error files. We detect this by
     * checking the sum of the parameters.
     */

    // Set default values
    if (InputFilePath == null)
    {
      InputFilePath = ".";
      message = "Input file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(InputFilePath);
    if (!dir.isDirectory())
    {
      message = "Input file path <" + InputFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    if (DoneFilePath == null)
    {
      DoneFilePath = ".";
      message = "Done file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(DoneFilePath);
    if (!dir.isDirectory())
    {
      message = "Done file path <" + DoneFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    if (ErrFilePath == null)
    {
      ErrFilePath = ".";
      message = "Error file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(ErrFilePath);
    if (!dir.isDirectory())
    {
      message = "Error file path <" + ErrFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((DoneFilePath + DoneFilePrefix + DoneFileSuffix).equals(ErrFilePath + ErrFilePrefix +
          ErrFileSuffix))
    {
      // These look suspiciously similar
      message = "Done file and Error file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((InputFilePath + InputFilePrefix + InputFileSuffix).equals(ErrFilePath + ErrFilePrefix +
          ErrFileSuffix))
    {
      // These look suspiciously similar
      message = "Input file and Error file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((DoneFilePath + DoneFilePrefix + DoneFileSuffix).equals(InputFilePath + InputFilePrefix +
          InputFileSuffix))
    {
      // These look suspiciously similar
      message = "Input file and Input file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------------- Start stream handling functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Selects input from the pending list for processing and marks it as
  * being in processing. Creates the transaction object that we will be using
  * and calculates the file names that will be used.
  *
  * This method never assigns more input files than the transaction manager
  * can handle.
  *
  * @return The number of files assigned
  * @throws OpenRate.exception.ProcessingException
  */
  public ArrayList<Integer> assignInput()
    throws ProcessingException
  {
    String         procName;
    String         doneName;
    String         errName;
    String         inpName;
    String         baseName;

    String[]       fileNames;
    File           dir;
    FilenameFilter filter;
    int            tmpTransNumber;
    int            filesOpened;
    TransControlStructure tmpFileNames;
    ArrayList<Integer> openedTransactions = new ArrayList<>();

    // This is the current filename we are working on
    String fileName;

    // get the first file name from the directory that matches the
    dir = new File(InputFilePath);
    filter = new GlobFilenameFilter(InputFilePrefix + "*" +
                                    InputFileSuffix,
                                    GlobCompiler.STAR_CANNOT_MATCH_NULL_MASK);
    fileNames = dir.list(filter);

    // if we have a file, add it to the list of transaction files
    if (fileNames.length > 0)
    {
      // Open up the maximum number of files that we can
      for (filesOpened = 0; filesOpened < fileNames.length ; filesOpened++)
      {
        fileName = fileNames[filesOpened];

        // See if we want to open this file
        if (filterFileName(fileName))
        {
          // We want to open it, will the transaction manager allow it?
          if (canStartNewTransaction())
          {
            // Create the new transaction to hold the information. This is done in
            // The transactional layer - we just trigger it here
            tmpTransNumber = createNewTransaction();

            getPipeLog().info("Input File name is <" + fileName + ">");

            // Calculate the processing file name that we are using for this file
            procName = getProcFilePath(fileName,
                                       InputFilePath,
                                       InputFilePrefix,
                                       InputFileSuffix,
                                       ProcessingPrefix,
                                       tmpTransNumber);

            doneName = getDoneFilePath(fileName,
                                       InputFilePrefix,
                                       InputFileSuffix,
                                       DoneFilePath,
                                       DoneFilePrefix,
                                       DoneFileSuffix,
                                       tmpTransNumber);

            errName =  getErrorFilePath(fileName,
                                        InputFilePrefix,
                                        InputFileSuffix,
                                        ErrFilePath,
                                        ErrFilePrefix,
                                        ErrFileSuffix,
                                        tmpTransNumber);

            inpName =  getInputFilePath(fileName,
                                        InputFilePath);

            baseName = getFileBaseName(fileName,
                                       InputFilePrefix,
                                       InputFileSuffix,
                                       tmpTransNumber);

            tmpFileNames = new TransControlStructure();
            tmpFileNames.ProcFileName  = procName;
            tmpFileNames.DoneFileName  = doneName;
            tmpFileNames.ErrorFileName = errName;
            tmpFileNames.BaseName      = baseName;

            // rename the input file to show that its our little piggy now
            File f = new File(inpName);
            if(f.renameTo(new File(procName)))
            {
              // Store the names for later
              CurrentFileNames.put(tmpTransNumber, tmpFileNames);

              // Add the transaction to the list of the transactions that we
              // have opened this time around
              openedTransactions.add(tmpTransNumber);

              // Set the scheduler - we have found some files to process
              getPipeline().setSchedulerHigh();
            }
            else
            {
              getPipeLog().warning("Could not rename file <" + inpName + ">");

              cancelTransaction(tmpTransNumber);
            }
          }
        }
        else
        {
          // filled up the possibilities finish for the moment
          break;
        }
      }
    }

    return openedTransactions;
  }

  /**
   * shutdownStreamProcessOK closes down the processing and renames the input file
   * to show that we have done with it. It then completes the transaction
   * from the point of view of the Transaction Manager. This represents the
   * successful completion of the transaction.
   *
   * @param TransactionNumber The transaction number of the transaction to close
   */
  public void shutdownStreamProcessOK(int TransactionNumber)
  {
    // rename the input file to show that it is no longer under the TMs control
    File f = new File(getProcName(TransactionNumber));
    f.renameTo(new File(getDoneName(TransactionNumber)));
  }

  /**
   * shutdownStreamProcessERR closes down the processing and renames the input file
   * to show that we have done with it. It then completes the transaction
   * from the point of view of the Transaction Manager. This represents the
   * failed completion of the transaction, and should leave everything as it
   * was before the transaction started.
   *
   * @param TransactionNumber The transaction number of the transaction to close
   */
  public void shutdownStreamProcessERR(int TransactionNumber)
  {
    // rename the input file to show that it is no longer under the TMs control
    File f = new File(getProcName(TransactionNumber));
    f.renameTo(new File(getErrName(TransactionNumber)));
  }

 /**
  * Calculate and return the processing file path for the given base name. This
  * is the name the file will have during the processing.
  *
  * @param fileName The base file name of the file to work on
  * @param InputFilePath The path of the input file
  * @param InputFilePrefix The file prefix of the input file
  * @param InputFileSuffix The file suffix of the input file
  * @param ProcessingPrefix the file processing prefix to use
  * @param tmpTransNumber The transaction number
  * @return The full file path of the file in processing
  */
  protected String getProcFilePath(String fileName,
                                   String InputFilePath,
                                   String InputFilePrefix,
                                   String InputFileSuffix,
                                   String ProcessingPrefix,
                                   int    tmpTransNumber)
  {
    return InputFilePath + System.getProperty("file.separator") +
           ProcessingPrefix + fileName;
  }

 /**
  * Calculate and return the done file path for the given base name. This
  * is the name the file will have during the processing.
  *
  * @param fileName The base file name of the file to work on
  * @param InputFilePrefix The file prefix of the input file
  * @param DoneFilePath The path of the done file
  * @param DoneFilePrefix The prefix of the done file
  * @param DoneFileSuffix The suffix of the done file
  * @param InputFileSuffix The file suffix of the input file
  * @param tmpTransNumber The transaction number
  * @return The full file path of the file in processing
  */
  protected String getDoneFilePath(String fileName,
                                   String InputFilePrefix,
                                   String InputFileSuffix,
                                   String DoneFilePath,
                                   String DoneFilePrefix,
                                   String DoneFileSuffix,
                                   int    tmpTransNumber)
  {
    String baseName;

    baseName = fileName.replaceAll("^" + InputFilePrefix, "");
    baseName = baseName.replaceAll(InputFileSuffix + "$", "");

    return DoneFilePath + System.getProperty("file.separator") +
           DoneFilePrefix + baseName + DoneFileSuffix;
  }

 /**
  * Calculate and return the error file path for the given base name. This
  * is the name the file will have during the processing.
  *
  * @param fileName The base file name of the file to work on
  * @param InputFilePrefix The file prefix of the input file
  * @param ErrFilePath The file path fo the error file
  * @param ErrFilePrefix The prefix of the error file
  * @param ErrFileSuffix The suffix of the error file
  * @param InputFileSuffix The file suffix of the input file
  * @param tmpTransNumber The transaction number
  * @return The full file path of the file in processing
  */
  protected String getErrorFilePath(String fileName,
                                    String InputFilePrefix,
                                    String InputFileSuffix,
                                    String ErrFilePath,
                                    String ErrFilePrefix,
                                    String ErrFileSuffix,
                                    int    tmpTransNumber)
  {
    String baseName;

    baseName = fileName.replaceAll("^" + InputFilePrefix, "");
    baseName = baseName.replaceAll(InputFileSuffix + "$", "");

    return ErrFilePath + System.getProperty("file.separator") +
           ErrFilePrefix + baseName + ErrFileSuffix;
  }

 /**
  * Calculate and return the base file path for the given base name. This
  * is the name the file will have during the processing.
  *
  * @param fileName The file name to use
  * @param InputFilePrefix The input file prefix
  * @param InputFileSuffix The input file suffix
  * @param TransactionNumber The transaction number
  * @return The base name for the transaction
  */
  protected String getFileBaseName(String fileName,
                                    String InputFilePrefix,
                                    String InputFileSuffix,
                                    int    TransactionNumber)
  {
    String baseName;

    baseName = fileName.replaceAll("^" + InputFilePrefix, "");
    baseName = baseName.replaceAll(InputFileSuffix + "$", "");

    return baseName;
  }

 /**
  * Calculate and return the input file path for the given base name.
  *
  * @param fileName The file name to use
  * @param InputFilePath The file path to use
  * @return The full file path of the file in input
  */
  protected String getInputFilePath(String fileName,
                                    String InputFilePath)
  {
    return InputFilePath + System.getProperty("file.separator") + fileName;
  }

 /**
  * Get the proc file name for the given transaction
  *
  * @param TransactionNumber The transaction number to get the proc name for
  * @return The temporary processing file name associated with the transaction
  */
  protected String getProcName(int TransactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = CurrentFileNames.get(TransactionNumber);

    return tmpFileNames.ProcFileName;
  }

 /**
  * Get the done file name for the given transaction
  *
  * @param TransactionNumber The transaction number to get the proc name for
  * @return The done name associated with the transaction
  */
  protected String getDoneName(int TransactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = CurrentFileNames.get(TransactionNumber);

    return tmpFileNames.DoneFileName;
  }

 /**
  * Get the error file name for the given transaction
  *
  * @param TransactionNumber The transaction number to get the proc name for
  * @return The error file name associated with the transaction
  */
  protected String getErrName(int TransactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = CurrentFileNames.get(TransactionNumber);

    return tmpFileNames.ErrorFileName;
  }

 /**
  * Get the base name for the given transaction
  *
  * @param TransactionNumber The transaction number to get the proc name for
  * @return The base file name associated with the transaction
  */
  protected String getBaseName(int TransactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = CurrentFileNames.get(TransactionNumber);

    return tmpFileNames.BaseName;
  }

  /**
   * Provides a second level file name filter for files - may be overridden
   * by the implementation class
   *
   * @param fileNameToFilter The name of the file to filter
   * @return true if the file is to be processed, otherwise false
   */
  public boolean filterFileName(String fileNameToFilter)
  {
    // Filter out files that already have the processing prefix
    return (fileNameToFilter.startsWith(ProcessingPrefix) == false);
  }
}
