/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
package OpenRate.adapter.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.oro.io.GlobFilenameFilter;
import org.apache.oro.text.GlobCompiler;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.adapter.AbstractInputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;

/**
 * Generic Flat File InputAdapter (Non transactional Version) The basic function
 * of this flat file input adapter is to facilitate a reading of a flat file in
 * the batches, instead of reading a whole file in a single fetch.
 *
 * The file input adapter scans for files, and when found, opens them, reads
 * them and turns them into batches to maintain the load on the pipeline.
 *
 * Scanning and Processing -----------------------
 *
 * The basic scanning and processing loop looks like this: - The loadBatch()
 * method is called regularly by the execution model, regardless of if there is
 * work in progress or not. - If we are not processing a file, we are allowed to
 * scan for a new file to process - If we are allowed to look for a new file to
 * process, we do this: - getInputAvailable() Scan to see if there is any work
 * to do - assignInput() marks the file as being in processing if we find work
 * to do open the input stream - Calculate the file names from the base name -
 * Open the file reader - Inject the synthetic HeaderRecord into the stream as
 * the first record to synchronise the processing down the pipe
 *
 * - If we are processing a stream, we do: - Read the records in from the
 * stream, creating a basic "FlatRecord" for each record we have read - When we
 * have finished reading the batch (either because we have reached the batch
 * limit or because there are no more records to read) call procValidRecord(),
 * which allows the user to perform preparation of the record (for example,
 * creating the user defined record from the generic FlatRecord, or performing
 * record compression on the incoming stream) - See if the file reader has run
 * out of records. It it has, this is the end of the stream. If it is the end of
 * the stream, we do: - Inject a trailer record into the stream - close the
 * input stream and reset the "file in processing" flag so that we can scan for
 * more files
 */
public abstract class FlatFileNTInputAdapter
        extends AbstractInputAdapter
        implements IEventInterface {

  // The buffer size is the size of the buffer in the buffered reader
  private static final int BUF_SIZE = 65536;

  // This is the locally cached base name that we have recovered from the
  // file name
  private static String IntBaseName;

  private String InputFilePath = null;
  private String DoneFilePath = null;
  private String ErrFilePath = null;
  private String InputFilePrefix = null;
  private String DoneFilePrefix = null;
  private String ErrFilePrefix = null;
  private String InputFileSuffix = null;
  private String DoneFileSuffix = null;
  private String ErrFileSuffix = null;

  // This tells us if we should look for a file to open
  // or continue reading from the one we have
  private boolean InputStreamOpen = false;

  // used to track the status of the stream processing
  private int InputRecordNumber = 0;

  // Used for simulating the transaction manager statistics
  private long TransactionStart = 0;
  private long TransactionEnd = 0;

  /*
   * Reader is initialized in the init() method and is kept open for loadBatch()
   * calls and then closed in cleanup(). This facilitates batching of input.
   */
  private BufferedReader reader;

  // Used as the processing prefix
  private String ProcessingPrefix;

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
   * Default Constructor
   */
  public FlatFileNTInputAdapter() {
    super();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of inherited Input Adapter functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * Initialise the module. Called during pipeline creation. Initialise input
   * adapter. sets the filename to use & initialises the file reader.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
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
  }

  /**
   * loadBatch() is called regularly by the framework to either process records
   * or to scan for work to do, depending on whether we are already processing
   * or not.
   *
   * @return
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  protected Collection<IRecord> loadBatch()
          throws ProcessingException {
    String tmpFileRecord;
    String procName = null;
    String baseName;
    int NumberOfInputFiles;
    Collection<IRecord> Outbatch;
    int ThisBatchCounter = 0;

    // The Record types we will have to deal with
    HeaderRecord tmpHeader;
    TrailerRecord tmpTrailer;
    FlatRecord tmpDataRecord;
    IRecord batchRecord;
    Outbatch = new ArrayList<>();

    // This layer deals with opening the stream if we need to
    if (InputStreamOpen == false) {
      // There is a file available, so open it and rename it to
      // show that we are doing something
      NumberOfInputFiles = assignInput();

      if (NumberOfInputFiles > 0) {
        // Now that we have the file name, try to open it from
        // the renamed file provided by assignInput
        try {
          // Start time for the statistics
          TransactionStart = System.currentTimeMillis();

          // Get the name to work on
          baseName = GetBaseName();
          procName = getProcFilePath(baseName);
          reader = new BufferedReader(new FileReader(procName), BUF_SIZE);
          InputStreamOpen = true;
          InputRecordNumber = 0;

          // Inject a stream header record into the stream
          tmpHeader = new HeaderRecord();
          tmpHeader.setStreamName(baseName);

          // Increment the stream counter
          incrementStreamCount();

          // Pass the header to the user layer for any processing that
          // needs to be done
          tmpHeader = (HeaderRecord) procHeader((IRecord) tmpHeader);
          Outbatch.add(tmpHeader);
          ThisBatchCounter++;
        } catch (FileNotFoundException exFileNotFound) {
          getPipeLog().error(
                  "Application is not able to read file <" + procName + ">");
          throw new ProcessingException("Application is not able to read file <"
                  + procName + ">",
                  exFileNotFound,
                  getSymbolicName());
        }
      } else {
        // No work to do - return the empty batch
        return Outbatch;
      }
    }

    if (InputStreamOpen) {
      try {
        // read from the file and prepare the batch
        while ((reader.ready()) & (ThisBatchCounter < batchSize)) {
          ThisBatchCounter++;
          tmpFileRecord = reader.readLine();
          tmpDataRecord = new FlatRecord(tmpFileRecord, InputRecordNumber);
          InputRecordNumber++;

          // Call the user layer for any processing that needs to be done
          batchRecord = procValidRecord((IRecord) tmpDataRecord);

          // Add the prepared record to the batch
          Outbatch.add(batchRecord);
        }

        // see the reason that we closed
        if (reader.ready() == false) {
          // we have finished
          InputStreamOpen = false;

          // get any pending records that are in the input handler
          batchRecord = purgePendingRecord();

          // Add the prepared record to the batch, because of record compression
          // we may receive a null here. If we do, don't bother adding it
          if (batchRecord != null) {
            InputRecordNumber++;
            Outbatch.add(batchRecord);
          }

          //close the input file
          closeStream();
          shutdownStreamProcessOK();

          // Inject a stream header record into the stream
          tmpTrailer = new TrailerRecord();
          tmpTrailer.setStreamName(GetBaseName());

          // Pass the trailer to the user layer for any processing that
          // needs to be done
          tmpTrailer = (TrailerRecord) procTrailer((IRecord) tmpTrailer);
          Outbatch.add(tmpTrailer);
          ThisBatchCounter++;

          // print some statistics
          TransactionEnd = System.currentTimeMillis();
          OpenRate.getOpenRateStatsLog().info("Stream closed");
          OpenRate.getOpenRateStatsLog().info("Statistics: Records  <" + InputRecordNumber + ">");
          OpenRate.getOpenRateStatsLog().info(
                  "            Duration <"
                  + (TransactionEnd - TransactionStart) + "> ms");
          OpenRate.getOpenRateStatsLog().info(
                  "            Speed    <"
                  + ((InputRecordNumber * 1000) / (TransactionEnd - TransactionStart))
                  + "> records /sec");
        }
      } catch (IOException ex) {
        getPipeLog().fatal("Error reading input file");
      }
    }

    return Outbatch;
  }

  /**
   * Closes down the input stream after all the input has been collected
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream()
          throws ProcessingException {
    String procName;

    try {
      reader.close();
    } catch (IOException exFileNotFound) {
      procName = getProcFilePath(GetBaseName());
      getPipeLog().error("Application is not able to close file <" + procName + ">");
      throw new ProcessingException("Application is not able to read file <"
              + procName + ">",
              exFileNotFound,
              getSymbolicName());
    }
  }

  /**
   * Allows any records to be purged at the end of a file
   *
   * @return The pending record
   */
  @Override
  public IRecord purgePendingRecord() {
    // default - do nothing
    return null;
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
          String Parameter) {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_I_PATH)) {
      if (Init) {
        InputFilePath = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return InputFilePath;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_D_PATH)) {
      if (Init) {
        DoneFilePath = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return DoneFilePath;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_PATH)) {
      if (Init) {
        ErrFilePath = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return ErrFilePath;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_I_PREFIX)) {
      if (Init) {
        InputFilePrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return InputFilePrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_D_PREFIX)) {
      if (Init) {
        DoneFilePrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return DoneFilePrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_PREFIX)) {
      if (Init) {
        ErrFilePrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return ErrFilePrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_I_SUFFIX)) {
      if (Init) {
        InputFileSuffix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return InputFileSuffix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_D_SUFFIX)) {
      if (Init) {
        DoneFileSuffix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return DoneFileSuffix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_E_SUFFIX)) {
      if (Init) {
        ErrFileSuffix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return ErrFileSuffix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_PROCPREFIX)) {
      if (Init) {
        ProcessingPrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return ProcessingPrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (ResultCode == 0) {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    } else {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

  /**
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
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
  private String initGetInputFilePath()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_I_PATH);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetDoneFilePath()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_D_PATH);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFilePath()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_E_PATH);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetInputFilePrefix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_I_PREFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetDoneFilePrefix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_D_PREFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFilePrefix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_E_PREFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetInputFileSuffix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_I_SUFFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetDoneFileSuffix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_D_SUFFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFileSuffix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(getPipeName(), getSymbolicName(), SERVICE_E_SUFFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetProcPrefix()
          throws InitializationException {
    String tmpProcPrefix;
    tmpProcPrefix = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_PROCPREFIX,
            "tmp");

    return tmpProcPrefix;
  }

  /**
   * Checks the file name from the input parameters. Called by init() so that
   * derived classes can still reuse most of the functionality provided by this
   * adapter and selectively change only the logic to pickup file for
   * processing.
   *
   * The method checks for validity of the input parameters that have been
   * configured, for example if the directory does not exist, an exception will
   * be thrown.
   *
   * Two methods of finding the file are supported: 1) You can specify a file
   * name and only that file will be read 2) You can specify a file path and a
   * regex prefix and suffix
   */
  private void initFileName()
          throws InitializationException {
    File dir;

    /*
     * Validate the inputs we have received. We must end up with three
     * dustinct paths for input done and error files. We detect this by
     * checking the sum of the paramters.
     */
    // Set default values
    if (InputFilePath == null) {
      InputFilePath = ".";
      message = "Input file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(InputFilePath);
    if (!dir.isDirectory()) {
      message = "Input file path <" + InputFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    if (DoneFilePath == null) {
      DoneFilePath = ".";
      message = "Done file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(DoneFilePath);
    if (!dir.isDirectory()) {
      message = "Done file path <" + DoneFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    if (ErrFilePath == null) {
      ErrFilePath = ".";
      message = "Error file path not set. Defaulting to <.>.";
      getPipeLog().warning(message);
    }

    // is the input file path valid?
    dir = new File(ErrFilePath);
    if (!dir.isDirectory()) {
      message = "Error file path <" + ErrFilePath + "> does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((DoneFilePath + DoneFilePrefix + DoneFileSuffix).equals(ErrFilePath + ErrFilePrefix
            + ErrFileSuffix)) {
      // These look suspiciously similar
      message = "Done file and Error file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((InputFilePath + InputFilePrefix + InputFileSuffix).equals(ErrFilePath + ErrFilePrefix
            + ErrFileSuffix)) {
      // These look suspiciously similar
      message = "Input file and Error file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Check that there is some variance in what we have received
    if ((DoneFilePath + DoneFilePrefix + DoneFileSuffix).equals(InputFilePath + InputFilePrefix
            + InputFileSuffix)) {
      // These look suspiciously similar
      message = "Input file and Input file cannot be the same";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------------- Start stream handling functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * Selects input from the pending list for processing and marks it as being in
   * processing. Assign the FileReader object to the chosen file Rename the
   * input to the temp name
   *
   * @return The number of files assigned
   * @throws OpenRate.exception.ProcessingException
   */
  public int assignInput()
          throws ProcessingException {
    String procName;
    String[] FileNames;
    File dir;
    FilenameFilter filter;
    int FilesAssigned = 0;

    // This is the current filename we are working on
    String fileName;
    String baseName;

    // get the first file name from the directory that matches the
    dir = new File(InputFilePath);
    filter = new GlobFilenameFilter(InputFilePrefix + "*"
            + InputFileSuffix,
            GlobCompiler.STAR_CANNOT_MATCH_NULL_MASK);
    FileNames = dir.list(filter);

    // if we have a file, add it to the list of transaction files
    if (FileNames.length > 0) {
      // get the first file in the list
      fileName = FileNames[0];
      FilesAssigned = 1;
      baseName = fileName.replaceAll("^" + InputFilePrefix, "");
      baseName = baseName.replaceAll(InputFileSuffix + "$", "");
      getPipeLog().info("File base name is <" + baseName + ">");

      // Create the new transaction to hold the information. This is done in
      // The transactional layer - we just trigger it here
      SetBaseName(baseName);
      procName = getProcFilePath(baseName);
      fileName = getInputFilePath(baseName);

      // rename the input file to show that its our little piggy now
      File f = new File(fileName);
      f.renameTo(new File(procName));
    }

    return FilesAssigned;
  }

  /**
   * shutdownStreamProcessOK closes down the processing and renames the input
   * file to show that we have done with it. It then completes the transaction
   * from the point of view of the Transaction Manager. This represents the
   * successful completion of the transaction.
   */
  public void shutdownStreamProcessOK() {
    String procName;
    String doneName;
    String baseName;

    // get the file information for the transaction
    baseName = GetBaseName();

    // Calculate the part of the name that will be constant
    // during the processing
    doneName = InputFilePath + System.getProperty("file.separator")
            + DoneFilePrefix + baseName + DoneFileSuffix;
    procName = getProcFilePath(baseName);

    // rename the input file to show that it is no longer under the TMs control
    File f = new File(procName);
    f.renameTo(new File(doneName));
  }

  /**
   * shutdownStreamProcessERR closes down the processing and renames the input
   * file to show that we have done with it. It then completes the transaction
   * from the point of view of the Transaction Manager. This represents the
   * failed completion of the transaction, and should leave everything as it was
   * before the transaction started.
   */
  public void shutdownStreamProcessERR() {
    String procName;
    String origName;
    String baseName;

    // get the file information for the transaction
    baseName = GetBaseName();

    // Calculate the part of the name that will be constant
    // during the processing
    origName = InputFilePath + System.getProperty("file.separator")
            + InputFilePrefix + baseName + InputFileSuffix;
    procName = getProcFilePath(baseName);

    // rename the input file to show that it is no longer under the TMs control
    File f = new File(procName);
    f.renameTo(new File(origName));
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of utility functions -------------------------
  // -----------------------------------------------------------------------------
  /**
   * Calculate and return the processing file path for the given base name. This
   * is the name the file will have during the processing.
   */
  private String getProcFilePath(String baseName) {
    return InputFilePath + System.getProperty("file.separator")
            + ProcessingPrefix + InputFilePrefix + baseName + InputFileSuffix;
  }

  /**
   * Calculate and return the input file path for the given base name.
   */
  private String getInputFilePath(String baseName) {
    return InputFilePath + System.getProperty("file.separator")
            + InputFilePrefix + baseName + InputFileSuffix;
  }

  /**
   * Provides reader created during init()
   *
   * @return The buffered reader
   */
  public BufferedReader getFileReader() {
    return reader;
  }

  /**
   * Get the internal cache of the base name that we are using.
   */
  private String GetBaseName() {
    return IntBaseName;
  }

  /**
   * Set the internal cache of the base name that we are using. Note that we
   * will include this information in the Header *and* the trailer record, in
   * order to save the processing or output adapters having to store this state
   * information.
   */
  private void SetBaseName(String baseName) {
    IntBaseName = baseName;
  }

  /**
   * Provides a second level file name filter for files - may be overridden by
   * the implementation class
   *
   * @param fileNameToFilter The name of the file to filter
   * @return true if the file is to be processed, otherwise false
   */
  public boolean filterFileName(String fileNameToFilter) {
    // Filter out files that already have the processing prefix
    return (fileNameToFilter.startsWith(ProcessingPrefix) == false);
  }
}
