package OpenRate.adapter.file;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Multi-stream Flat File Output Adapter. Writes to file streams output, using
 * transaction aware handling. Specialised to write multiple files
 * simultaneously, therefore good for splitting and routing functions.
 *
 * The number of parallel streams that this adapter can write is determined by
 * the implementation layer.
 */
public abstract class FlatFileMultiStreamOutputAdapter
        extends AbstractTransactionalOutputAdapter
        implements IEventInterface {

  // The buffer size is the size of the buffer in the buffered reader

  private static final int BUF_SIZE = 65536;
  private BufferedWriter errorWriter;
  private String filePath;
  private String filePrefix;
  private String fileSuffix;
  private String errPath;
  private String errPrefix;
  private String errSuffix;
  private boolean DelEmptyOutFile = false;
  private boolean DelEmptyErrFile = true;

  // This is the prefix that will be added during processing
  private String ProcessingPrefix;

  // This tells us if we should look for a file to open
  // or continue reading from the one we have
  private boolean OutputStreamOpen = false;

  // This is the base name of the file we are outputting
  private String fileBaseName = null;

  // List of Services that this Client supports
  private final static String SERVICE_FILE_PATH = "OutputFilePath";
  private final static String SERVICE_FILE_PREFIX = "OutputFilePrefix";
  private final static String SERVICE_FILE_SUFFIX = "OutputFileSuffix";
  private final static String SERVICE_DEL_EMPTY_OUT_FILE = "DeleteEmptyOutputFile";
  private final static String SERVICE_ERR_PATH = "ErrFilePath";
  private final static String SERVICE_ERR_PREFIX = "ErrFilePrefix";
  private final static String SERVICE_ERR_SUFFIX = "ErrFileSuffix";
  private final static String SERVICE_DEL_EMPTY_ERR_FILE = "DeleteEmptyErrorFile";
  private static final String SERVICE_PROCPREFIX = "ProcessingPrefix";

  //final static String SERVICE_OUT_FILE_NAME = "OutputFileName";
  //final static String SERVICE_ERR_FILE_NAME = "ErrFileName";
  // This is used to hold the calculated file names
  private class TransControlStructure {

    HashMap<Integer, String> OutputFileName;
    String ErrorFileName;
    HashMap<Integer, String> ProcOutputFileName;
    HashMap<Integer, BufferedWriter> ValidWriter;
    String ProcErrorFileName;
  }

  // This holds the file names for the files that are in processing at any
  // given moment
  private HashMap<Integer, TransControlStructure> currentFileNames;

  /**
   * Default Constructor.
   */
  public FlatFileMultiStreamOutputAdapter() {
    super();

    this.errorWriter = null;
  }

  /**
   * Get the error writer
   *
   * @return The error writer
   */
  public BufferedWriter getErrorWriter() {
    return errorWriter;
  }

  /**
   * Initialise the output adapter with the configuration that is to be used for
   * this instance of the adapter.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    String ConfigHelper;

    super.init(PipelineName, ModuleName);

    ConfigHelper = initGetFilePath();
    processControlEvent(SERVICE_FILE_PATH, true, ConfigHelper);
    ConfigHelper = initGetOutFilePrefix();
    processControlEvent(SERVICE_FILE_PREFIX, true, ConfigHelper);
    ConfigHelper = initGetOutFileSuffix();
    processControlEvent(SERVICE_FILE_SUFFIX, true, ConfigHelper);
    ConfigHelper = initGetDelEmptyOutFile();
    processControlEvent(SERVICE_DEL_EMPTY_OUT_FILE, true, ConfigHelper);
    ConfigHelper = initGetErrFilePath();
    processControlEvent(SERVICE_ERR_PATH, true, ConfigHelper);
    ConfigHelper = initGetErrFilePrefix();
    processControlEvent(SERVICE_ERR_PREFIX, true, ConfigHelper);
    ConfigHelper = initGetErrFileSuffix();
    processControlEvent(SERVICE_ERR_SUFFIX, true, ConfigHelper);
    ConfigHelper = initGetDelEmptyErrFile();
    processControlEvent(SERVICE_DEL_EMPTY_ERR_FILE, true, ConfigHelper);
    ConfigHelper = initGetProcPrefix();
    processControlEvent(SERVICE_PROCPREFIX, true, ConfigHelper);

    // Check the parameters we received
    initFileName();

    // create the structure for storing filenames
    currentFileNames = new HashMap<>(10);
  }

  /**
   * Process the stream header. Get the file base name and open the transaction.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException {
    HeaderRecord tmpHeader;
    int tmpTransNumber;

    // initialise the transaction record
    TransControlStructure tmpFileNames = new TransControlStructure();
    tmpFileNames.OutputFileName = new HashMap<>();
    tmpFileNames.ProcOutputFileName = new HashMap<>();
    tmpFileNames.ValidWriter = new HashMap<>();

    // call the transactional layer
    super.procHeader(r);

    // if we are not currently streaming, open the stream using the transaction
    // information for the transaction we are processing
    if (!OutputStreamOpen) {
      fileBaseName = r.getStreamName();
      tmpTransNumber = r.getTransactionNumber();

      // Calculate the names and open the error writer (valid writers are created
      // on demand
      tmpFileNames.ProcErrorFileName = errPath + System.getProperty("file.separator")
              + ProcessingPrefix + errPrefix + fileBaseName + errSuffix;
      tmpFileNames.ErrorFileName = errPath + System.getProperty("file.separator")
              + errPrefix + fileBaseName + errSuffix;

      // Store the names for later
      currentFileNames.put(tmpTransNumber, tmpFileNames);

      openErrFile(tmpFileNames.ProcErrorFileName);
      OutputStreamOpen = true;
    }

    return r;
  }

  /**
   * Write good records to the defined output stream. This method performs
   * record expansion (the opposite of record compression) and then calls the
   * write for each of the records that results.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException {
    Collection<FlatRecord> outRecCol;
    FlatRecord outRec;
    Iterator<FlatRecord> outRecIter;
    int stream;
    BufferedWriter tmpOutStream;
    String tmpProcOutputFileName;
    String tmpOutputFileName;
    TransControlStructure tcs;

    outRecCol = procValidRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      // get the stream for this record
      stream = getValidRecordStreamNumber(r);

      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext()) {
        outRec = outRecIter.next();

        // see if the stream writer exists
        tcs = currentFileNames.get(getTransactionNumber());
        if (tcs.OutputFileName.containsKey(stream)) {
          tmpOutStream = tcs.ValidWriter.get(stream);
        } else {
          // get the file name for the new stream
          tmpProcOutputFileName = filePath + System.getProperty("file.separator")
                  + ProcessingPrefix + filePrefix + fileBaseName
                  + "_" + String.valueOf(stream) + fileSuffix;
          tmpOutputFileName = filePath + System.getProperty("file.separator")
                  + filePrefix + fileBaseName + "_"
                  + String.valueOf(stream) + fileSuffix;
          tcs.ProcOutputFileName.put(stream, tmpProcOutputFileName);
          tcs.OutputFileName.put(stream, tmpOutputFileName);

          //create the new out stream
          tmpOutStream = openValidFile(tcs.OutputFileName.get(stream));

          // save the stream for later
          tcs.ValidWriter.put(stream, tmpOutStream);
        }

        try {
          tmpOutStream.write(outRec.getData());
          tmpOutStream.newLine();
        } catch (IOException ioe) {
          this.getExceptionHandler().reportException(new ProcessingException(ioe, getSymbolicName()));
        }
      }
    }

    return r;
  }

  /**
   * Write bad records to the defined output stream.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException {
    Collection<FlatRecord> outRecCol;
    FlatRecord outRec;
    Iterator<FlatRecord> outRecIter;

    outRecCol = procErrorRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext()) {
        outRec = outRecIter.next();

        try {
          errorWriter.write(outRec.getData());
          errorWriter.newLine();
        } catch (IOException ioe) {
          this.getExceptionHandler().reportException(new ProcessingException(ioe, getSymbolicName()));
        }
      }
    }

    return r;
  }

  /**
   * Process the stream trailer. Get the file base name and open the
   * transaction.
   *
   * @param r The record we are working on
   * @return
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    // Close the files
    closeFiles(getTransactionNumber());

    // Do the transaction level maintenance
    super.procTrailer(r);

    return r;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here. Note that the result is a collection for the case that we
   * have to re-expand after a record compression input adapter has done
   * compression on the input stream.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<FlatRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<FlatRecord> procErrorRecord(IRecord r) throws ProcessingException;

  /**
   * Open the output file for writing.
   *
   * @param fileName The name of the file to open
   * @return The buffered file writer for the valid file
   */
  public BufferedWriter openValidFile(String fileName) {
    FileWriter fwriter = null;
    File file;
    file = new File(fileName);

    try {
      if (file.createNewFile() == false) {
        getPipeLog().error("output file already exists = " + fileName);
      }

      fwriter = new FileWriter(file);
    } catch (IOException ex) {
      getPipeLog().error("Error opening valid stream output for file " + fileName);
    }

    return new BufferedWriter(fwriter, BUF_SIZE);
  }

  /**
   * Open the output file for writing error records
   *
   * @param filename The name of the file to open
   */
  public void openErrFile(String filename) {
    FileWriter fwriter = null;
    File file;
    file = new File(filename);

    try {
      if (file.createNewFile() == false) {
        getPipeLog().error("output file already exists = " + filename);
      }

      fwriter = new FileWriter(file);
    } catch (IOException ex) {
      getPipeLog().error("Error opening error stream output for file " + filename);
    }

    errorWriter = new BufferedWriter(fwriter);
  }

  @Override
  public void closeStream(int transactionNumber) {
    // Nothing for the moment
  }

  /**
   * Close the files now that writing has been concluded.
   *
   * @param transactionNumber The transaction number to close files for
   * @return 0 if the file closing went OK
   */
  public int closeFiles(int transactionNumber) {
    boolean ErrorFound = false;
    int ReturnCode = 0;
    BufferedWriter tmpValidWriter;
    int StreamNumber;
    TransControlStructure tcs;

    if (OutputStreamOpen) {
      try {
        // iterate over all valid writers and close them
        tcs = currentFileNames.get(transactionNumber);
        Iterator<Integer> tcsIter = tcs.ValidWriter.keySet().iterator();
        while (tcsIter.hasNext()) {
          // get the stream number to close
          StreamNumber = tcsIter.next();

          // get the corresponding writer object
          tmpValidWriter = tcs.ValidWriter.get(StreamNumber);

          // close it
          if (tmpValidWriter != null) {
            tmpValidWriter.close();
          }
        }
      } catch (IOException ioe) {
        getPipeLog().error("Error closing output file", ioe);
        ErrorFound = true;
      }

      try {
        if (errorWriter != null) {
          errorWriter.close();
        }
      } catch (IOException ioe) {
        getPipeLog().error("Error closing output file", ioe);
        ErrorFound = true;
      }

      OutputStreamOpen = false;

      if (ErrorFound) {
        ReturnCode = 1;
      } else {
        ReturnCode = 0;
      }
    }

    return ReturnCode;
  }

  /**
   * Close the files now that writing has been concluded. This changes the names
   * of the files that have been opened with processing names to the final
   * names, or deletes then if they are empty and we are configured to delete
   * empty files
   *
   * @param transactionNumber the transaction number to close
   */
  public void closeTransactionOK(int transactionNumber) {
    File f;
    int tmpStreamNumber;
    TransControlStructure tcs;

    // rename the valid files
    tcs = currentFileNames.get(transactionNumber);
    Iterator<Integer> tcsIter = tcs.ProcOutputFileName.keySet().iterator();
    while (tcsIter.hasNext()) {
      tmpStreamNumber = tcsIter.next();

      f = new File(getProcOutputName(transactionNumber, tmpStreamNumber));
      if ((f.length() == 0) & (DelEmptyOutFile)) {
        // Delete the empty file
        f.delete();
      } else {
        // Rename the file if it to be preserved
        f.renameTo(new File(getOutputName(transactionNumber, tmpStreamNumber)));
      }
    }

    // rename the error file
    f = new File(getProcErrorName(transactionNumber));
    if ((f.length() == 0) & (DelEmptyErrFile)) {
      // Delete the empty file
      f.delete();
    } else {
      // Rename the file
      f.renameTo(new File(getErrorName(transactionNumber)));
    }
  }

  /**
   * Delete the temporary files now that writing has been concluded with errors.
   *
   * @param transactionNumber the transaction number to close
   */
  public void closeTransactionErr(int transactionNumber) {
    File f;
    int tmpStreamNumber;

    // rename the file
    while (currentFileNames.get(transactionNumber).ProcOutputFileName.keySet().iterator().hasNext()) {
      tmpStreamNumber = currentFileNames.get(transactionNumber).ProcOutputFileName.keySet().iterator().next();

      f = new File(getProcOutputName(transactionNumber, tmpStreamNumber));
      f.delete();
    }

    // rename the file
    f = new File(getProcErrorName(transactionNumber));
    f.delete();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * When a transaction is started, the transactional layer calls this method to
   * see if we have any reason to stop the transaction being started, and to do
   * any preparation work that may be necessary before we start.
   *
   * @return 0 if the transaction can start
   */
  @Override
  public int startTransaction(int transactionNumber) {
    // We do not have any reason to inhibit the transaction start, so return
    // the OK flag
    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are flushing the
   * transaction
   *
   * @return 0 if the transaction was flushed OK
   */
  @Override
  public int flushTransaction(int transactionNumber) {
    // close the input stream
    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are committing the
   * transaction;
   */
  @Override
  public void commitTransaction(int transactionNumber) {
    closeTransactionOK(transactionNumber);
  }

  /**
   * Perform any processing that needs to be done when we are rolling back the
   * transaction;
   */
  @Override
  public void rollbackTransaction(int transactionNumber) {
    closeTransactionErr(transactionNumber);
  }

  /**
   * Close Transaction is the trigger to clean up transaction related
   * information such as variables, status etc.
   *
   * @param transactionNumber The transaction we are working on
   */
  @Override
  public void closeTransaction(int transactionNumber) {
    // Clean up the file names array
    currentFileNames.remove(transactionNumber);
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
  public String processControlEvent(String Command,
          boolean Init,
          String Parameter) {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_FILE_PATH)) {
      if (Init) {
        filePath = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return filePath;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_FILE_PREFIX)) {
      if (Init) {
        filePrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return filePrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_FILE_SUFFIX)) {
      if (Init) {
        fileSuffix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return fileSuffix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_PATH)) {
      if (Init) {
        errPath = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return errPath;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_PREFIX)) {
      if (Init) {
        errPrefix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return errPrefix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_SUFFIX)) {
      if (Init) {
        errSuffix = Parameter;
        ResultCode = 0;
      } else {
        if (Parameter.equals("")) {
          return errSuffix;
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DEL_EMPTY_OUT_FILE)) {
      if (Init) {
        if (Parameter != null) {
          if (Parameter.equalsIgnoreCase("true")) {
            DelEmptyOutFile = true;
            ResultCode = 0;
          }

          if (Parameter.equalsIgnoreCase("false")) {
            DelEmptyOutFile = false;
            ResultCode = 0;
          }
        }
      } else {
        if (Parameter.equals("")) {
          if (DelEmptyOutFile) {
            return "true";
          } else {
            return "false";
          }
        } else {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DEL_EMPTY_ERR_FILE)) {
      if (Init) {
        if (Parameter.equalsIgnoreCase("true")) {
          DelEmptyErrFile = true;
          ResultCode = 0;
        }

        if (Parameter.equalsIgnoreCase("false")) {
          DelEmptyErrFile = false;
          ResultCode = 0;
        }
      } else {
        if (Parameter.equals("")) {
          if (DelEmptyErrFile) {
            return "true";
          } else {
            return "false";
          }
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_FILE_PATH, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_FILE_PREFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_FILE_SUFFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERR_PATH, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERR_PREFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERR_SUFFIX, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DEL_EMPTY_OUT_FILE, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DEL_EMPTY_ERR_FILE, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);

    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OUT_FILE_NAME, false, false);
    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERR_FILE_NAME, false, false);
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of initialisation functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetFilePath()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_FILE_PATH);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetOutFilePrefix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_FILE_PREFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetOutFileSuffix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_FILE_SUFFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFilePath()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_ERR_PATH);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFilePrefix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_ERR_PREFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetErrFileSuffix()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(getPipeName(), getSymbolicName(),
            SERVICE_ERR_SUFFIX);

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetDelEmptyOutFile()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_DEL_EMPTY_OUT_FILE, "");

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetDelEmptyErrFile()
          throws InitializationException {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_DEL_EMPTY_ERR_FILE,
            "");

    return tmpFile;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetProcPrefix()
          throws InitializationException {
    String tmpProcPrefix;
    tmpProcPrefix = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_PROCPREFIX,
            "tmp");

    return tmpProcPrefix;
  }

  /**
   * Checks the file name from the input parameters.
   *
   * The method checks for validity of the input parameters that have been
   * configured, for example if the directory does not exist, an exception will
   * be thrown.
   */
  private void initFileName() throws InitializationException {
    File dir;

    if (filePath == null) {
      // The path has not been defined
      message = "Output Adapter <" + getSymbolicName() + "> processed file path has not been defined";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // if it is defined, is it valid?
    dir = new File(filePath);
    if (!dir.isDirectory()) {
      message = "Output Adapter <" + getSymbolicName() + "> used a processed file path <" + filePath + ">that does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    if (errPath == null) {
      // The path has not been defined
      message = "Output Adapter <" + getSymbolicName() + "> error file path has not been defined";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // if it is defined, is it valid?
    dir = new File(errPath);
    if (!dir.isDirectory()) {
      message = "Output Adapter <" + getSymbolicName() + "> used an error file path <" + errPath + "> that does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Check that we do not have a collision
    if (filePrefix.equals(errPrefix) && fileSuffix.equals(errSuffix)) {
      // we cannot do this, because we would open two files with the same name
      message = "Output Adapter <" + getSymbolicName() + "> uses a processed file prefix/suffix that is the same as error prefix/suffix";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }
  }

  /**
   * Get the proc file name for the valid record output file for the given
   * transaction
   *
   * @param TransactionNumber The number of the transaction to get the name for
   * @param stream
   * @return The processing output file name
   */
  protected String getProcOutputName(int TransactionNumber, int stream) {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(TransactionNumber);

    return tmpFileNames.ProcOutputFileName.get(stream);
  }

  /**
   * Get the final output file name for the valid record output file for the
   * given transaction
   *
   * @param TransactionNumber The number of the transaction to get the name for
   * @param stream The stream we are writing for
   * @return The final output file name
   */
  protected String getOutputName(int TransactionNumber, int stream) {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(TransactionNumber);

    return tmpFileNames.OutputFileName.get(stream);
  }

  /**
   * Get the proc file name for the error record output file for the given
   * transaction
   *
   * @param TransactionNumber The number of the transaction to get the name for
   * @return The processing error file name
   */
  protected String getProcErrorName(int TransactionNumber) {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(TransactionNumber);

    return tmpFileNames.ProcErrorFileName;
  }

  /**
   * Get the final output file name for the error record output file for the
   * given transaction
   *
   * @param TransactionNumber The number of the transaction to get the name for
   * @return The processing output file name
   */
  protected String getErrorName(int TransactionNumber) {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(TransactionNumber);

    return tmpFileNames.ErrorFileName;
  }

  /**
   * Get the stream file number for the given record. It is up to the
   * implementation class to manage the range of values that the stream number
   * can sensibly be.
   *
   * @param OutRec The record to get the stream number for
   * @return the stream number
   */
  protected abstract int getValidRecordStreamNumber(IRecord OutRec);
}
