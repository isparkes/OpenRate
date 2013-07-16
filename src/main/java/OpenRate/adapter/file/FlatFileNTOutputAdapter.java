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
import OpenRate.adapter.AbstractOutputAdapter;
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
import java.util.Iterator;

/**
 * Flat File Output Adapter. Writes to a file stream output, using transaction
 * aware handling.
 */
public abstract class FlatFileNTOutputAdapter
  extends AbstractOutputAdapter
  implements IEventInterface
{
  // The buffer size is the size of the buffer in the buffered reader
  private static final int BUF_SIZE = 65536;

  // File writers
  private BufferedWriter   validWriter;
  private BufferedWriter   errorWriter;

  // If we are using a single writer
  private boolean          singleWriter     = false;

  private String           filePath;
  private String           filePrefix;
  private String           fileSuffix;
  private String           errPath;
  private String           errPrefix;
  private String           errSuffix;
  private String           OutputFileName;
  private String           ErrFileName;
  private String           IntBaseName;
  private boolean          DelEmptyOutFile = false;
  private boolean          DelEmptyErrFile = true;

  // This is the prefix that will be added during processing
  private String ProcessingPrefix;

  // This tells us if we should look for a file to open
  // or continue reading from the one we have
  private boolean OutputStreamOpen = false;

  // This is the base name of the file we are outputting
  private String fileBaseName = null;

  // List of Services that this Client supports
  private final static String SERVICE_FILE_PATH          = "OutputFilePath";
  private final static String SERVICE_FILE_PREFIX        = "OutputFilePrefix";
  private final static String SERVICE_FILE_SUFFIX        = "OutputFileSuffix";
  private final static String SERVICE_DEL_EMPTY_OUT_FILE = "DeleteEmptyOutputFile";
  private final static String SERVICE_ERR_PATH           = "ErrFilePath";
  private final static String SERVICE_ERR_PREFIX         = "ErrFilePrefix";
  private final static String SERVICE_ERR_SUFFIX         = "ErrFileSuffix";
  private final static String SERVICE_DEL_EMPTY_ERR_FILE = "DeleteEmptyErrorFile";
  private static final String SERVICE_PROCPREFIX         = "ProcessingPrefix";

  //final static String SERVICE_OUT_FILE_NAME = "OutputFileName";
  //final static String SERVICE_ERR_FILE_NAME = "ErrFileName";

  /**
    * Default Constructor.
    */
  public FlatFileNTOutputAdapter()
  {
    super();

    this.validWriter = null;
    this.errorWriter = null;
  }

  /**
   * Gets the buffered file writer for valid records.
   *
   * @return The writer for valid records
   */
  public BufferedWriter getValidWriter()
  {
    return validWriter;
  }

  /**
   * Gets the buffered file writer for error records.
   *
   * @return The writer for valid records
   */
  public BufferedWriter getErrorWriter()
  {
    return errorWriter;
  }

 /**
  * Initialise the module. Called during pipeline creation.
  * Initialize the output adapter with the configuraton that is to be used
  * for this instance of the adapter.
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
  }

  /**
  * Write good records to the defined output stream.
  */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException
  {
    Collection<IRecord> OutRecCol;
    FlatRecord          OutRec;
    Iterator<IRecord>   OutRecIter;

    OutRecCol = procValidRecord(r);
    OutRecIter = OutRecCol.iterator();

    while (OutRecIter.hasNext())
    {
      OutRec = (FlatRecord)OutRecIter.next();

      try
      {
        validWriter.write(OutRec.getData());
        validWriter.newLine();
      }
      catch (IOException ioe)
      {
        this.getExceptionHandler().reportException(new ProcessingException(ioe,getSymbolicName()));
      }
    }

    return r;
  }

 /**
  * Write bad records to the defined output stream.
  */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException
  {
    Collection<IRecord> OutRecCol;
	  FlatRecord          OutRec;
	  Iterator<IRecord>   OutRecIter;

    OutRecCol = procErrorRecord(r);
    OutRecIter = OutRecCol.iterator();

    while (OutRecIter.hasNext())
    {
      OutRec = (FlatRecord)OutRecIter.next();

      try
      {
        validWriter.write(OutRec.getData());
        validWriter.newLine();
      }
      catch (IOException ioe)
      {
        this.getExceptionHandler().reportException(new ProcessingException(ioe,getSymbolicName()));
      }
    }

    return r;
  }

  /**
  * Stub to the header processing that opens the output file when we receive the
  * header record
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    HeaderRecord tmpHeader;
    tmpHeader = (HeaderRecord)r;
    fileBaseName = tmpHeader.getStreamName();
    OutputStreamOpen = true;

    // Calculate the names and open the writers
    OutputFileName = filePath + System.getProperty("file.separator") +
                     ProcessingPrefix + filePrefix + fileBaseName +
                     fileSuffix;
    ErrFileName    = errPath + System.getProperty("file.separator") +
                     ProcessingPrefix + errPrefix + fileBaseName +
                     errSuffix;
    openValidFile(OutputFileName);
    openErrFile(ErrFileName);

    return r;
  }

  /**
  * Stub to the trailer processing that closes the output file
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    int CloseResult;

    TrailerRecord tmpTrailer;
    tmpTrailer = (TrailerRecord)r;
    fileBaseName = tmpTrailer.getStreamName();
    SetBaseName(fileBaseName);
    CloseResult = closeFiles();
    if (CloseResult == 0)
    {
      // all OK
      closeOK();
    }
    else
    {
      // There was an error
      closeErr();
    }
    SetBaseName(null);

    return r;
  }

 /**
  * Open the output file for writing good records to
  *
  * @param filename The name of the file to open
  */
  public void openValidFile(String filename)
  {
    FileWriter fwriter = null;
    File       file;
    file = new File(filename);

    try
    {
      if (file.createNewFile() == false)
      {
        getPipeLog().error("output file already exists = " + filename);
      }

      fwriter = new FileWriter(file);
    }
    catch (IOException ex)
    {
      getPipeLog().error("Error opening valid stream output for file " + filename);
    }

    validWriter = new BufferedWriter(fwriter, BUF_SIZE);
  }

 /**
  * Open the output file for writing error records
  *
  * @param filename The name of the file to open
  */
  public void openErrFile(String filename)
  {
    FileWriter fwriter = null;
    File       file;
    file = new File(filename);

    if (singleWriter)
    {
      errorWriter = validWriter;
    }
    else
    {
      try
      {
        if (file.createNewFile() == false)
        {
          getPipeLog().error("output file already exists = " + filename);
        }

        fwriter = new FileWriter(file);
      }
      catch (IOException ex)
      {
        getPipeLog().error("Error opening error stream output for file " + filename);
      }

      errorWriter = new BufferedWriter(fwriter);
    }
  }

  @Override
  public void closeStream(int TransactionNumber)
  {
    // Nothing for the moment
  }

  /**
  * Close the files now that writing has been concluded.
   *
   * @return 0 if the file was closed OK, otherwise 1
   */
  public int closeFiles()
  {
    boolean ErrorFound = false;
    int ReturnCode = 0;

    if (OutputStreamOpen)
    {
      try
      {
        if (validWriter != null)
        {
          validWriter.close();
        }
      }
      catch (IOException ioe)
      {
        getPipeLog().error("Error closing output file", ioe);
        ErrorFound = true;
      }

      if (singleWriter == false)
      {
        try
        {
          if (errorWriter != null)
          {
            errorWriter.close();
          }
        }
        catch (IOException ioe)
        {
          getPipeLog().error("Error closing output file", ioe);
          ErrorFound = true;
        }
      }

      OutputStreamOpen = false;

      if (ErrorFound)
      {
        ReturnCode = 1;
      }
      else
      {
        ReturnCode = 0;
      }
    }

    return ReturnCode;
  }

  /**
  * Close the files now that writing has been concluded.
  */
  public void closeOK()
  {
    String ValidFileProcName;
    String ValidFileDoneName;
    String ErrFileProcName;
    String ErrFileDoneName;
    File f;

    fileBaseName = GetBaseName();

    // Calculate the names for the writer rename
    ValidFileProcName = filePath + System.getProperty("file.separator") +
                        ProcessingPrefix + filePrefix + fileBaseName +
                        fileSuffix;
    ErrFileProcName = errPath + System.getProperty("file.separator") +
                      ProcessingPrefix + errPrefix + fileBaseName +
                      errSuffix;
    ValidFileDoneName = filePath + System.getProperty("file.separator") +
                        filePrefix + fileBaseName + fileSuffix;
    ErrFileDoneName = errPath + System.getProperty("file.separator") +
                      errPrefix + fileBaseName + errSuffix;

    // rename the file
    f = new File(ValidFileProcName);
    if (DelEmptyOutFile && (f.length() == 0))
    {
      // Delete the empty file
      f.delete();
    }
    else
    {
      // Rename the file
      f.renameTo(new File(ValidFileDoneName));
    }

    if (singleWriter == false)
    {
      // rename the file
      f = new File(ErrFileProcName);
      if (DelEmptyErrFile && (f.length() == 0))
      {
        // Delete the empty file
        f.delete();
      }
      else
      {
        // Rename the file
        f.renameTo(new File(ErrFileDoneName));
      }
    }
  }

  /**
  * Close the files now that writing has been concluded.
  */
  public void closeErr()
  {
    String ValidFileProcName;
    String ErrFileProcName;
    File f;

    fileBaseName = GetBaseName();

    // Calculate the names for the writer rename
    ValidFileProcName = filePath + System.getProperty("file.separator") +
                        ProcessingPrefix + filePrefix + fileBaseName +
                        fileSuffix;
    ErrFileProcName = errPath + System.getProperty("file.separator") +
                      ProcessingPrefix + errPrefix + fileBaseName +
                      errSuffix;

    // rename the file
    f = new File(ValidFileProcName);
    f.delete();

    if (singleWriter == false)
    {
      // rename the file
      f = new File(ErrFileProcName);
      f.delete();
    }
  }

  private void SetBaseName(String BaseName)
  {
    IntBaseName = BaseName;
  }

  private String GetBaseName()
  {
    return IntBaseName;
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of custom implementation functions --------------------
  // -----------------------------------------------------------------------------

  /**
   * Checks if the valid output file is empty. This method is intended to be
   * overwritten in the case that you wish to modify the behaviour of the
   * output file deletion.
   *
   * The default behaviour is that we check to see if any bytes have been
   * written to the output file, but sometimes this is not the right way, for
   * example if a file has a header/trailer but no detail records.
   *
   * @param validFileProcName The file to check
   * @return true if the file is empty, otherwise false
   */
  public boolean getOutputFileEmpty(String validFileProcName)
  {
    File f = new File(validFileProcName);
    if (f.length() == 0)
    {
      return true;
    }
    else
    {
      return false;
    }
  }

  /**
   * Checks if the error output file is empty. This method is intended to be
   * overwritten in the case that you wish to modify the behaviour of the
   * output file deletion.
   *
   * The default behaviour is that we check to see if any bytes have been
   * written to the output file, but sometimes this is not the right way, for
   * example if a file has a header/trailer but no detail records.
   *
   * Note that this method is not called if the "single output file mode" has
   * been selected by not defining an error output file in the output adapter.
   *
   * @param errorFileProcName The file to check
   * @return true if the file is empty, otherwise false
   */
  public boolean getErrorFileEmpty(String errorFileProcName)
  {
    File f = new File(errorFileProcName);
    if (f.length() == 0)
    {
      return true;
    }
    else
    {
      return false;
    }
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

    if (Command.equalsIgnoreCase(SERVICE_FILE_PATH))
    {
      if (Init)
      {
        filePath = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return filePath;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_FILE_PREFIX))
    {
      if (Init)
      {
        filePrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return filePrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_FILE_SUFFIX))
    {
      if (Init)
      {
        fileSuffix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return fileSuffix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_PATH))
    {
      if (Init)
      {
        errPath = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return errPath;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_PREFIX))
    {
      if (Init)
      {
        errPrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return errPrefix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ERR_SUFFIX))
    {
      if (Init)
      {
        errSuffix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return errSuffix;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DEL_EMPTY_OUT_FILE))
    {
      if (Init)
      {
        if (Parameter != null)
        {
          if (Parameter.equalsIgnoreCase("true"))
          {
            DelEmptyOutFile = true;
            ResultCode = 0;
          }

          if (Parameter.equalsIgnoreCase("false"))
          {
            DelEmptyOutFile = false;
            ResultCode = 0;
          }
        }
      }
      else
      {
        if (Parameter.equals(""))
        {
          if (DelEmptyOutFile)
          {
            return "true";
          }
          else
          {
            return "false";
          }
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DEL_EMPTY_ERR_FILE))
    {
      if (Init)
      {
        if (Parameter.equalsIgnoreCase("true"))
        {
          DelEmptyErrFile = true;
          ResultCode = 0;
        }

        if (Parameter.equalsIgnoreCase("false"))
        {
          DelEmptyErrFile = false;
          ResultCode = 0;
        }
      }
      else
      {
        if (Parameter.equals(""))
        {
          if (DelEmptyErrFile)
          {
            return "true";
          }
          else
          {
            return "false";
          }
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
                          throws InitializationException
  {
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
                               throws InitializationException
  {
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
                               throws InitializationException
  {
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
                             throws InitializationException
  {
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
                               throws InitializationException
  {
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
                               throws InitializationException
  {
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
                               throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                     SERVICE_DEL_EMPTY_OUT_FILE,
                                                     "");

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetDelEmptyErrFile()
                               throws InitializationException
  {
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
                                 throws InitializationException
  {
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
  private void initFileName() throws InitializationException
  {
    String message;
    File   dir;

    if (filePath == null)
    {
      // we cannot do this, because we would open two files with the same name
      message = "Output adapter output file path has not been defined";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // if it is defined, is it valid?
    dir = new File(filePath);
    if (!dir.isDirectory())
    {
      message = "Processed file path does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    if (errPath == null)
    {
      // The path has not been defined
      message = "Using Single Output for Adapter <" + getSymbolicName() + ">";
      getPipeLog().info(message);
      singleWriter = true;
    }
    else
    {
      // if it is defined, is it valid?
      dir = new File(errPath);
      if (!dir.isDirectory())
      {
        message = "Output Adapter <" + getSymbolicName() + "> used an error file path <" + errPath + "> that does not exist or is not a directory";
        getPipeLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // Check that we do not have a collision
      if (filePrefix.equals(errPrefix) && fileSuffix.equals(errSuffix))
      {
        // use a single output file
        message = "Using Single Output for Adapter <" + getSymbolicName() + ">";
        getPipeLog().info(message);
        singleWriter = true;
      }
    }
  }
}
