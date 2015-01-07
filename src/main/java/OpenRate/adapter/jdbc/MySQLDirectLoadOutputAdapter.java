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

package OpenRate.adapter.jdbc;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=MySQL_Direct_Load_Output_Adapter'>click here</a> to go to wiki page.
 * <br>MySQL Direct Load Output Adapter. Writes records to an intermediate file
 * <br>output, using transaction aware handling. After the file is written
 * <br>the records are loaded into a MySQL table using a "LOAD FILE" invocation.
 */
public abstract class MySQLDirectLoadOutputAdapter
  extends AbstractTransactionalOutputAdapter
  implements IEventInterface
{
  // The buffer size is the size of the buffer in the buffered reader
  private static final int BUF_SIZE = 65536;

  // File writers
  private BufferedWriter   validWriter;

  // paths components
  private String           filePath;
  private String           filePrefix;
  private String           fileSuffix;
  private boolean          DelEmptyOutFile = false;

  // This is the prefix that will be added during processing
  private String processingPrefix;

  // This tells us if we should look for a file to open
  // or continue reading from the one we have
  private boolean outputStreamOpen = false;

  // This is the base name of the file we are outputting
  private String fileBaseName = null;

  // this is the connection from the connection pool that we are using
  private static final String DATASOURCE_KEY = "DataSource";

  // The SQL statements from the properties that are used to get the records
  private static final String INIT_QUERY_KEY = "InitStatement";
  private static final String LOAD_QUERY_KEY = "LoadStatement";

  // List of Services that this Client supports
  private final static String SERVICE_FILE_PATH          = "OutputFilePath";
  private final static String SERVICE_FILE_PREFIX        = "OutputFilePrefix";
  private final static String SERVICE_FILE_SUFFIX        = "OutputFileSuffix";
  private final static String SERVICE_DEL_EMPTY_OUT_FILE = "DeleteEmptyOutputFile";
  private static final String SERVICE_PROCPREFIX         = "ProcessingPrefix";
  private final static String SERVICE_INIT_QUERY_KEY     = "InitStatement";
  private final static String SERVICE_LOAD_QUERY_KEY     = "LoadStatement";

  /**
   * The query that is used to prepare the database for record insert
   */
  protected String initQuery;

  /**
   * The insert query via direct load command
   */
  protected String loadQuery;

  /**
   * This is the name of the data source
   */
  protected String dataSourceName;

  /**
   * This is the query that is used to make the record insert into the table
   */
  protected PreparedStatement stmtLoadQuery;

/**
 * This is our connection object
 */
  protected Connection jdbcCon;

  // this is the persistent result set that we use to incrementally get the records
  ResultSet rs = null;

  // This is used to hold the calculated file names
  private class TransControlStructure
  {
    String outputFileName;
    String procOutputFileName;
  }

  // This holds the file names for the files that are in processing at any
  // given moment
  private HashMap <Integer, TransControlStructure> currentFileNames;

  /**
    * Default Constructor.
    */
  public MySQLDirectLoadOutputAdapter()
  {
    super();

    this.validWriter = null;
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
  * Initialise the output adapter with the configuration that is to be used
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
    ConfigHelper = initGetProcPrefix();
    processControlEvent(SERVICE_PROCPREFIX, true, ConfigHelper);

    // get any initialisation SQL that we need to perform, such as marking
    // records for select and removal
    ConfigHelper = initInitQuery();
    processControlEvent(SERVICE_INIT_QUERY_KEY, true, ConfigHelper);

    // this is the SQL that will tidy up after the select, and ensure that
    // they are not selected next time, in the case that the processing
    // was completed correctly
    ConfigHelper = initLoadQuery();
    processControlEvent(SERVICE_LOAD_QUERY_KEY, true, ConfigHelper);

    // The data source property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    ConfigHelper = initDataSourceName();
    processControlEvent(DATASOURCE_KEY, true, ConfigHelper);

    // Check the parameters we received
    initFileName();

    // create the structure for storing filenames
    currentFileNames = new HashMap <>(10);

    // prepare the data source - this does not open a connection
    if(DBUtil.initDataSource(dataSourceName) == null)
    {
      message = "Could not initialise DB connection <" + dataSourceName + "> to in module <" + getSymbolicName() + ">.";
      getPipeLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * Process the stream header. Get the file base name and open the transaction.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord procHeader(IRecord r) throws ProcessingException
  {
    HeaderRecord tmpHeader;
    int tmpTransNumber;
    TransControlStructure tmpFileNames = new TransControlStructure();

    tmpHeader = (HeaderRecord)r;

    super.procHeader(r);

    // if we are not currently streaming, open the stream using the transaction
    // information for the transaction we are processing
    if (!outputStreamOpen)
    {
      fileBaseName = tmpHeader.getStreamName();
      tmpTransNumber = tmpHeader.getTransactionNumber();

      // Calculate the names and open the writers
      tmpFileNames.procOutputFileName = filePath + System.getProperty("file.separator") +
                                        processingPrefix + filePrefix + fileBaseName + fileSuffix;
      tmpFileNames.outputFileName     = filePath + System.getProperty("file.separator") +
                                        filePrefix + fileBaseName + fileSuffix;

      // Store the names for later
      currentFileNames.put(tmpTransNumber, tmpFileNames);

      openValidFile(tmpFileNames.procOutputFileName);
      outputStreamOpen = true;
    }

    return r;
  }

 /**
  * Write good records to the defined output stream. This method performs
  * record expansion (the opposite of record compression) and then calls the
  * write for each of the records that results.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException
  {
    Collection<IRecord> outRecCol;
    FlatRecord          outRec;
    Iterator<IRecord>   outRecIter;

    outRecCol = procValidRecord(r);

    // Null return means "do not bother to process"
    if (outRecCol != null)
    {
      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext())
      {
        outRec = (FlatRecord)outRecIter.next();
        try
        {
          validWriter.write(outRec.getData());
          validWriter.newLine();
        }
        catch (IOException ioe)
        {
          getExceptionHandler().reportException(new ProcessingException(ioe,getSymbolicName()));
        }
      }
    }

    return r;
  }

 /**
  * This adapter does not allow error records. Error records will be written
  * to the load log file.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException
  */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException
  {
    // Just return - no processing needed
    return r;
  }

 /**
  * Process the stream trailer. Get the file base name and open the transaction.
  *
  * @param r The current record we are working on
  * @return The prepared record
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    // Close the files
    closeFiles(getTransactionNumber());

    // Do the transaction level maintenance
    super.procTrailer(r);

    return r;
  }

 /**
  * Open the output file for writing.
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

  @Override
  public void closeStream(int transactionNumber)
  {
    // Nothing for the moment
  }

 /**
  * Close the files now that writing has been concluded.
  *
  * @param transactionNumber The transaction number we are working on
  * @return 0 if the file closing went OK
  */
  public int closeFiles(int transactionNumber)
  {
    boolean ErrorFound = false;
    int ReturnCode = 0;

    if (outputStreamOpen)
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

      outputStreamOpen = false;

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
   *
  * @param transactionNumber The transaction number we are working on
  */
  public void closeTransactionOK(int transactionNumber)
  {
    File f;

    // rename the valid file
    f = new File(getProcOutputName(transactionNumber));
    if (DelEmptyOutFile && getOutputFileEmpty(transactionNumber))
    {
      // Delete the empty file
      f.delete();
    }
    else
    {
      // Rename the file
      f.renameTo(new File(getOutputName(transactionNumber)));
    }
  }

 /**
  * Close the files now that writing has been concluded.
  *
  * @param transactionNumber The transaction number we are working on
  */
  public void closeTransactionErr(int transactionNumber)
  {
    File f;

    // rename the file
    f = new File(getProcOutputName(transactionNumber));
    f.delete();
  }

 /**
  * Load the output file using the SQL direct load.
  *
  * @param transactionNumber Transaction to load
  * @return true if it as good, otherwise false
  */
  public boolean performLoad(int transactionNumber)
  {
    // get the file name we are to load
    String loadFileName = getProcOutputName(transactionNumber);
    String FQFileName = System.getProperty("user.dir") + "/" +  loadFileName;

    // Correct the separators
    FQFileName = FQFileName.replaceAll("\\\\", "/");
    getPipeLog().info("Load of file <" + FQFileName + "> in module <" + getSymbolicName() + "> for transaction <" + transactionNumber + ">");

    // Perform the initialisation of the DB side
    try
    {
      jdbcCon = DBUtil.getConnection(dataSourceName);
    }
    catch (InitializationException ex)
    {
      getExceptionHandler().reportException(ex);
    }

    try
    {
      prepareStatement(FQFileName);
    }
    catch (SQLException Sex)
    {
      // Not good. Abort the transaction
      message = "Error preparing statement. message <" + Sex.getMessage() + ">. Aborting transaction.";
      getExceptionHandler().reportException(new ProcessingException(message,Sex,getSymbolicName()));
      this.setTransactionAbort(getTransactionNumber());
    }
    try
    {
      // Really do the load
      OpenRate.getOpenRateStatsLog().info("Output <" + getSymbolicName() + "> start load of file for transaction <" + transactionNumber + ">");
      stmtLoadQuery.executeUpdate();
      OpenRate.getOpenRateStatsLog().info("Output <" + getSymbolicName() + "> end load of file for transaction <" + transactionNumber + ">");

      // Clean up
      stmtLoadQuery.close();
      jdbcCon.close();
    }
    catch (SQLException ex)
    {
      getPipeLog().error("Load for transaction <" + transactionNumber + "> failed. message = <" + ex.getMessage() + ">");
      try
      {
        // Clean up
        stmtLoadQuery.close();
        jdbcCon.close();
      }
      catch (SQLException ex1)
      {
        // Sliently drop it
      }

      return false;
    }

    // Everything went well
    return true;
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
   * @param transactionNumber The number of the transaction to check for
   * @return true if the file is empty, otherwise false
   */
  public boolean getOutputFileEmpty(int transactionNumber)
  {
    File f = new File(getProcOutputName(transactionNumber));
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
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * When a transaction is started, the transactional layer calls this method to
  * see if we have any reason to stop the transaction being started, and to do
  * any preparation work that may be necessary before we start.
  *
  * @param transactionNumber The transaction number we are working on
  * @return 0 if the transaction can start
  */
  @Override
  public int startTransaction(int transactionNumber)
  {
    // We do not have any reason to inhibit the transaction start, so return
    // the OK flag
    return 0;
  }

 /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction
  *
  * @param transactionNumber The transaction number we are working on
  * @return 0 if the transaction was flushed OK
  */
  @Override
  public int flushTransaction(int transactionNumber)
  {
    // close the input stream
    if (performLoad(transactionNumber))
    {
      return 0;
    }
    else
    {
      return -1;
    }
  }

 /**
  * Perform any processing that needs to be done when we are committing the
  * transaction
  *
  * @param transactionNumber The transaction number we are working on
  */
  @Override
  public void commitTransaction(int transactionNumber)
  {
    closeTransactionOK(transactionNumber);
  }

 /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction
  *
  * @param transactionNumber The transaction number we are working on
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    closeTransactionErr(transactionNumber);
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

    if (Command.equalsIgnoreCase(DATASOURCE_KEY))
    {
      if (Init)
      {
        dataSourceName = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return dataSourceName;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_INIT_QUERY_KEY))
    {
      if (Init)
      {
        initQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return initQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_LOAD_QUERY_KEY))
    {
      if (Init)
      {
        loadQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return loadQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

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

    if (Command.equalsIgnoreCase(SERVICE_PROCPREFIX))
    {
      if (Init)
      {
        processingPrefix = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return processingPrefix;
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DEL_EMPTY_OUT_FILE, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);

    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OUT_FILE_NAME, false, false);
    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ERR_FILE_NAME, false, false);
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of initialisation functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
  */
  private void prepareStatement(String loadFileName) throws SQLException
  {
    // prepare the SQL for the Insert statement
    if(loadQuery == null || loadQuery.isEmpty())
    {
      stmtLoadQuery = null;
    }
    else
    {
      // Change the file name
      String changedLoadQuery = loadQuery.replace("load_file_name", loadFileName);
      stmtLoadQuery = jdbcCon.prepareStatement(changedLoadQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
    }

    // prepare the SQL for the Commit Statement
/*    StmtCommitQuery = JDBCcon.prepareStatement(CommitQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);

    // prepare the SQL for the Rollback Statement
    StmtRollbackQuery = JDBCcon.prepareStatement(RollbackQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY); */
  }

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
      // The path has not been defined
      message = "Output Adapter <" + getSymbolicName() + "> processed file path has not been defined";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // if it is defined, is it valid?
    dir = new File(filePath);
    if (!dir.isDirectory())
    {
      message = "Output Adapter <" + getSymbolicName() + "> used a processed file path <" + filePath + ">that does not exist or is not a directory";
      getPipeLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * Get the proc file name for the valid record output file for the given
  * transaction
  *
  * @param transactionNumber The number of the transaction to get the name for
  * @return The processing output file name
  */
  protected String getProcOutputName(int transactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(transactionNumber);

    return tmpFileNames.procOutputFileName;
  }

 /**
  * Get the final output file name for the valid record output file for the
  * given transaction
  *
  * @param transactionNumber The number of the transaction to get the name for
  * @return The final output file name
  */
  protected String getOutputName(int transactionNumber)
  {
    TransControlStructure tmpFileNames;

    // Get the name to work on
    tmpFileNames = currentFileNames.get(transactionNumber);

    return tmpFileNames.outputFileName;
  }

 /**
  * The InitQuery is the query that will be executed at the beginning of a
  * new stream of data. This is executed once, and should be used to prepare
  * data for extraction.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initInitQuery()
    throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   INIT_QUERY_KEY,
                                                   "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      message = "Output <" + getSymbolicName() + "> - Initialisation statement not found from <" + INIT_QUERY_KEY + ">";
      getPipeLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    return query;
  }

 /**
  * The LoadQuery is the query that will be executed to actually handle the
  * data writing.
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initLoadQuery()
                         throws InitializationException
  {
    String query;

    // Get the init statement from the properties
   query = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   LOAD_QUERY_KEY,
                                                   "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      message = "Output <" + getSymbolicName() + "> - Initialisation statement not found from <" + LOAD_QUERY_KEY + ">";
      getPipeLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    return query;
  }

 /**
  * Get the data source name from the properties
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initDataSourceName()
                        throws InitializationException
  {
    String DSN;
    DSN = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                 DATASOURCE_KEY,
                                                 "None");

    if ((DSN == null) || DSN.equalsIgnoreCase("None"))
    {
      message = "Output <" + getSymbolicName() + "> - Datasource name not found from <" + DATASOURCE_KEY + ">";
      getPipeLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    return DSN;
  }
}
