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

package OpenRate.adapter.jdbc;

import OpenRate.CommonConfig;
import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.DBRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=JDBC_Input_Adapter'>click here</a> to go to wiki page.
 *
 * <p>Generic JDBC InputAdapter.<br>This module is a little more complicated than the
 * file input adapter, because we are more restricted about the way that we
 * deal with done records, which will depend on the limitations of the upstream
 * systems and your particular business model. Possible strategies are:
 * 1) Read the rows and delete them from the source table
 * 2) Read the rows which have a timestamp after time "t" and then update the
 *    internal value of "t" so that we do not read the records again
 * 3) flag records that have been read with an update
 * 4) mark the records the have been read in a "tick list" so that we do not
 *    process them again
 *
 * The exact method that is used will have to be decided by you. To provide an
 * abstraction of these methods, JDBCInputAdapter provides the following methods
 * to allow you enough possibilities to hook the adapter to get the results you
 * want:
 *
 *   Initialisation
 *   --------------
 *
 * Initialisation will validate the configuration to ensure that nothing will go
 * wrong at a later stage. While this is not strictly necessary (we could always
 * catch errors on the fly), it is highly recommended that all possible validations
 * are done once and only once here. All checks at a later state will slow things
 * down, and will be performed millions of times (once for each record) instead
 * of once here.
 *
 *   Scanning and Processing
 *   -----------------------
 *
 * The basic scanning and processing loop looks like this:
 * - getInputAvailable() Scan to see if there is any work to do
 *     assignInput() mark the file as being in processing if we find work to do
 *     open the input stream and create a new transaction object in the TM
 *     read the records in from the stream, updating the TM record count
 *     in batches of n records
 *     - transformInput() is a user definable method in the implementation class
 *       which transforms the FlatRecord read from the file into a record for
 *       the processing
 *       - When the stream runs out, set the TM status to FLUSHED and wait for
 *         the TM to confirm that processing has finished down the pipe
 *         - When the TM confirms that the pipe has flushed by calling the
 *           trigger() method with the parameter of "flushed", the stream is
 *           closed, setting the TM status to FINISHED
 *           - When all the modules have finished, the transaction is committed
 *             by calling the trigger() method with the parameter of "committed"
 *             which causes the input file to be renamed and the transaction
 *             to be committed or rolled back.
 */
public abstract class JDBCInputAdapter
  extends AbstractTransactionalInputAdapter
{
  /**
   * This is the statement we use to validate the connection
   */
  protected String ValidateQuery;

  /**
   * Count the number of records waiting for processing
   */
  protected String CountQuery;

  /**
   * Prepare the records for processing
   */
  protected String InitQuery;

  /**
   * Get the prepared records
   */
  protected String SelectQuery;

  /**
   * Commit the processed records if the transaction ended correctly
   */
  protected String CommitQuery;

  /**
   * Rollback the changes if the transaction did not end correctly
   */
  protected String RollbackQuery;

  /**
   * This is the name of the data source
   */
  protected String dataSourceName;

  /**
   * Prepared count query
   */
  protected PreparedStatement StmtCountQuery;

  /**
   * Prepared Init Query
   */
  protected PreparedStatement StmtInitQuery;

  /**
   * Prepared Select Query
   */
  protected PreparedStatement StmtSelectQuery;

  /**
   * Prepared Commit Query
   */
  protected PreparedStatement StmtCommitQuery;

  /**
   * Prepared Rollback Query
   */
  protected PreparedStatement StmtRollbackQuery;

  // this is the connection from the connection pool that we are using
  private static final String DATASOURCE_KEY = "DataSource";

  // The SQL statements from the properties that are used to get the records
  private static final String INIT_QUERY_KEY = "InitStatement";
  private static final String SELECT_QUERY_KEY = "RecordSelectStatement";
  private static final String COUNT_QUERY_KEY = "RecordCountStatement";
  private static final String COMMIT_QUERY_KEY = "CommitStatement";
  private static final String ROLLBACK_QUERY_KEY = "RollbackStatement";

  // List of Services that this Client supports
  private final static String SERVICE_DATASOURCE_KEY = "DataSource";
  private final static String SERVICE_INIT_QUERY_KEY = "InitStatement";
  private final static String SERVICE_SELECT_QUERY_KEY = "RecordSelectStatement";
  private final static String SERVICE_COUNT_QUERY_KEY = "RecordCountStatement";
  private final static String SERVICE_COMMIT_QUERY_KEY = "CommitStatement";
  private final static String SERVICE_ROLLBACK_QUERY_KEY = "RollbackStatement";
  private final static String SERVICE_CONNECTION_TEST_KEY = "ValidateStatement";

  // This tells us if we should look for new work or continue with something
  // that is going on at the moment
  private boolean InputStreamOpen = false;

  // This is our connection object
  Connection JDBCcon;

  // this is the persistent result set that we use to incrementally get the records
  ResultSet rs = null;

  // used to track the status of our transaction
  private int transactionNumber = 0;
  private int InputRecordNumber = 0;

 /**
  * Holds the time stamp for the transaction
  */
  protected String ORTransactionId = null;

 /**
  * Default Constructor
  */
  public JDBCInputAdapter()
  {
    super();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation.
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

    // this is the SQL that we perform on adapter startup
    ConfigHelper = initInitQuery(PipelineName);
    processControlEvent(SERVICE_INIT_QUERY_KEY, true, ConfigHelper);

    // this is the SQL that scans to see if we have work waiting for process
    ConfigHelper = initCountQuery(PipelineName);
    processControlEvent(SERVICE_COUNT_QUERY_KEY, true, ConfigHelper);

    // this is the SQL that will select the records
    ConfigHelper = initSelectQuery(PipelineName);
    processControlEvent(SERVICE_SELECT_QUERY_KEY, true, ConfigHelper);

    // this is the SQL that will tidy up after the select, and ensure that
    // they are not selected next time, in the case that the processing
    // was completed correctly
    ConfigHelper = initCommitQuery(PipelineName);
    processControlEvent(SERVICE_COMMIT_QUERY_KEY, true, ConfigHelper);

    // this is the SQL that will tidy up after the select, and ensure that
    // they are not selected next time, in the case that the processing
    // was completed correctly
    ConfigHelper = initRollbackQuery(PipelineName);
    processControlEvent(SERVICE_ROLLBACK_QUERY_KEY, true, ConfigHelper);

    // The datasource property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    ConfigHelper = initDataSourceName(PipelineName);
    processControlEvent(SERVICE_DATASOURCE_KEY, true, ConfigHelper);

    // prepare the data source - this does not open a connection
    if(DBUtil.initDataSource(dataSourceName) == null)
    {
      String Message = "Could not initialise DB connection <" + dataSourceName + "> to in module <" + getSymbolicName() + ">.";
      PipeLog.error(Message);
      throw new InitializationException(Message);
    }
  }

  /**
   * Retrieve all the source records that should be processed by the
   * pipeline.
   */
  @Override
  protected Collection<IRecord> loadBatch() throws ProcessingException
  {
    ResultSetMetaData   Rsmd;
    Collection<IRecord> Outbatch;
    int                 ThisBatchCounter = 0;
    int                 ColumnCount;
    int                 ColumnIdx;
    String[]            tmpColumns;

    // The Record types we will have to deal with
    HeaderRecord  tmpHeader;
    TrailerRecord tmpTrailer;
    DBRecord      tmpRecord;
    IRecord       batchRecord;

    PipeLog.debug("loadBatch()");
    Outbatch = new ArrayList<>();

    // This layer deals with opening the stream if we need to
    if (InputStreamOpen == false)
    {
      // Check to see if there is any new work to do
      if (canStartNewTransaction() && (getInputAvailable() > 0))
      {
        // There is work to do, we execute the Init SQL so that we have the chance to
        // prepare the data for reading
        assignInput();

        // the renamed file provided by assignInput
        try
        {
          // Open the select statement
          prepareSelectStatement();
          rs = StmtSelectQuery.executeQuery();

          // See if we get an empty result set
          rs.last();
          if(rs.getRow() > 0)
          {
            // Create the new transaction to hold the information. This is done in
            // The transactional layer - we just trigger it here
            // Create the transaction base name according to a simple counter
            transactionNumber = createNewTransaction();

            // This is the transaction identifier for all records in this stream
            ORTransactionId = getTransactionID(transactionNumber);

            // reset the cursor
            rs.beforeFirst();

            InputStreamOpen = true;
            InputRecordNumber = 0;

            // Inform the transactional layer that we have started processing
            setTransactionProcessing(transactionNumber);

            // Inject a stream header record into the stream
            tmpHeader = new HeaderRecord();
            tmpHeader.setStreamName(ORTransactionId);
            tmpHeader.setTransactionNumber(transactionNumber);

            // Increment the stream counter
            incrementStreamCount();

            // Pass the header to the user layer for any processing that
            // needs to be done
            tmpHeader = (HeaderRecord)procHeader((IRecord)tmpHeader);
            Outbatch.add(tmpHeader);
          }
          else
          {
            String Message = "Select statement did not return rows in <" + getSymbolicName() + ">";
            PipeLog.error(Message);

            // No work to do - return the empty batch
            return Outbatch;
          }
        }
        catch (SQLException Sex)
        {
          String Message = "Select SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
          PipeLog.error(Message);

          // Stop any transactions that are open
          if (transactionNumber > 0)
          {
            cancelTransaction(transactionNumber);
          }

          // Close statement and connection
          closeSelectStatement();

          // report the exception
          throw new ProcessingException(Message);
        }
      }
    }

    if (InputStreamOpen)
    {
      try
      {
        // we need to know something about the result set so we can build
        // the records out of it
        Rsmd = rs.getMetaData();
        ColumnCount = Rsmd.getColumnCount();

        while ((ThisBatchCounter < BatchSize) & (!rs.isLast()))
        {
          // get next row
          rs.next();

          ThisBatchCounter++;
          tmpColumns = new String[ColumnCount];

          // create the array to transfer the columns into the DBRecord
          for (ColumnIdx = 0; ColumnIdx < ColumnCount; ColumnIdx++)
          {
            tmpColumns[ColumnIdx] = rs.getString(ColumnIdx + 1);
          }

          // create the record
          tmpRecord = new DBRecord(ColumnCount, tmpColumns, InputRecordNumber);

          // Call the user layer for any processing that needs to be done
          batchRecord = procValidRecord((IRecord) tmpRecord);

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
          // if so, clear down the out batch, so we don't keep filling the pipe
          PipeLog.warning("Pipe <"+ getSymbolicName() + "> discarded <" + Outbatch.size() + "> input records, because of pending abort.");
          Outbatch.clear();
        }

        // Keep track of the records
        updateRecordCount(transactionNumber,InputRecordNumber);
      }
      catch (SQLException Sex)
      {
        String Message = "Retrieve SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
        throw new ProcessingException(Message);
      }

      // See if we need to add a stream trailer record - this is done immediately
      // after the last real record of the stream
      try
      {
        // see the reason that we closed
        if (rs.isLast())
        {
          // we have finished
          InputStreamOpen = false;

          // Inject a stream header record into the stream
          tmpTrailer = new TrailerRecord();
          tmpTrailer.setStreamName(ORTransactionId);
          tmpTrailer.setTransactionNumber(transactionNumber);

          // Pass the header to the user layer for any processing that
          // needs to be done. To allow for purging in the case of record
          // compression, we allow multiple calls to procTrailer until the
          // trailer is returned
          batchRecord = procTrailer((IRecord)tmpTrailer);

          while (!(batchRecord instanceof TrailerRecord))
          {
            // the call the trailer returned a purged record. Add this
            // to the batch and fetch again
            Outbatch.add(batchRecord);
            batchRecord = procTrailer((IRecord)tmpTrailer);
          }

          Outbatch.add(tmpTrailer);

          // Notify the transaction layer that we have finished
          setTransactionFlushed(transactionNumber);

          // Close the connection
          // Connection will be closed after commit or rollback
          closeSelectStatement();
        }
      }
      catch (SQLException Sex)
      {
        String Message = "Close SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
        throw new ProcessingException(Message);
      }
    }

    return Outbatch;
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of overridable processing  functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * Allows any records to be purged at the end of a transaction
  *
  * @return The pending record
  */
  @Override
  public IRecord purgePendingRecord()
  {
    // default - do nothing
    return null;
  }

 /**
  * Get the transaction id for the transaction. Intended to be overwritten
  * in the case that you want another transaction ID format.
  *
  * @param transactionNumber The number of the transaction
  * @return The calculated transaction id
  */
  public String getTransactionID(int transactionNumber)
  {
    return ""+new Date().getTime();
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
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

    if (Command.equalsIgnoreCase(SERVICE_INIT_QUERY_KEY))
    {
      if (Init)
      {
        InitQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return InitQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_COUNT_QUERY_KEY))
    {
      if (Init)
      {
        CountQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return CountQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_SELECT_QUERY_KEY))
    {
      if (Init)
      {
        SelectQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return SelectQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_COMMIT_QUERY_KEY))
    {
      if (Init)
      {
        CommitQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return CommitQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_ROLLBACK_QUERY_KEY))
    {
      if (Init)
      {
        RollbackQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return RollbackQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_CONNECTION_TEST_KEY))
    {
      if (Init)
      {
        ValidateQuery = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return ValidateQuery;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DATASOURCE_KEY))
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
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DATASOURCE_KEY, ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_INIT_QUERY_KEY, ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_SELECT_QUERY_KEY,ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_COUNT_QUERY_KEY,ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_COMMIT_QUERY_KEY,ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_ROLLBACK_QUERY_KEY,ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_CONNECTION_TEST_KEY,ClientManager.PARAM_MANDATORY);
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction
  *
  * @param TransactionNumber
  * @return 0 if everything flushed OK, otherwise -1
  */
  @Override
  public int flushTransaction(int TransactionNumber)
  {
    try
    {
      // close the input stream
      closeStream();
    }
    catch (ProcessingException ex)
    {
      return -1;
    }

    return 0;
  }

 /**
  * Perform any processing that needs to be done when we are committing the
  * transaction
  *
  * @param transactionNumber The transaction to commit
  */
  @Override
  public void commitTransaction(int transactionNumber)
  {
    try
    {
      CommitStream(transactionNumber);
    }
    catch (ProcessingException ex)
    {
      Logger.getLogger(JDBCInputAdapter.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  /**
  * Perform any processing that needs to be done when we are rolling back the
  * transaction;
  *
  * @param transactionNumber The transaction to roll back
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    try
    {
      RollbackStream(transactionNumber);
    }
    catch (ProcessingException ex)
    {
      Logger.getLogger(JDBCInputAdapter.class.getName()).log(Level.SEVERE, null, ex);
    }
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
  // ----------------- Start of inherited IAdapter functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Close all statements and perform clean up
  */
  @Override
  public void cleanup()
  {
    PipeLog.debug("JDBCInputAdapter running cleanup");

    // Close the statements and connections
    closeInitStatement();
    closeCountStatement();
    closeSelectStatement();
    closeCommitRollbackStatement();
    closeConnection();

    super.cleanup();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Custom connection management functions -------------------
  // -----------------------------------------------------------------------------

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed
  */
  private void prepareInitStatement() throws ProcessingException
  {
    try
    {
      // Get the connection
      openConnection();

      // prepare the SQL for the TestStatement
      StmtInitQuery = JDBCcon.prepareStatement(InitQuery);
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> preparing query <" + InitQuery + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed
  */
  private void prepareCountStatement()
    throws ProcessingException
  {
    try
    {
      // Get the connection
      openConnection();

      // prepare the SQL for the TestStatement
      StmtCountQuery = JDBCcon.prepareStatement(CountQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> preparing query <" + CountQuery + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed
  */
  private void prepareSelectStatement()
    throws ProcessingException
  {
    try
    {
      // Get the connection
      openConnection();
      // prepare the SQL for the TestStatement
      StmtSelectQuery = JDBCcon.prepareStatement(SelectQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
      if (StmtSelectQuery.getMaxRows() > BatchSize)
      {
        String Message = "Input Adapter <" + getSymbolicName() + "> cannot get requested batch size <" + BatchSize + ">, setting to <" + StmtSelectQuery.getMaxRows() +">";
        PipeLog.warning(Message);
        StmtSelectQuery.setFetchSize(StmtSelectQuery.getMaxRows());
      }
      else
      {
        StmtSelectQuery.setFetchSize(BatchSize);
      }
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> preparing query <" + SelectQuery + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed
  */
  private void prepareCommitRollbackStatement()
    throws ProcessingException
  {
    try
    {
      // Get the connection
      openConnection();

      // prepare the SQL for the TestStatement
      if(CommitQuery == null || CommitQuery.isEmpty()){
          StmtCommitQuery = null;
      }else{
        StmtCommitQuery = JDBCcon.prepareStatement(CommitQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
      }
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> preparing query <" + CommitQuery + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }

    try
    {
      // prepare the SQL for the TestStatement
      if(RollbackQuery == null || RollbackQuery.isEmpty()){
          StmtRollbackQuery = null;
      }else{
        StmtRollbackQuery = JDBCcon.prepareStatement(RollbackQuery,
                                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_READ_ONLY);
      }
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> preparing query <" + RollbackQuery + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

 /**
  * CloseStatements closes the statements from the SQL expressions
  */
  public void closeCountStatement()
  {
    if (StmtCountQuery != null)
    {
      try
      {
        StmtCountQuery.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing query <" + CountQuery + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }

    // close the connection
    closeConnection();
  }

 /**
  * CloseStatements closes the statements from the SQL expressions
  */
  public void closeInitStatement()
  {
    // close all the connections and deallocate objects
    if (StmtInitQuery != null)
    {
      try
      {
        StmtInitQuery.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing query <" + InitQuery + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }

    // close the connection
    closeConnection();
  }

 /**
  * CloseStatements closes the statements from the SQL expressions
  */
  public void closeSelectStatement()
  {
    if (StmtSelectQuery != null)
    {
      try
      {
        StmtSelectQuery.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing query <" + SelectQuery + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }

    // close the connection
    closeConnection();
  }

 /**
  * CloseStatements closes the statements from the SQL expressions
  */
  public void closeCommitRollbackStatement()
  {
    if (StmtCommitQuery != null)
    {
      try
      {
        StmtCommitQuery.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing query <" + CommitQuery + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }

    if (StmtRollbackQuery != null)
    {
      try
      {
        StmtRollbackQuery.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing query <" + RollbackQuery + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }

    // close the connection
    closeConnection();
  }

  /**
  * Open the connection
  */
  public void openConnection()
  {
    try
    {
      // Get our connection, exception if it goes wrong
      if ((JDBCcon == null) || JDBCcon.isClosed())
      {
        JDBCcon = DBUtil.getConnection(dataSourceName);
      }
    }
    catch (SQLException Sex)
    {
      String Message = "SQL Exception in <" + getSymbolicName() + "> opening connection. Message = <" + Sex.getMessage() +">";
      handler.reportException(new ProcessingException(Message,Sex));
    }
    catch (InitializationException ie)
    {
      handler.reportException(ie);
    }
  }

  /**
  * Close down the connection
  */
  public void closeConnection()
  {
    if (JDBCcon != null)
    {
      try
      {
        JDBCcon.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception in <" + getSymbolicName() + "> closing connection. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ----------------- Stream opening and closing functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * getInputAvailable performs the count query to see the number of records that
  * are candidates for processing
  */
  private int getInputAvailable() throws ProcessingException
  {
    ResultSet Trs;
    String    WorkingResult;
    int       InputAvail = 0;

    try
    {
      // prepare the count query
      prepareCountStatement();

      if (StmtCountQuery.execute())
      {
        Trs = StmtCountQuery.getResultSet();

        if (Trs.next())
        {
          WorkingResult = Trs.getString(1);
          InputAvail = Integer.parseInt(WorkingResult);

          // Log what we have got
          if (InputAvail > 0)
          {
            StatsLog.info("Input  <" + getSymbolicName() + "> found <" + InputAvail + "> events for processing");
          }
        }

        Trs.close();
      }

      // close the statement
      closeCountStatement();
    }
    catch (SQLException Sex)
    {
      String Message = "Count SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }

    return InputAvail;
  }

  /**
  * assignInput performs the init query to mark the input records that we are
  * going to process. This means that any records that arrive after the
  * processing has started will not be included in this transaction, but instead
  * will have to wait for a later transaction
  */
  private void assignInput() throws ProcessingException
  {
    try
    {
      // prepare the statement
      prepareInitStatement();

      // Execute it
      StmtInitQuery.execute();

      // Close the statement
      closeInitStatement();
    }
    catch (SQLException Sex)
    {
      String Message = "Init SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

  /**
   * Closes down the input stream after all the input has been collected and
   * informs the transaction manager
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeStream()
    throws ProcessingException
  {
    // close down the result set now that we have read everything
    if (rs != null)
    {
      try
      {
        rs.close();
      }
      catch (SQLException Sex)
      {
        String Message = "SQL Exception closing resultset in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
        PipeLog.error(Message);
        throw new ProcessingException(Message);
      }
    }
  }

 /**
  * Commit stream performs the commit query to fix the data
  *
  * @param TransactionNumber The transaction number we are working on
  * @throws ProcessingException
  */
  public void CommitStream(int TransactionNumber) throws ProcessingException
  {
    try
    {
      // prepare the statement
      prepareCommitRollbackStatement();

      // deinit the records so that we don't have to read them ever again
      if(StmtCommitQuery != null){
        perfomCommit();
      }

      // Close down the connection and return to the pool
      closeCommitRollbackStatement();
    }
    catch (SQLException Sex)
    {
      String Message = "Commit SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
    }
  }

 /**
  * Rollback stream performs the rollback query to remove the data
  *
  * @param TransactionNumber The transaction number we are working on
  * @throws ProcessingException
  */
   public void RollbackStream(int TransactionNumber) throws ProcessingException
   {
     try
     {
      // prepare the statement
      prepareCommitRollbackStatement();

       // deinit the records so that we don't have to read them ever again
       if(StmtRollbackQuery != null){
         perfomRollback();
       }

      // Close down the connection and return to the pool
      closeCommitRollbackStatement();
     }
     catch (SQLException Sex)
     {
      String Message = "Rollback SQL Exception in <" + getSymbolicName() + ">. Message = <" + Sex.getMessage() +">";
      PipeLog.error(Message);
      throw new ProcessingException(Message);
     }
   }

  /**
   * Overridable commit block for allowing the addition of parameters
   *
   * @throws SQLException
   */
  public void perfomCommit() throws SQLException
  {
    StmtCommitQuery.execute();
  }

  /**
   * Overridable rollback block for allowing the addition of parameters
   *
   * @throws SQLException
   */
  public void perfomRollback() throws SQLException
  {
    StmtRollbackQuery.execute();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of custom initialisation functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * The InitQuery is the query that will be executed at the beginning of a
  * new stream of data. This is executed once, and should be used to prepare
  * data for extraction
  *
  * @param PipelineName The pipeline name we are working in
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initInitQuery(String PipelineName)
                       throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                          INIT_QUERY_KEY,
                                                          "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      PipeLog.error(
            "Initialisation statement not found from <" + INIT_QUERY_KEY +
            ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        INIT_QUERY_KEY +
                                        " property not found.");
    }

    return query;
  }

 /**
  * The SelectQuery is the query that will be executed to actually handle the
  * data. This will deliver a data set that contains the records to be
  * processed.
  *
  * @param PipelineName The pipeline name we are working in
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initSelectQuery(String PipelineName)
    throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                          SELECT_QUERY_KEY,
                                                          "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      PipeLog.error("Select statement not found from <" + SELECT_QUERY_KEY +
                ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        SELECT_QUERY_KEY +
                                        " property not found.");
    }

    return query;
  }

  /**
  * The CountQuery is the query that will be return the number of rows that
  * will be extracted during the SelectQuery. This is used to see if there is
  * work to be done. If the number of records is more than 0, then the Select
  * will be performed or not.
   *
  * @param PipelineName The pipeline name we are working in
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initCountQuery(String PipelineName)
    throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                          COUNT_QUERY_KEY,
                                                          "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      PipeLog.error("Count statement not found from <" + COUNT_QUERY_KEY + ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        COUNT_QUERY_KEY +
                                        " property not found.");
    }

    return query;
  }

 /**
  * The CommitQuery is used to undo any operations that were done during the
  * InitQuery, or to tidy up after the work has been done.
  *
  * @param PipelineName The pipeline name we are working in
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initCommitQuery(String PipelineName)
    throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                          COMMIT_QUERY_KEY,
                                                          "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      PipeLog.error(
            "Deinitialisation statement not found from <" +
            COMMIT_QUERY_KEY + ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        COMMIT_QUERY_KEY +
                                        " property not found.");
    }

    return query;
  }

 /**
  * The RollbackQuery is used to undo any operations that were done during the
  * InitQuery, or to tidy up after the work has been done.
  *
  * @param PipelineName The pipeline name we are working in
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initRollbackQuery(String PipelineName)
    throws InitializationException
  {
    String query;

    // Get the init statement from the properties
    query = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                          ROLLBACK_QUERY_KEY,
                                                          "None");

    if ((query == null) || query.equalsIgnoreCase("None"))
    {
      PipeLog.error(
            "Deinitialisation statement not found from <" +
            ROLLBACK_QUERY_KEY + ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        ROLLBACK_QUERY_KEY +
                                        " property not found.");
    }

    return query;
  }

 /**
  * Get the data source name from the properties
  *
  * @param PipelineName The pipeline name we are working in
  * @return The data source name
  * @throws OpenRate.exception.InitializationException
  */
  public String initDataSourceName(String PipelineName)
    throws InitializationException
  {
    String DSN;
    DSN = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(PipelineName, getSymbolicName(),
                                                        DATASOURCE_KEY,
                                                        "None");

    if ((DSN == null) || DSN.equalsIgnoreCase("None"))
    {
      PipeLog.error("Datasource name not found from <" + DATASOURCE_KEY + ">");
      throw new InitializationException("JDBCInputAdapter config error. " +
                                        DATASOURCE_KEY +
                                        " property not found.");
    }

    return DSN;
  }
}