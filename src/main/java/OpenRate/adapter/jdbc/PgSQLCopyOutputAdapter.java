package OpenRate.adapter.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.lang.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.adapter.AbstractTransactionalOutputAdapter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.DBRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;

/**
 * Output Adapter module that uses COPY command to populate PostgreSQL database.
 * @author ddijak
 *
 */
public abstract class PgSQLCopyOutputAdapter
  extends AbstractTransactionalOutputAdapter
  implements IEventInterface
{  
  /**
   * Data holder 
   */
  protected ConcurrentHashMap<String, CopyOnWriteArrayList<String>> dataHolder;
  
  /**
   * The copy statement
   */
  protected String CopyStatement;
    
  /**
   * Partition identification string
   */
  protected String partitionIdent;
  
  /**
   * This is the name of the data source
   */
  protected String dataSourceName;
  
  // default partition name
  private static final String DEFAULT_PARTITON_NAME = "Default";
  
  // this is the connection from the connection pool that we are using
  private static final String DATASOURCE_KEY = "DataSource";

  // The SQL statements from the properties that are used to process records
  private static final String COPY_STMT_KEY = "CopyStatement";
  
  // Partition identification used to identify what needs to be replaced in copy statement
  private static final String PARTITION_IDENT_KEY = "PartitionIdent";

  // List of Services that this Client supports
  private final static String SERVICE_DATASOURCE_KEY = "DataSource";
  private final static String SERVICE_COPY_STMT_KEY = "CopyStatement";
  private final static String SERVICE_PARTITION_IDENT_KEY = "PartitionIdent";
  private final static String SERVICE_STATUS_KEY = "PrintStatus";

 /**
 * This is our connection object
 */
  protected Connection JDBCcon;
  
 /**
  * This is our CopyManager 
  */
  protected CopyManager cpManager;
  
  /**
   * Default constructor
   */
  public PgSQLCopyOutputAdapter()
  {
    super();
  }

 /**
  * Initialize the module. Called during pipeline creation.
  * Initialize the Logger, and load the SQL statements.
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
    registerClientManager();

    // Register ourself with the client manager
    setSymbolicName(ModuleName);
    
    // Get copy statement from properties
    ConfigHelper = initCopyStatement();
    processControlEvent(SERVICE_COPY_STMT_KEY, true, ConfigHelper);
    
    // Get partition identification from properties
    ConfigHelper = initPartitionIdentStatement();
    processControlEvent(SERVICE_PARTITION_IDENT_KEY, true, ConfigHelper);
    
    // The data source property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    ConfigHelper = initDataSourceName();
    processControlEvent(SERVICE_DATASOURCE_KEY, true, ConfigHelper);

    // prepare the data source - this does not open a connection
    if(DBUtil.initDataSource(dataSourceName) == null)
    {
      message = "Could not initialise DB connection <" + dataSourceName + "> to in module <" + getSymbolicName() + ">.";
      getPipeLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }
    
    
  }

 /**
  * Process the stream header. Get the file base name and open the transaction.
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException  
  */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException
  {
    // perform any parent processing first
    super.procHeader(r);
    // Initialize dataHolder 
    dataHolder = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>(2);
    
    return r;
  }

 /**
  * Prepare good records for writing to the defined output stream.
  * 
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException  
  */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException
  {
    
    DBRecord outRec;
    String recordPartitionIdent = null;
    Iterator<DBRecord> outRecIter;
    Collection<DBRecord> outRecCol = null;

    try
    {
      outRecCol = procValidRecord(r);      
      recordPartitionIdent = this.getPartitionIdent(r);
    }
    catch (ProcessingException pe)
    {
      // Pass the exception up
      passUpErrorMessageAbortTransaction("Processing exception preparing valid record",pe);
    }
    catch (ArrayIndexOutOfBoundsException aiex)
    {
      // Not good. Abort the transaction
      setErrorMessageAbortTransaction("Column Index preparing valid record", aiex);
    }
    catch (Exception ex)
    {
      // Not good. Abort the transaction
      setErrorMessageAbortTransaction("Unknown Exception preparing valid record", ex);
    }

    // Null return means "do not bother to process"
    if (outRecCol != null && CopyStatement != null)
    {
      outRecIter = outRecCol.iterator();
      try
      {
    	  String assignedPartition = null;
	      while (outRecIter.hasNext())
	      {
	        outRec = (DBRecord)outRecIter.next();	
	        if(recordPartitionIdent != null && !recordPartitionIdent.isEmpty())
	        {
	        	assignedPartition = recordPartitionIdent;	        	
	        } else
	        {
	        	assignedPartition = DEFAULT_PARTITON_NAME;
	        }
	        
	        if (dataHolder.containsKey(assignedPartition))
	        {
	        	dataHolder.get(assignedPartition).add(outRec.getDataString());
	        } else
	        {
	        	CopyOnWriteArrayList<String> newEntry = new CopyOnWriteArrayList<>();
	        	newEntry.add(outRec.getDataString());
	        	dataHolder.put(assignedPartition, newEntry);
	        }
	      }
      } catch (NullPointerException npe)
      {
    	  setErrorMessageAbortTransaction("Null value is not a vailid SQL statement", npe);
      }
    }

    return r;
  }

 /**
  * Prepare bad records for writing to the defined output stream.
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
   * This is called when a data record is encountered. You should do any normal
   * processing here. Note that the result is a collection for the case that we
   * have to re-expand after a record compression input adapter has done
   * compression on the input stream.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<DBRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<DBRecord> procErrorRecord(IRecord r) throws ProcessingException;
  
  
  /**
   * Perform data copy into database
   * @param transactionNumber
   * @return true if successful, false if not
   */
  public boolean performCopy(int transactionNumber) 
  {
	    try
	    {
    	    // Get connection  	
    		JDBCcon = DBUtil.getConnection(dataSourceName);
    		 
    		// Initialize copy manager
    		cpManager = JDBCcon.unwrap(PGConnection.class).getCopyAPI();
	    	
		    //###################################
		    // Start with copy steps	    
			for (String copyRecords : dataHolder.keySet()) 
			{
				// Prepare copy data				
				    byte[] CopyData = StringUtils.join(dataHolder.get(copyRecords), System.getProperty("line.separator")).getBytes();
					//  Start with COPY operation/s
					long numOfRowsEffected = cpManager.copyIn(this.prepareCopyStatement(copyRecords), new ByteArrayInputStream(CopyData));	
					getPipeLog().debug("Copy effected " + numOfRowsEffected + " rows in module <" + getSymbolicName() + ">");				
					
			}
	    }
		catch (InitializationException iex)
	    {
	    	// Not good. Abort the transaction
	    	setErrorMessageAbortTransaction("Error acquiring connection from DataSource", iex);
	    } catch (SQLException Sex) {
			// Not good. Abort the transaction
			setErrorMessageAbortTransaction("Error performing copy to database", Sex);
		
		} catch (IOException ioe) {
			// Not good. Abort the transaction
			setErrorMessageAbortTransaction("Error closing InputStream", ioe);
		} finally
		{
		    // Close the connection
			DBUtil.close(JDBCcon);
		}
	    
    // We have errors. Abort.	
	if (getExceptionHandler().hasError())
	{
	    return false;	
	}
	
	// Everything went well
	return true;
  }
    
  /**
   * Prepare copy statement
   * @param partition
   * @return copy statement
   */
  private String prepareCopyStatement(String partition)
  {
	  if(partitionIdent != null) 
	  { // Copy to different partition tables
		  return CopyStatement.replace(partitionIdent, partition);
	  } else
	  { // Copy to specific table
		  return CopyStatement;
	  }
  }
  
  /**
   * Returns table partition identification if set 
   * @param OutRec
   * @return table partition identification
   */
  protected abstract String getPartitionIdent(IRecord OutRec);
     
  // -----------------------------------------------------------------------------
  // ------------------ Custom connection management functions -------------------
  // -----------------------------------------------------------------------------

  /*
   * closeStream() is called by the pipeline when no more information comes
   * down it. We must perform a transaction state change here to FLUSHED
   */
  @Override
  public void closeStream(int TransactionNumber)
  {
     // Nothing at the moment
  }

 /**
  * Used to skip to the end of the stream in the case that the transaction is
  * aborted.
  *
  * @return True if the rest of the transaction was skipped otherwise false
  */
  @Override
  public boolean SkipRestOfStream()
  {
    return getTransactionAborted(getTransactionNumber());
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

    if (Command.equalsIgnoreCase(SERVICE_COPY_STMT_KEY))
    {
      if (Init)
      {
        CopyStatement = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return CopyStatement;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }
    
    if (Command.equalsIgnoreCase(SERVICE_PARTITION_IDENT_KEY))
    {
      if (Init)
      {
        partitionIdent = Parameter;
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return partitionIdent;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }
    
    if (Command.equalsIgnoreCase(SERVICE_STATUS_KEY))
    {      
      return "OK";
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DATASOURCE_KEY, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_COPY_STMT_KEY,ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PARTITION_IDENT_KEY,ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATUS_KEY,ClientManager.PARAM_DYNAMIC);
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

  /**
  * When a transaction is started, the transactional layer calls this method to
  * see if we have any reson to stop the transaction being started, and to do
  * any preparation work that may be necessary before we start.
  * 
  * @param transactionNumber The transaction to start
  */
  @Override
  public int startTransaction(int transactionNumber)
  {
    return 0;
  }

  /**
   * Perform any processing that needs to be done when we are committing the
   * transaction;
   * 
   * @param transactionNumber The transaction to commit
   */
   @Override
   public void commitTransaction(int transactionNumber)
   {
	 // no op
   }

   /**
   * Perform any processing that needs to be done when we are rolling back the
   * transaction;
   * 
   * @param transactionNumber The transaction to rollback
   */
   @Override
   public void rollbackTransaction(int transactionNumber)
   {
	   // Something went wrong, abort transaction
	   this.setTransactionAbort(transactionNumber);
   }
  
 /**
  * Perform any processing that needs to be done when we are flushing the
  * transaction;
  * 
  * @param transactionNumber The transaction to flush
  */
  @Override
  public int flushTransaction(int transactionNumber)
  {
    // close the input stream
    if (performCopy(transactionNumber))
    {
      return 0;
    }
    else
    {
      return -1;
    }
  }

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * Close down the statements we opened. Because the commit and rollback
  * statements are optional, we check if they have been defined before we ry
  * to close them.
  * 
  * @param transactionNumber The transaction we are working on
  */
  @Override
  public void closeTransaction(int transactionNumber)
  {	  	
	// Clear CopyManager
	cpManager = null;
      
	// Clear data holder
	dataHolder.clear();
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of custom initialisation functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialization of copy statement
  *
  * @return The copy statement string
  * @throws OpenRate.exception.InitializationException
  */
  public String initCopyStatement() throws InitializationException
  {
    String copyStmt;

    // Get the initialization statement from the properties
    copyStmt = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                   COPY_STMT_KEY,
                                                   "None");

    if ((copyStmt == null) || copyStmt.equalsIgnoreCase("None"))
    {
      message = "Output <" + getSymbolicName() + "> - Initialisation statement not found from <" + COPY_STMT_KEY + ">";
      getPipeLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    return copyStmt;
  }
  
  public String initPartitionIdentStatement() throws InitializationException
  {	  
	// Get the initialization statement from the properties
	  return PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
	                                                   PARTITION_IDENT_KEY,
	                                                   null);
  }
  
 /**
  * Get the data source name from the properties
  *
  * @return The query string
  * @throws OpenRate.exception.InitializationException
  */
  public String initDataSourceName() throws InitializationException
  {
    String DSN;
    DSN = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
                                                 DATASOURCE_KEY,
                                                 "None");

    if ((DSN == null) || DSN.equalsIgnoreCase("None"))
    {
      message = "Output <" + getSymbolicName() + "> - Datasource name not found from <" + DATASOURCE_KEY + ">";
      getPipeLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    return DSN;
  }
  
  /**
   * Report the error and abort current Transaction
   * @param message
   * @param err
   */
  private void setErrorMessageAbortTransaction(String errMessage, Throwable err)
  {
	  message = errMessage + " in module <"+ getSymbolicName() + ">. Message <" + err.getMessage()+ ">. Aborting transaction.";	 
	  getPipeLog().fatal(message);
	  
	  getExceptionHandler().reportException(new ProcessingException(message, err, getSymbolicName()));
	  setTransactionAbort(getTransactionNumber());
  }
  
  /**
   * Pass up the error and abort current Transaction
   * @param message
   * @param err
   */
  private void passUpErrorMessageAbortTransaction(String errMessage, Throwable err)
  {
	  message = errMessage + " in module <"+ getSymbolicName() + ">. Message <" + err.getMessage()+ ">. Aborting transaction.";	  
	  getPipeLog().fatal(message);
	  
	  getExceptionHandler().reportException(new ProcessingException(err, getSymbolicName()));
	  setTransactionAbort(getTransactionNumber());
  }
}