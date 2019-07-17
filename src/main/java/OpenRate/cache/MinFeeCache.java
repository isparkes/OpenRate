
package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * This class implements a Minimum Fee Cache
 */
public class MinFeeCache
     extends AbstractSyncLoaderCache
{
 /**
  * This stores all the cacheable data
  */
  protected HashMap<String, String> MinFeeCache;


/** Constructor
  * Creates a new instance of the Cache.
  */
  public MinFeeCache()
  {
    super();

    MinFeeCache = new HashMap<>(10);
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Load the data from the defined file
  */
  @Override
  public void loadDataFromFile()
                        throws InitializationException
  {
    // Variable declarations
    int            dataLoaded = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       dataFields;
    String         tmpMinCost;
    String         tmpName;
    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting MinFeeCache Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
      OpenRate.getOpenRateFrameworkLog().error(
            "Application is not able to read file : <" +
            cacheDataFile + ">");
      throw new InitializationException("Application is not able to read file: <" +
                                        cacheDataFile + ">",
                                        ex,
                                        getSymbolicName());
    }

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, ignore
        }
        else
        {
          dataLoaded++;
          dataFields = tmpFileRecord.split(";");

          // Prepare and add the line
          tmpName = dataFields[0];
          tmpMinCost = dataFields[1];

         addEntry(tmpName, tmpMinCost);
        }
      }
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + dataLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + dataLoaded + ">. Malformed Record.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + cacheDataFile +
                  ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "MinFeeCache Cache Data Loading completed. " + dataLoaded +
          " configuration lines loaded from <" + cacheDataFile +
          ">");
  }

 /**
  * Load the data from the defined Data Source
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int     dataLoaded = 0;
    String         tmpMinCost;
   String         tmpName;

   // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting MinFeeCache Cache Loading from DB");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try
    {
      mrs = StmtCacheDataSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving MinFeeCache Cache data";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        dataLoaded++;
        tmpName  = mrs.getString(1);

       tmpMinCost = mrs.getString(2);

        // Add it
        addEntry(tmpName, tmpMinCost);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    DBUtil.close(mrs);
    DBUtil.close(StmtCacheDataSelectQuery);
    DBUtil.close(JDBCcon);

    OpenRate.getOpenRateFrameworkLog().info(
          "Data Loading completed. " + dataLoaded +
          " configuration lines loaded from <" + cacheDataSourceName +
          ">");
  }

 /**
  * Load the data from the defined Data Source Method
  */
  @Override
  public void loadDataFromMethod()
                      throws InitializationException
  {
    throw new InitializationException("Not implemented yet",getSymbolicName());
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Add a value into the Cache
  *
  * @param name The name of the entry to add
  * @param value The value to add
  */
  public void addEntry(String name, String value)
  {
    MinFeeCache.put(name, value);
  }

 /**
  * Get a value from the Cache.
 *
  * @param name The name of the entry to recover
  * @return The returned value
  */
  public String getEntry(String name)
  {
    return MinFeeCache.get(name);
  }

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    MinFeeCache.clear();
  }
}

