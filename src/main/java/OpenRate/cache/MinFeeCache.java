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
package OpenRate.cache;

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
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: MinFeeCache.java,v $, $Revision: 1.19 $, $Date: 2013-05-13 18:12:10 $";

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
    getFWLog().info("Starting MinFeeCache Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(CacheDataFile));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      getFWLog().error(
            "Application is not able to read file : <" +
            CacheDataFile + ">");
      throw new InitializationException("Application is not able to read file: <" +
                                        CacheDataFile + ">",
                                        exFileNotFound);
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
      getFWLog().fatal(
            "Error reading input file <" + CacheDataFile +
            "> in record <" + dataLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      getFWLog().fatal(
            "Error reading input file <" + CacheDataFile +
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
        getFWLog().error("Error closing input file <" + CacheDataFile +
                  ">", ex);
      }
    }

    getFWLog().info(
          "MinFeeCache Cache Data Loading completed. " + dataLoaded +
          " configuration lines loaded from <" + CacheDataFile +
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
    getFWLog().info("Starting MinFeeCache Cache Loading from DB");

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
      getFWLog().fatal("Error performing SQL for retieving MinFeeCache Cache data");
      throw new InitializationException("Connection error. Error retieving MinFeeCache Cache data");
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
      getFWLog().fatal(
            "Error opening Data for <" + cacheDataSourceName +
            ">");
      throw new InitializationException("Error opening Data for <" +
                                        cacheDataSourceName + ">", ex);
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtCacheDataSelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      getFWLog().fatal(
            "Error closing Data connection for <" +
            cacheDataSourceName + ">");
      throw new InitializationException("Error closing Data connection for <" +
                                        cacheDataSourceName + ">");
    }

    getFWLog().info(
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
    throw new InitializationException("Not implemented yet");
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

