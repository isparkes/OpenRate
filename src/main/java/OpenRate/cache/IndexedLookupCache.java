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

package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This class implements an indexed in-memory lookup, using one or more hash
 * tables as keys into a hash map, which then returns an object indexed by
 * the key.
 *
 * The data can be loaded either from a file, or from a DB. The properties that
 * must be configured for these are slightly different in each case.
 *
 * Loading from a file:
 * --------------------
 *   Define "DataSourecType" as "File"
 *   Define "DataFile" to point to the (relative or absolute) location of the
 *     file to load
 *   Define "ObjectFields" as the number of fields to expect from the file
 *   Define "IndexFields" as the number of fields to index
 *   For each of the index fields that are defined it is necessary to define the
 *     index in the data record to use
 *
 * CacheableClass.0.ClassName=OpenRate.cache.IndexedLookupCache
 * CacheableClass.0.ModuleName=IMSICache
 * IMSICache.DataSourceType=File
 * IMSICache.DataFile=ConfigData/Router/IMSIMapFile.dat
 * IMSICache.ObjectFields=5
 * IMSICache.IndexFields=2
 * IMSICache.ObjectField.0=0
 * IMSICache.ObjectField.1=2
 *
 * This example will load a record from a file, where 5 fields are expected, and
 * an index will be created on field 1 and field 3
 *
 * Loading from a DB:
 * ------------------
 *   Define "DataSourecType" as "DB"
 *   Define "DataSource" to point to the data source name to load from
 *   Define "SelectStatement" to return the data you wish to retrieve
 *   Define "ObjectFields" as the number of fields to expect from the file
 *   Define "IndexFields" as the number of fields to index
 *   For each of the index fields that are defined it is necessary to define the
 *   index in the data record to use
 *
 * CacheableClass.0.ClassName=OpenRate.cache.IndexedLookupCache
 * CacheableClass.0.ModuleName=IMSICache
 * IMSICache.DataSourceType=DB
 * IMSICache.DataSource=LookupDataSource
 * IMSICache.SelectStatement=select IMSI,AccountNumber,MSISDN from IMSI_tab
 * IMSICache.ObjectFields=5
 * IMSICache.IndexFields=2
 * IMSICache.ObjectField.0=0
 * IMSICache.ObjectField.1=2
 */
public class IndexedLookupCache
     extends AbstractSyncLoaderCache
{
  /**
   * This stores the index to all the groups.
   */
  protected HashMap<String, String[]> ObjectCache;

  /**
   * Object ID generator
   */
  protected int ObjectID = 0;

  /**
   * These are the hashes that form the indexes
   */
  protected ArrayList<HashMap<String, String>> IndexList;

  /**
   * This is the form factor of the key table
   */
  protected int KeyFormFactor;

  /**
   * This is the form factor of the object fields we will load
   */
  protected int ObjectFields;

  /**
   * This is the list of the fields we are going to use as indexes
   */
  protected ArrayList<Integer> KeyFieldList;

  // List of Services that this Client supports
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

  /**
   * The default return when there is no match
   */
  public static final String NO_INDEXED_MATCH = "NOMATCH";

 /**
  * Creates a new instance of the Indexed Match Cache. The Cache contains all
  * of the Objects that are later cached. The lookup is performed using the
  * indexes that created at loading time.
  */
  public IndexedLookupCache()
  {
    super();

    ObjectCache = new HashMap<>(5000);
    IndexList = new ArrayList<>();
    KeyFieldList = new ArrayList<>();
  }

// -----------------------------------------------------------------------------
// ----------------- Start of overridable Plug In functions --------------------
// -----------------------------------------------------------------------------
 /**
  * The method allows the implementation class the possibility to manipulate
  * or validate the key fields before they are stored
  *
  * @param ObjectKeyFields The key fields to validate or manipulate
  * @return the modified or checked key fields
  * @throws InitializationException
  */
  public String[] validateKeyFields(String[] ObjectKeyFields) throws InitializationException
  {
    // Pass through - override to change
    return ObjectKeyFields;
  }

 /**
  * The method allows the implementation class the possibility to manipulate
  * or validate the key fields before they are stored
  *
  * @param ObjectSplitFields The fiels that we are to check
  * @return the modified or checked key fields
  * @throws InitializationException
  */
  public String[] validateMapFields(String[] ObjectSplitFields) throws InitializationException
  {
    // Pass through - override to change
    return ObjectSplitFields;
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    // Variable definitions
    String tmpValue;
    int IndexFields = 0;
    int tmpIndexField;
    int i;

    // Perform the specific initialisation before the base initialisation
    // Get the index and object field form factors
    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                         CacheName,
                                                         "ObjectFields",
                                                         "None");

    if (tmpValue.equals("None"))
    {
      // No object field count found
      message = "ObjectFields entry for cache <" + getSymbolicName() + "> not found.";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      try
      {
        ObjectFields = Integer.parseInt(tmpValue);
      }
      catch(NumberFormatException nfe)
      {
        // The object fields value was not numeric
        message = "ObjectFields entry for cache <" + getSymbolicName() +
                         "> not numeric. Found value <" + tmpValue + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }

    // Get the source of the data to load
    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                     CacheName,
                                                     "IndexFields",
                                                     "None");
    if (tmpValue.equals("None"))
    {
      // Could not find the index fields group
      message = "IndexFields entry for cache <" + getSymbolicName() + "> not found.";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      try
      {
        IndexFields = Integer.parseInt(tmpValue);
      }
      catch(NumberFormatException nfe)
      {
      // The index fields group was not numeric
        message = "IndexFields entry for cache <" + getSymbolicName() + "> not numeric. Found value <" + tmpValue + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }

    // Passed verification, move to the active variable
    KeyFormFactor = IndexFields;

    // Check that we have sensible values for the index and object field values
    if (ObjectFields < 2)
    {
      message = "ObjectFields entry for cache <" + getSymbolicName() +
                       "> must be greater than 1. Found value <" + ObjectFields + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    if (IndexFields < 1)
    {
      message = "IndexFields entry for cache <" + getSymbolicName() +
                       "> must be greater than 0. Found value <" + IndexFields + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    if (IndexFields > ObjectFields)
    {
      message =  "IndexFields entry for cache <" + getSymbolicName() +
                        "> must be less than or equal to greater than ObjectFields.";
      throw new InitializationException(message,getSymbolicName());
    }

    // Now get the index field entries
    for (i = 0 ; i < IndexFields ; i++)
    {
      // Get the source of the data to load
      tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "IndexField.Index" + String.valueOf(i),
                                                       "None");

      if (tmpValue.equals("None"))
      {
        // We can't find the index field we were told exists
        message = "IndexField <Index" + i + "> entry for cache <" + getSymbolicName() + "> not found.";

        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        try
        {
          tmpIndexField = Integer.parseInt(tmpValue);
          KeyFieldList.add(tmpIndexField);

          // Add in the index object
          IndexList.add(new HashMap<String, String>(5000));
        }
        catch(NumberFormatException nfe)
        {
          message = "IndexFields entry for cache <" + getSymbolicName() +
                           "> not numeric. Found value <" + tmpValue + ">";
          throw new InitializationException(message,getSymbolicName());
        }
      }
    }

    // Perform the base initialisation
    super.loadCache(ResourceName, CacheName);
 }

  /**
   * Add an object into the Object Cache, creating the indexes as required.
   *
   * @param inputKeys The keys to add to the map
   * @param inputResult The resutls array
   * @throws InitializationException
   */
  public void addEntry(String[] inputKeys, String[] inputResult)
    throws InitializationException
  {
    HashMap<String, String> tmpIndex;
    int     Index;
    String  tmpObjectID;
    boolean AddedOK = true;
    tmpObjectID = String.valueOf(ObjectID);
    String[] Keys;
    String[] Result;

    // Validate the list of all fields we got
    Result = validateMapFields(inputResult);

    // Validate the key fields we got
    Keys = validateKeyFields(inputKeys);

    // Add the keys
    for (Index = 0; Index < Keys.length; Index++)
    {
      tmpIndex = IndexList.get(Index);

      if (!tmpIndex.containsKey(Keys[Index]))
      {
        tmpIndex.put(Keys[Index], tmpObjectID);
      }
      else
      {
        OpenRate.getOpenRateFrameworkLog().error("Cache <" + getSymbolicName() +
              "> index <" + Index + "> already contains value <" +
              Keys[Index] + ">");
        AddedOK = false;
      }
    }

    // Create the new object to the cache
    if (AddedOK)
    {
      ObjectCache.put(tmpObjectID, Result);
      ObjectID++;
    }
  }

  /**
  * Get an object from the Object Cache, using any of the keys available
   * @param Index
   * @param Key
   * @return
   * @throws ProcessingException
   */
  public String[] getEntry(int Index, String Key)
                  throws ProcessingException
  {
    String[] tmpResult = null;
    HashMap<String, String>  tmpIndex;
    String   tmpObjectID;

    if (Index > KeyFormFactor)
    {
      message = "ObjectCache does not contain a key with index <" + Index +
                       "> in module <" + getSymbolicName() + ">";
      throw new ProcessingException(message,getSymbolicName());
    }

    // Get the Index
    tmpIndex = IndexList.get(Index);

    if (tmpIndex.containsKey(Key))
    {
      tmpObjectID = tmpIndex.get(Key);
      tmpResult = ObjectCache.get(tmpObjectID);
    }

    return tmpResult;
  }

 /**
  * Load the data from the defined file
  */
  @Override
  public void loadDataFromFile()
                        throws InitializationException
  {
    // Variable declarations
    int            ObjectLinesLoaded = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       ObjectSplitFields;
    String[]       ObjectKeyFields;
    Object         tmpKeyIndexObj;
    int            Index;
    int            tmpKeyIndex;

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(CacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
      message = "Application is not able to read file : <" + CacheDataFile +
                       "> in module <" + getSymbolicName() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
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
          // Comment line or whitespace line, ignore
        }
        else
        {
          ObjectLinesLoaded++;
          ObjectSplitFields = tmpFileRecord.split(";", ObjectFields);

          if (ObjectSplitFields.length != ObjectFields)
          {
            message = "Line <" + ObjectLinesLoaded +
                             "> does not conform to defined form factor of <" +
                             ObjectFields + "> in module <" + getSymbolicName() + ">";
            throw new InitializationException(message,getSymbolicName());
          }

          // Create the Index List
          ObjectKeyFields = new String[KeyFormFactor];

          for (Index = 0; Index < KeyFormFactor; Index++)
          {
            tmpKeyIndexObj = KeyFieldList.get(Index);
            tmpKeyIndex = (Integer)tmpKeyIndexObj;
            ObjectKeyFields[Index] = ObjectSplitFields[tmpKeyIndex];
          }

          // Add it
          addEntry(ObjectKeyFields, ObjectSplitFields);
        }
      }
    }
    catch (IOException ex)
    {
      message =  "Error reading input file <" + CacheDataFile +
                        "> in record <" + ObjectLinesLoaded + "> in module <" +
                        getSymbolicName() + ">. IO Error.";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      message = "Error reading input file <" + CacheDataFile +
                       "> in record <" + ObjectLinesLoaded + "> in module <" +
                       getSymbolicName() + ">. Malformed Record.";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        message = "Error closing input file <" + CacheDataFile +
                         "> in module <" + getSymbolicName() + ">";
        throw new InitializationException(message,ex,getSymbolicName());
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Indexed Match Data Loading completed. <" + ObjectLinesLoaded +
          "> configuration lines loaded from <" +
          CacheDataFile + ">");
  }

 /**
  * Load the data from the defined Data Source
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromDB() throws InitializationException
  {
    Object   tmpKeyIndexObj;
    String[] ObjectKeyFields;
    Integer  tmpKeyIndex;
    int      Index;
    String[] ObjectSplitFields;
    int      ObjectLinesLoaded = 0;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Indexed Lookup Cache Loading from DB");

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
      message = "Error performing SQL for retieving Indexed Match data in module <" +
                       getSymbolicName() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        ObjectLinesLoaded++;
        ObjectSplitFields = new String[ObjectFields];

        for (Index = 0; Index < ObjectFields; Index++)
        {
          ObjectSplitFields[Index] = mrs.getString(Index + 1);
        }

        // Create the Index List
        ObjectKeyFields = new String[KeyFormFactor];

        for (Index = 0; Index < KeyFormFactor; Index++)
        {
          tmpKeyIndexObj = KeyFieldList.get(Index);
          tmpKeyIndex = (Integer)tmpKeyIndexObj;
          ObjectKeyFields[Index] = ObjectSplitFields[tmpKeyIndex];
        }

        // Add the map
        addEntry(ObjectKeyFields, ObjectSplitFields);
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Search Map Data for <" +
            cacheDataSourceName + "> in module <" + getSymbolicName() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
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
      message = "Error closing Search Map Data connection for <" +
                       cacheDataSourceName + "> in module <" +
                       getSymbolicName() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Indexed Match Data Loading completed. <" + ObjectLinesLoaded +
          "> configuration lines loaded from <" +
          cacheDataSourceName + ">");
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
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      return Integer.toString(ObjectCache.size());
    }

    if (ResultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command,Init,Parameter);
    }
  }

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    Iterator<HashMap<String, String>> indexIter;
    HashMap<String, String> tmpIndex;

    // Clear out the object cache
    ObjectCache.clear();

    // iterate through the hash indexes, clearing them, but leaving the
    // structure intact
    indexIter = IndexList.iterator();

    while (indexIter.hasNext())
    {
      tmpIndex = indexIter.next();
      tmpIndex.clear();
    }
  }
}
