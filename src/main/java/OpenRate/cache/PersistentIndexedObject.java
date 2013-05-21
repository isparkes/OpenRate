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

import OpenRate.audit.AuditUtils;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a cache implementing a persistent in-memory hash table, which must
 * be saved on shutdown or periodically.
 */
public class PersistentIndexedObject
     extends AbstractCache
  implements ICacheLoader,
             ICacheSaver,
             IEventInterface,
             ISyncPoint
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: PersistentIndexedObject.java,v $, $Revision: 1.44 $, $Date: 2013-05-13 18:12:11 $";

  /**
   * The name of the location we store the information in the persistent
   * storage. Can be a file name or a table name.
   */
  protected String CachePersistenceName = null;

 /**
  * This stores all the cacheable data. The DigitTree class is
  * a way of storing numeric values for a best match search.
  * The cost of a search is linear with the number of digits
  * stored in the search tree
  */
  protected HashMap<String, Object> ObjectList;

  // List of Services that this Client supports
  private final static String SERVICE_PERSIST = "Persist";
  private final static String SERVICE_PURGE = "Purge";
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

  // Variables for managing the sync points
  private int SyncStatus = 0;

  /**
   * Constructor. Create the persistent object cache.
   */
  public PersistentIndexedObject()
  {
    // Add the version map
    AuditUtils.getAuditUtils().buildHierarchyVersionMap(this.getClass());

    ObjectList = new HashMap<>(50000);
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
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    // Variable declarations
    boolean           foundDataSourceType = false;
    String            DataSourceType;

    // Get the source of the data to load
    setSymbolicName(CacheName);

    // Find the location of the  zone configuration file
    getFWLog().info("Starting Persistent Cache Loading <" + getSymbolicName() + ">");

    DataSourceType = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "DataSourceType",
                                                       "None");

    if (DataSourceType.equalsIgnoreCase("File"))
    {
      foundDataSourceType = true;
    }

    if (DataSourceType.equalsIgnoreCase("DB"))
    {
      getFWLog().error(
            "Persistent Cache does not yet support DB persistence sources.");
      throw new InitializationException("Persistent Cache does not yet support DB persistence sources");
    }

    if (!foundDataSourceType)
    {
      getFWLog().error(
            "DataSourceType for cache <" + getSymbolicName() +
            "> must be File, found <" + DataSourceType + ">");
      throw new InitializationException("DataSourceType for cache <" +
                                        getSymbolicName() +
                                        "> must be File, found <" +
                                        DataSourceType + ">");
    }

    // Get the configuration we are working on
    if (DataSourceType.equalsIgnoreCase("File"))
    {
      CachePersistenceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "DataFile",
                                                                     "None");

      if (CachePersistenceName.equals("None"))
      {
        getFWLog().error(
              "Data source file name not found for cache <" + getSymbolicName() +
              ">");
        throw new InitializationException("Data source file name not found for cache <" +
                                          getSymbolicName() + ">");
      }
      else
      {
        getFWLog().debug(
              "Found Persistence File Configuration <" +
              CachePersistenceName + ">");
      }
    }

    // perform the actual loading
    loadCacheObjectsFromFile();
  }

 /**
  * putObject inserts the given object into the cache. If the object already
  * exists, we update it overwriting the previous version
  *
  * @param RecordKey the hash key to use
  * @param ObjectToCache the object to store in the hash
  */
  public void putObject(String RecordKey, Object ObjectToCache)
  {
    ObjectList.put(RecordKey, ObjectToCache);
  }

 /**
  * getObject retrieves the given object from the cache, as defined by the
  * key
  *
  * @param RecordKey the hash key to use
  * @return Object the object stored in the hash, otherwise null if none found
  */
  public Object getObject(String RecordKey)
  {
    if (ObjectList.containsKey(RecordKey))
    {
      return ObjectList.get(RecordKey);
    }
    else
    {
      return null;
    }
  }

 /**
  * deleteObject removes the given object from the cache, as defined by the
  * key
  *
  * @param RecordKey the hash key to remove
  */
  public void deleteObject(String RecordKey)
  {
    if (ObjectList.containsKey(RecordKey))
    {
      ObjectList.remove(RecordKey);
    }
  }

 /**
  * See if an key exists in the cache
  *
  * @param ObjectKey The object key to find
  * @returns true if the object exists otherwise false
  */
  public boolean containsObjectKey(String RecordKey)
  {
    return ObjectList.containsKey(RecordKey);
  }

 /**
  * Get the key set for the cache, used for iterating over it
  *
  * @returns the object key set
  */
  public Set<String> getObjectKeySet()
  {
    return ObjectList.keySet();
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  */
  @Override
  public int getSyncStatus()
  {
    return SyncStatus;
  }

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  *
  * *** This is a stub function for the moment ***
  */
  @Override
  public void setSyncStatus(int newStatus)
  {
    if (newStatus == 2)
    {
      SyncStatus = 3;
    }
    else if (newStatus == 4)
    {
      SyncStatus = 5;
    }
    else
    {
      SyncStatus = newStatus;
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ICacheSaver functions -------------------
  // -----------------------------------------------------------------------------

 /**
  * Save the internal object store to the persistence target
  */
  @Override
  public void saveCache()
  {
    saveCacheObjectsToFile();
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
    //Register this Client
    ClientManager.registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    //ClientManager.registerClientService(getSymbolicName(), SERVICE_PERSIST, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PURGE, ClientManager.PARAM_DYNAMIC);
    //ClientManager.registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
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

    if (Command.equalsIgnoreCase(SERVICE_PURGE))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // Clear the persistence object
        ObjectList.clear();

        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("true"))
      {
        // do nothing
      }
      else if (Parameter.equals(""))
      {
        return "false";
      }
    }

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      return Integer.toString(ObjectList.size());
    }

    if (Command.equalsIgnoreCase(SERVICE_PERSIST))
    {
      //try
      //{
      //  if (DataSourceType.equalsIgnoreCase("DB"))
      //  {
      // Reload
      //PersistToFile();
      //  }
      //}
      //catch (InitializationException ex)
      //{
      //  FWLog.error("Command SERVICE_PERSIST not executed because of InitializationException thrown by loadData()", ex);
      //  return "Command not executed because of InitializationException thrown by loadData()";
      //}
      ResultCode = 0;
    }

    if (ResultCode == 0)
    {
      getFWLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood \n";
    }
  }

 /**
  * Save the object data to a file. This works with objects that are serializable
  * in the case that yours are not, you must overwrite this in an inherited
  * class.
  */
  public void saveCacheObjectsToFile()
  {
    FileOutputStream   outStream = null;
    ObjectOutputStream objOutStream;

    // Check to see if we need to open our output stream
    if (CachePersistenceName != null)
    {
      try
      {
        outStream = new FileOutputStream(CachePersistenceName);
      }
      catch (FileNotFoundException ex)
      {
        String Message = "File not found saving persistent objects";
        getFWLog().fatal(Message);
      }

      try
      {
        objOutStream = new ObjectOutputStream(outStream);

        if (objOutStream != null)
        {
          if (ObjectList instanceof Serializable)
          {
            objOutStream.writeObject(ObjectList);
            objOutStream.flush();
            objOutStream.close();
          }
          else
          {
            getFWLog().error("Class contains non-serializable data");
          }
        }
      }
      catch (IOException ex)
      {
        String Message = "IO Exception saving persistent objects";
        getFWLog().fatal(Message);
      }
    }
  }

 /**
  * Load the object data from a file. This works with objects that are serializable
  * in the case that yours are not, you must overwrite this in an inherited
  * class.
  */
  @SuppressWarnings("unchecked")
  public void loadCacheObjectsFromFile()
  {
    FileInputStream   inStream;
    ObjectInputStream objStream;

    try
    {
      inStream = new FileInputStream(CachePersistenceName);
    }
    catch (FileNotFoundException fnfe)
    {
      getFWLog().warning(
            "Persistent data file <" + CachePersistenceName +
            "> not found.");

      return;
    }

    try
    {
      objStream = new ObjectInputStream(inStream);
    }
    catch (IOException ex)
    {
      getFWLog().warning(
            "Persistent data file <" + CachePersistenceName +
            "> could not be opened.");

      return;
    }

    try
    {
      ObjectList = (HashMap<String, Object>)objStream.readObject();
    }
    catch (IOException ex)
    {
      getFWLog().warning(
            "Persistent data file <" + CachePersistenceName +
            "> could not be read.");
    }
    catch (ClassNotFoundException ex)
    {
        String Message = "Class not found loading persistent objects";
        getFWLog().fatal(Message);
    }
 }

 /**
  * Save the object data to a table. This works with objects that are serializable
  * in the case that yours are not, you must overwrite this in an inherited
  * class.
  */
  public void saveCacheObjectsToDB()
  {
    String objectKey;
    Object tmpObject;
    Iterator<String> objectIter = ObjectList.keySet().iterator();

    while (objectIter.hasNext())
    {
      objectKey = objectIter.next();
      tmpObject = ObjectList.get(objectKey);
      System.out.println(objectKey + ":" + tmpObject.toString());
    }
  }
}
