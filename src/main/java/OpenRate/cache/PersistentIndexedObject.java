
package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a cache implementing a persistent in-memory hash table, which must be
 * saved on shutdown or periodically.
 */
public class PersistentIndexedObject
        extends AbstractCache
        implements ICacheLoader,
        ICacheSaver,
        IEventInterface,
        ISyncPoint {

  /**
   * The name of the location we store the information in the persistent
   * storage. Can be a file name or a table name.
   */
  protected String CachePersistenceName = null;

  /**
   * This stores all the cacheable data. The DigitTree class is a way of storing
   * numeric values for a best match search. The cost of a search is linear with
   * the number of digits stored in the search tree
   */
  protected HashMap<String, Object> ObjectList;

  // List of Services that this Client supports
  private final static String SERVICE_PERSIST = "Persist";
  private final static String SERVICE_PURGE = "Purge";
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";
  private final static String SERVICE_DUMP_OBJECTS = "DumpObjects";
  private final static String SERVICE_INITIAL_HASH_SIZE = "InitialHashSize";
  private final static String DEFAULT_INITIAL_HASH_SIZE = "50000";
  private final static String SERVICE_REMOVE_KEY = "RemoveKey";

  // Variables for managing the sync points
  private int SyncStatus = 0;

  // Variable holding the initial hash size
  private int initialHashSize;

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------
  /**
   * loadCache is called automatically on startup of the cache factory, as a
   * result of implementing the CacheLoader interface. This should be used to
   * load any data that needs loading, and to set up variables.
   *
   * @param ResourceName The resource name we are loading for
   * @param CacheName The cache name we are loading for
   * @throws InitializationException
   */
  @Override
  public void loadCache(String ResourceName, String CacheName)
          throws InitializationException {
    // Variable declarations
    boolean foundDataSourceType = false;
    String DataSourceType;

    // Get the source of the data to load
    setSymbolicName(CacheName);

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Persistent Cache Loading <" + getSymbolicName() + ">");

    DataSourceType = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "DataSourceType",
            "None");

    if (DataSourceType.equalsIgnoreCase("File")) {
      foundDataSourceType = true;
    }

    if (DataSourceType.equalsIgnoreCase("DB")) {
      message = "Persistent Cache does not yet support DB persistence sources";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    if (!foundDataSourceType) {
      message = "DataSourceType for cache <" + getSymbolicName()
              + "> must be File, found <" + DataSourceType + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Get the configuration we are working on
    if (DataSourceType.equalsIgnoreCase("File")) {
      CachePersistenceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "DataFile",
              "None");

      if (CachePersistenceName.equals("None")) {
        message = "Data source file name not found for cache <" + getSymbolicName() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      } else {
        OpenRate.getOpenRateFrameworkLog().debug(
                "Found Persistence File Configuration <"
                + CachePersistenceName + ">");
      }
    }

    // Get the initial hash size
    String tmpInitialSize = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            SERVICE_INITIAL_HASH_SIZE,
            DEFAULT_INITIAL_HASH_SIZE);

    try {
      initialHashSize = Integer.valueOf(tmpInitialSize);
    } catch (NumberFormatException ex) {
      message = "Expected a numeric value for <" + SERVICE_INITIAL_HASH_SIZE + "> in cache <" + getSymbolicName() + ">, but got <" + tmpInitialSize + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Initialise the object cache
    ObjectList = new HashMap<>(initialHashSize);

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
  public void putObject(String RecordKey, Object ObjectToCache) {
    ObjectList.put(RecordKey, ObjectToCache);
  }

  /**
   * getObject retrieves the given object from the cache, as defined by the key
   *
   * @param RecordKey the hash key to use
   * @return Object the object stored in the hash, otherwise null if none found
   */
  public Object getObject(String RecordKey) {
    if (ObjectList.containsKey(RecordKey)) {
      return ObjectList.get(RecordKey);
    } else {
      return null;
    }
  }

  /**
   * deleteObject removes the given object from the cache, as defined by the key
   *
   * @param RecordKey the hash key to remove
   */
  public void deleteObject(String RecordKey) {
    if (ObjectList.containsKey(RecordKey)) {
      ObjectList.remove(RecordKey);
    }
  }

  /**
   * See if an key exists in the cache
   *
   * @param recordKey The object key to find
   * @return true if the object exists otherwise false
   */
  public boolean containsObjectKey(String recordKey) {
    return ObjectList.containsKey(recordKey);
  }

  /**
   * Get the key set for the cache, used for iterating over it
   *
   * @return the object key set
   */
  public Set<String> getObjectKeySet() {
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
  public int getSyncStatus() {
    return SyncStatus;
  }

  /**
   * This is used for the pipeline synchronisation. See the description in the
   * OpenRate framework module to understand how this works.
   *
   * *** This is a stub function for the moment ***
   */
  @Override
  public void setSyncStatus(int newStatus) {
    if (newStatus == 2) {
      SyncStatus = 3;
    } else if (newStatus == 4) {
      SyncStatus = 5;
    } else {
      SyncStatus = newStatus;
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ICacheSaver functions -------------------
  // -----------------------------------------------------------------------------
  /**
   * Save the internal object store to the persistence target
   *
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public void saveCache() throws ProcessingException {
    saveCacheObjectsToFile();
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * registerClientManager registers the client module to the ClientManager
   * class which manages all the client modules available in this OpenRate
   * Application.
   *
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
    //Register this Client
    ClientManager.getClientManager().registerClient("Resource", getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PURGE, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_INITIAL_HASH_SIZE, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DUMP_OBJECTS, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_REMOVE_KEY, ClientManager.PARAM_DYNAMIC);
  }

  /**
   * processControlEvent is the method that will be called when an event is
   * received for a module that has registered itself as a client of the
   * External Control Interface
   *
   * @param Command - command that is understand by the client module
   * @param Init - we are performing initial configuration if true
   * @param Parameter - parameter for the command
   * @return The result string of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init,
          String Parameter) {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_PURGE)) {
      if (Parameter.equalsIgnoreCase("true")) {
        // Clear the persistence object
        ObjectList.clear();

        ResultCode = 0;
      } else if (Parameter.isEmpty()) {
        return "false";
      } else {
        return getSymbolicName() + ":" + SERVICE_PURGE + "=true to purge cache";
      }
    }

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT)) {
      return Integer.toString(ObjectList.size());
    }

    // Return the number initial size of the hash
    if (Command.equalsIgnoreCase(SERVICE_INITIAL_HASH_SIZE)) {
      return Integer.toString(initialHashSize);
    }

    if (Command.equalsIgnoreCase(SERVICE_DUMP_OBJECTS)) {
      if (Parameter.equalsIgnoreCase("true")) {
        // Clear the persistence object
        dumpObjects();

        ResultCode = 0;
      } else if (Parameter.isEmpty()) {
        return "false";
      } else {
        return getSymbolicName() + ":" + SERVICE_DUMP_OBJECTS + "=true to dump stored objects";
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_PERSIST)) {
      try {
        saveCache();
      } catch (ProcessingException ex) {
        OpenRate.getOpenRateFrameworkLog().error("Command SERVICE_PERSIST not executed because of Exception thrown by saveCache()", ex);
        return "Command not executed because of Exception thrown by saveCache()";
      }
      ResultCode = 0;
    }

    if (Command.equalsIgnoreCase(SERVICE_REMOVE_KEY)) {
      if (!Parameter.isEmpty()) {

        if (ObjectList.containsKey(Parameter)) {
          ObjectList.remove(Parameter);
        } else {
          return "cound not find key " + Parameter + "\n";
        }

        ResultCode = 0;
      } else if (Parameter.isEmpty()) {
        return "false";
      } else {
        return getSymbolicName() + ":" + SERVICE_DUMP_OBJECTS + "=true to dump stored objects";
      }
    }

    if (ResultCode == 0) {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    } else {
      return "Command Not Understood\n";
    }
  }

  /**
   * Save the object data to a file. This works with objects that are
   * serializable in the case that yours are not, you must overwrite this in an
   * inherited class.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void saveCacheObjectsToFile() throws ProcessingException {
    FileOutputStream outStream = null;
    ObjectOutputStream objOutStream;

    // Check to see if we need to open our output stream
    if (CachePersistenceName != null) {
      try {
        outStream = new FileOutputStream(CachePersistenceName);
      } catch (FileNotFoundException ex) {
        message = "File not found saving persistent objects";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
      }

      try {
        objOutStream = new ObjectOutputStream(outStream);
        if (ObjectList instanceof Serializable) {
          objOutStream.writeObject(ObjectList);
          objOutStream.flush();
          objOutStream.close();
        } else {
          OpenRate.getOpenRateFrameworkLog().error("Class contains non-serializable data");
        }
      } catch (IOException ex) {
        message = "IO Exception saving persistent objects";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
      }
    }
  }

  /**
   * Load the object data from a file. This works with objects that are
   * serializable in the case that yours are not, you must overwrite this in an
   * inherited class.
   */
  @SuppressWarnings("unchecked")
  public void loadCacheObjectsFromFile() {
    FileInputStream inStream;
    ObjectInputStream objStream;

    try {
      inStream = new FileInputStream(CachePersistenceName);
    } catch (FileNotFoundException ex) {
      OpenRate.getOpenRateFrameworkLog().warning(
              "Persistent data file <" + CachePersistenceName
              + "> not found.");

      return;
    }

    try {
      objStream = new ObjectInputStream(inStream);
    } catch (IOException ex) {
      OpenRate.getOpenRateFrameworkLog().warning(
              "Persistent data file <" + CachePersistenceName
              + "> could not be opened.");

      return;
    }

    try {
      ObjectList = (HashMap<String, Object>) objStream.readObject();
    } catch (IOException ex) {
      OpenRate.getOpenRateFrameworkLog().warning(
              "Persistent data file <" + CachePersistenceName
              + "> could not be read.");
    } catch (ClassNotFoundException ex) {
      message = "Class not found loading persistent objects";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
    }
  }

  /**
   * Dump the internal objects. The implementation class is responsible for
   * formatting the objects.
   */
  public void dumpObjects() {
    String objectKey;
    Object tmpObject;

    String filename = "Dump-" + getSymbolicName() + "-All-" + Calendar.getInstance().getTimeInMillis() + ".dump";
    OpenRate.getOpenRateFrameworkLog().info("Dumping PersistentIndexedObject data to file <" + filename + ">");

    try (BufferedWriter outFile = new BufferedWriter(new FileWriter(filename))) {
      outFile.write("# Balance data dump file\n");
      Iterator<String> objectIter = ObjectList.keySet().iterator();

      while (objectIter.hasNext()) {
        objectKey = objectIter.next();
        tmpObject = ObjectList.get(objectKey);
        String fileLine = formatObject(objectKey, tmpObject);

        outFile.write(fileLine + "\n");
      }

      outFile.flush();
      outFile.close();
    } catch (IOException ex) {
      message = "IO Error dumping data in cache <" + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message, ex);
    }
  }

  /**
   * Format the object for outputting to the dump file. Because we cannot know
   * in advance how the Object will be stored, we delegate the formatting to the
   * implementation class.
   *
   * @param key the key to format
   * @param object The object to format
   * @return The formatted object
   */
  public String formatObject(String key, Object object) {
    OpenRate.getOpenRateFrameworkLog().warning("Using default formatter, override 'formatObject' method of " + getSymbolicName() + " for a better output");
    return key + ":" + object.toString();
  }

  /**
   * Save the object data to a table. This works with objects that are
   * serializable in the case that yours are not, you must overwrite this in an
   * inherited class.
   */
  public void saveCacheObjectsToDB() {
    String objectKey;
    Object tmpObject;
    Iterator<String> objectIter = ObjectList.keySet().iterator();

    while (objectIter.hasNext()) {
      objectKey = objectIter.next();
      tmpObject = ObjectList.get(objectKey);
      System.out.println(objectKey + ":" + tmpObject.toString());
    }
  }
}
