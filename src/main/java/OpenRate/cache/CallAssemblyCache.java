

package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.lang.AssemblyCtx;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Call assembly cache used to perform call assembly for voice or data
 * partials
 */
public class CallAssemblyCache
  extends PersistentIndexedObject
{
  // List of Services that this Client supports
  private final static String SERVICE_STORE_LIMIT = "StoreLimit";

  /**
   * this is the number of days history that we keep
   */
  protected int StoreLimit;

 /** Constructor
  * Audit Logging Info
  */

  public CallAssemblyCache()
  {
    super();
  }

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @param ResourceName The name of the resource name
  * @param CacheName The name of the cache
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    String tmpStoreLimit;

    // do the proceeding stuff first
    super.loadCache(ResourceName, CacheName);

    tmpStoreLimit = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       SERVICE_STORE_LIMIT,
                                                       "180");
    StoreLimit = Integer.valueOf(tmpStoreLimit);
  }

 /**
  * Save the object data to a file. Because the objects in the object are
  * small and non-persistent, we store them in a flat file format.
  */
  @Override
  public void saveCacheObjectsToFile() throws ProcessingException
  {
    int                objectsLoaded = 0;
    BufferedWriter     outFile = null;
    String             tmpFileRecord;
    Collection<String> objectSet;
    Iterator<String>   objectIterator;
    String             tmpKey;
    AssemblyCtx        tmpInfo;

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Assembly Cache saving to file");

    try
    {
      // Try to open the file
      outFile = new BufferedWriter(new FileWriter(CachePersistenceName));
    }
    catch (IOException ex)
    {
      message = "Error opening output file";
      throw new ProcessingException(message,ex,getSymbolicName());
    }

    objectSet = ObjectList.keySet();
    objectIterator = objectSet.iterator();

    while (objectIterator.hasNext())
    {
      tmpKey = objectIterator.next();
      tmpInfo = (AssemblyCtx) ObjectList.get(tmpKey);
      tmpFileRecord = tmpKey + ";" +
                      String.valueOf(tmpInfo.totalDuration) + ";" +
                      String.valueOf(tmpInfo.totalData) + ";" +
                      String.valueOf(tmpInfo.uplink) + ";" +
                      String.valueOf(tmpInfo.downlink) + ";" +
                      String.valueOf(tmpInfo.state) + ";" +
                      String.valueOf(tmpInfo.StartDate) + ";" +
                      String.valueOf(tmpInfo.ClosedDate);
      try
      {
        outFile.write(tmpFileRecord);
        outFile.newLine();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error writing to file");
      }
    }

    try
    {
      // close the file
      outFile.close();
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().error("Error closing file");
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Assembly Data Saving completed. " + objectsLoaded +
          " configuration lines saved <" + CachePersistenceName +
          ">");
  }

 /**
  * Load the object data from a file. Because the objects in the object are
  * small and non-persistent, we store them in a flat file format.
  */
  @Override
  public void loadCacheObjectsFromFile()
  {
    // Variable declarations
    int            ObjectsLoaded = 0;
    BufferedReader inFile = null;
    String         tmpFileRecord;
    String[]       ObjectFields;

    // Get the sysdate for getting rid of old data
    long storeCutoff = new Date().getTime()/1000 - StoreLimit*86400;

    // Log that we are starting the loading
    OpenRate.getOpenRateFrameworkLog().info("Starting Persistent Partial Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(CachePersistenceName));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      OpenRate.getOpenRateFrameworkLog().warning(
            "Application is not able to read file : <" +
            CachePersistenceName + ">");
    }

    if (inFile != null)
    {
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
            ObjectsLoaded++;
            ObjectFields = tmpFileRecord.split(";");
            AssemblyCtx tmpInfo = new AssemblyCtx();
            tmpInfo.totalDuration = Double.valueOf(ObjectFields[1]);
            tmpInfo.totalData     = Double.valueOf(ObjectFields[2]);
            tmpInfo.uplink        = Double.valueOf(ObjectFields[3]);
            tmpInfo.downlink      = Double.valueOf(ObjectFields[4]);
            tmpInfo.state         = Integer.parseInt(ObjectFields[5]);
            tmpInfo.StartDate     = Integer.parseInt(ObjectFields[6]);
            tmpInfo.ClosedDate    = Integer.parseInt(ObjectFields[7]);

            // if the call is not too old
            if (tmpInfo.ClosedDate > storeCutoff)
            {
              // add it to the cache
              ObjectList.put(ObjectFields[0],tmpInfo);
            }
          }
        }
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().fatal(
              "Error reading input file <" + CachePersistenceName +
              "> in record <" + ObjectsLoaded + ">. IO Error.");
      }
      catch (ArrayIndexOutOfBoundsException ex)
      {
        OpenRate.getOpenRateFrameworkLog().fatal(
              "Error reading input file <" + CachePersistenceName +
              "> in record <" + ObjectsLoaded + ">. Malformed Record.");
      }
      finally
      {
        try
        {
          inFile.close();
        }
        catch (IOException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + CachePersistenceName +
                    ">", ex);
        }
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Persistent Partial Data Loading completed. " + ObjectsLoaded +
          " configuration lines loaded from <" + CachePersistenceName +
          ">");
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
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STORE_LIMIT, ClientManager.PARAM_DYNAMIC);
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
    int tmpStoreLimit;
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_STORE_LIMIT))
    {
      if (Parameter.equals(""))
      {
        return String.valueOf(StoreLimit);
      }
      else
      {
        try
        {
          tmpStoreLimit = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          // do not change the value
          tmpStoreLimit = StoreLimit;
        }

        StoreLimit = tmpStoreLimit;

        ResultCode = 0;
      }
    }

    if (ResultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

  /**
   * Format the internal record for dumping
   * 
   * @param key
   * @param object
   * @return 
   */
  @Override
  public String formatObject(String key, Object object) {
    // Cast to our object
    AssemblyCtx tmpCtx = (AssemblyCtx) object;

    String result =
      key + ":" +
      String.valueOf(tmpCtx.totalDuration) + ";" +
      String.valueOf(tmpCtx.totalData) + ";" +
      String.valueOf(tmpCtx.uplink) + ";" +
      String.valueOf(tmpCtx.downlink) + ";" +
      String.valueOf(tmpCtx.state) + ";" +
      String.valueOf(tmpCtx.StartDate) + ";" +
      String.valueOf(tmpCtx.ClosedDate);
    
    return result;
  }
}
