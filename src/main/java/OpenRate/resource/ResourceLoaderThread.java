

package OpenRate.resource;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import java.util.HashMap;

/**
 * Runnable container for threaded resource loading.
 *
 * @author tgdspia1
 */
public class ResourceLoaderThread extends Thread
{
  private IResource resource;
  private String    resourceName;
  private ResourceContext resourceContext;
  private HashMap<String, IEventInterface> syncPointResourceMap;

  /**
   * Constructor for creating the loader thread.
   *
   * @param tmpGrpResource The thread group we assign to.
   * @param tmpResourceName The resource we are creating for.
   */
  public ResourceLoaderThread(ThreadGroup tmpGrpResource, String tmpResourceName)
  {
    super(tmpGrpResource,tmpResourceName);
  }

  /**
   * Setter for the IResource we are managing.
   *
   * @param resourceToInit The resource we are creating the thread for.
   */
  public void setResource(IResource resourceToInit)
  {
    this.resource = resourceToInit;
  }

  /**
   * Setter for the name of the resource we are managing. Needed to access the
   * properties configuration.
   *
   * @param resourceName The resource name.
   */
  public void setResourceName(String resourceName)
  {
    this.resourceName = resourceName;
  }

 /**
  * Main execution thread of the resource loader thread. Initialises the
  * resource and registers it with the context and sync point map before ending.
  */
  @Override
  public void run()
  {
    try
    {
      // initalise the resource
      resource.init(resourceName);

      //resource.init(tmpResourceName);
      resourceContext.register(resourceName, resource);

      // Now see if we have to register with the config manager
      if (resource instanceof IEventInterface)
      {
        // Register
        IEventInterface tmpEventIntf = (IEventInterface)resource;
        tmpEventIntf.registerClientManager();

        // Add the resource to the list of the resources that can call for
        // a sync point
        syncPointResourceMap.put(resourceName, tmpEventIntf);
      }
    }
    catch (InitializationException ie)
    {
      OpenRate.getFrameworkExceptionHandler().reportException(ie);
    }
  }

  /**
   * Setter for the resource context. Used for registering the resource with
   * the context once we have made it.
   *
   * @param resourceContext The resource context we are using.
   */
  public void setResourceContext(ResourceContext resourceContext)
  {
    this.resourceContext = resourceContext;
  }

  /**
   * Setter for the Sync Point Resource Map. In the case that this resource
   * is bound into the synchronisation framework, we need access to the map
   * to register ourselves for management of sync point handling.
   *
   * @param syncPointResourceMap The resource map.
   */
  public void setsyncPointResourceMap(HashMap<String, IEventInterface> syncPointResourceMap)
  {
    this.syncPointResourceMap = syncPointResourceMap;
  }
}
