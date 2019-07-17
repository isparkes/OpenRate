

package OpenRate.resource;

import OpenRate.exception.InitializationException;

/**
 * This interface can be implemented by objects that maintain
 * static resources requiring some "finalization" type
 * behavior. Since the JVM won't commit to calling finalize()
 * :-) I'm providing a hook where the object can be registered
 * w/ the application framework. All registered Resources will
 * have cleanup() called as part of application shutdown.
 *
 */
public interface IResource
{
  /**
   * Perform whatever initialization is required of the resource.
   *
   * @param ResourceName The name of the resource in the properties
   * @throws OpenRate.exception.InitializationException
   */
  public void init(String ResourceName) throws InitializationException;

 /**
  * return the symbolic name
  *
  * @return The module symbolic name
  */
  public String getSymbolicName();

 /**
  * Perform whatever cleanup is required of the underlying object..
  */
  public void close();
}

