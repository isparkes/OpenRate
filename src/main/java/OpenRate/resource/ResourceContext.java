/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.resource;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The ResourceContext class is a pseudo InitialContext. It is
 * responsible for servicing lookups from the application classes
 * to a set of the ResourceFactories. It creates & initializes
 * the Factories during application startup, and call the Factory
 * cleanup methods during application shutdown.
 *
 */
public class ResourceContext
{
  private static HashMap<String, IResource> resourceMap = new HashMap<>();
  private boolean active;

 /**
  * Default Constructor
  */
  public ResourceContext()
  {
    // no op
  }

 /**
  * Get the keyset of all the resources in the map
  *
  * @return The Keyset of the resources in the map
  */
  public Collection<String> keySet()
  {
    return resourceMap.keySet();
  }

 /**
  * Manually register a resource with the ResourceContext object. The
  * resource should have already been initialized.
  *
  * @param name The resource name
  * @param resource The resource object reference
  */
  public void register(String name, IResource resource) throws InitializationException
  {
    if (name == null || name.isEmpty())
    {
      throw new InitializationException("Resource name is empty for resource <" + resource.toString() + ">", "resourceContext");
    }
    
    if (resource == null)
    {
      throw new InitializationException("Resource is null for resource <" + name + ">", "resourceContext");
    }
    
    resourceMap.put(name, resource);
    
    // set that we are active
    active = true;
  }

 /**
  * Lookup a resource by name. If found, return to caller. Else
  * return null.
  *
  * @param name The resource to look up
  * @return The resource object
  */
  public IResource get(String name)
  {
    return resourceMap.get(name);
  }

 /**
  * Perform whatever cleanup is required of the underlying object. We take two
  * goes at this, shutting down all non-log resources first, then the log last.
  * 
  * The reason for this this that we might need the log right up until the very
  * last minute.
  */
  public void cleanup()
  {
    Collection<IResource> resources = resourceMap.values();
    Iterator<IResource>   iter      = resources.iterator();

    IResource logResource = null;
    
    while (iter.hasNext())
    {
      IResource resource = iter.next();
      
      if (resource.getSymbolicName() == null)
      {
        System.err.println("Resource name is null for resource <" + resource.toString() + ">");
      }
      
      System.out.println("Closing <" + resource.getSymbolicName() + ">");
      
      if (resource.getSymbolicName().equals(LogFactory.RESOURCE_KEY))
      {
        logResource = resource;
      }
      else
      {
        resource.close();
      }
    }
    
    // Now do the log resource
    if (logResource != null)
      logResource.close();

    // Clear down the map
    resourceMap.clear();

    // set that we are no longer active
    active = false;
  }

  /**
   * @return the active
   */
  public boolean isActive() {
    return active;
  }

  /**
   * @param active the active to set
   */
  public void setActive(boolean active) {
    this.active = active;
  }
}
