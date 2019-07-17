

package OpenRate.resource;

import OpenRate.buffer.IBuffer;
import OpenRate.buffer.IConsumer;
import OpenRate.buffer.ISupplier;
import OpenRate.exception.InitializationException;
import java.util.HashMap;

/**
 * The ConversionCache provides access to conversion objects, with the aim of
 * making record objects lighter by re-use of shared conversion objects. The
 * conversion object is particularly heavy during creation, and is used often.
 * This cache therefore gives a reasonable increase in performance.
 */
public class LinkedBufferCache implements IResource
{
  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "LinkedBufferCache";

  // This is the symbolic name of the resource
  private String symbolicName;

  // cache Categories
  private static final HashMap<String, IBuffer> BufferList = new HashMap<>();

  /**
   * default constructor - protected
   */
  public LinkedBufferCache()
  {
    super();
  }

  /**
   * Perform whatever initialization is required of the resource.
   * This method should only be called once per application instance.
   *
   * @param ResourceName The name of the resource in the properties
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
	// Set the symbolic name
	symbolicName = ResourceName;
	
    if (ResourceName.equals(RESOURCE_KEY) == false)
    {
      throw new InitializationException("The linked buffer cache must be called " + RESOURCE_KEY,getSymbolicName());
    }
  }

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close()
  {
    BufferList.clear();
  }

  /**
   * Get the buffer supplier name
   *
   * @param name The name to get the buffer for
   * @return The supplier for the name
   * @throws InitializationException
   */
  public ISupplier getSupplier(String name) throws InitializationException
  {
    if (BufferList.containsKey(name))
    {
      // just return it
      return BufferList.get(name);
    }
    else
    {
      throw new InitializationException("Tried to get supplier <"+name+">, but this has not been stored",getSymbolicName());
    }
  }

  /**
   * Get the consumer for the given buffer name
   *
   * @param name The name to get the consumer for
   * @return The consumer
   * @throws InitializationException
   */
  public IConsumer getConsumer(String name) throws InitializationException
  {
    if (BufferList.containsKey(name))
    {
      // just return it
      return BufferList.get(name);
    }
    else
    {
      throw new InitializationException("Tried to get Consumer <"+name+">, but this has not been stored",getSymbolicName());
    }
  }

  /**
   * Put the buffer into the cache, indexing it under the given name
   *
   * @param name The name to store with
   * @param buffer The buffer to store
   * @throws InitializationException
   */
  public void putBuffer(String name, IBuffer buffer) throws InitializationException
  {
    if (BufferList.containsKey(name))
    {
      throw new InitializationException("Buffer <"+name+"> has already been stored",getSymbolicName());
    }
    else
    {
      BufferList.put(name,buffer);
    }
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
