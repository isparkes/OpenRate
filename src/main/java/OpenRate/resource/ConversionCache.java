

package OpenRate.resource;

import OpenRate.exception.InitializationException;
import OpenRate.utils.ConversionUtils;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ConversionCache provides access to conversion objects, with the aim of
 * making record objects lighter by re-use of shared conversion objects. The
 * conversion object is particularly heavy during creation, and is used often.
 * This cache therefore gives a reasonable increase in performance.
 */
public class ConversionCache implements IResource
{
  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "ConversionCache";

  // This is the symbolic name of the resource
  private String symbolicName;

  // cache Categories
  private static ConcurrentHashMap<String, ConversionUtils> ConversionManagers = new ConcurrentHashMap<>();

  /**
   * default constructor - protected
   */
  public ConversionCache()
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
    if (ResourceName.equals(RESOURCE_KEY) == false)
    {
      throw new InitializationException("Conversion Cache must be called ConversionCache",getSymbolicName());
    }
    else
    {
      symbolicName = ResourceName;
    }
  }

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close()
  {
    ConversionManagers.clear();
  }

  /**
   * Utility to return the reference to the conversion resource
   *
   * @param type The type string of the conversion object to return
   * @return The conversion object
   * @throws ConfigurationException
   */
  public ConversionUtils getConversionObject(String type)
  {
    if (ConversionManagers.containsKey(type))
    {
      // just return it
      return ConversionManagers.get(type);
    }
    else
    {
      // create it and return it
      ConversionUtils tmpConv = new ConversionUtils();
      ConversionManagers.put(type, tmpConv);
      return tmpConv;
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
