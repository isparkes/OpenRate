package OpenRate.logging;

import OpenRate.exception.InitializationException;
import OpenRate.resource.IResource;

/**
 * AbstractLogFactory - create Factory for a certain logging library.
 */
public abstract class AbstractLogFactory implements IResource
{
  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "LogFactory";

  /**
   * Get a logger instance for the provided type.
   *
   * @param type The logger type
   * @return The logger
   */
  public abstract AstractLogger getLogger(String type);

  /**
   * Get a logger instance for the provided type.
   *
   * @param type The logger type
   * @return The logger
   * @throws InitializationException
   */
  public AstractLogger getLogger(Class<?> type) throws InitializationException
  {
    return getLogger(type.getName());
  }

  /**
   * Get a AbstractLogFactory based on the implementation class name
   * provided as a parameter.
   *
   * @param factoryImpl
   * @return The log factory
   * @throws InitializationException
   */
  public static AbstractLogFactory getFactory(String factoryImpl)
    throws InitializationException
  {
    AbstractLogFactory factory = null;

    try
    {
      if (factoryImpl == null)
      {
        throw new InitializationException("LogFactory className == null",RESOURCE_KEY);
      }

      Class<?> type = Class.forName(factoryImpl);
      Object   obj  = type.newInstance();
      factory     = (AbstractLogFactory) obj;
    }
    catch (ClassCastException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "LogFactory class name is not a LogFactory sub-class.",ex,RESOURCE_KEY);
    }
    catch (ClassNotFoundException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "LogFactory implementation class not found in classpath.",ex,RESOURCE_KEY);
    }
    catch (InstantiationException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "No default constructor for LogFactory",ex,RESOURCE_KEY);
    }
    catch (IllegalAccessException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "Cannot invoke default constructor on LogFactory. " +
        "Check that it's visibility is public.",ex,RESOURCE_KEY);
    }

    return factory;
  }
}
