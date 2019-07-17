

package OpenRate.transaction;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.resource.IResource;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Factory for creating TransactionManager objects. Normally one Transaction
 * Manager will be created for each pipeline in the framework. This
 * allows the efficient multiprocessing of pipelines in parallel.
 */
public class TransactionManagerFactory implements IResource
{
  // This is the symbolic name of the resource
  private String symbolicName;

 /**
  * the configuration key used by the ResourceContext to look for & return
  * the configured TransactionManagerFactory
  */
  //public static final String KEY = "Resource.TMFactory";
  public static final String RESOURCE_KEY = "TransactionManagerFactory";

  // for handling thread safety
  private static final Object  lock    = new Object();
  private static HashMap<String, TransactionManager> transactionManagers = new HashMap<>();

  private static boolean    enabled = false;

  /**
   * Default Constructor
   */
  public TransactionManagerFactory()
  {
  }

  /**
   * Initialization the transaction manager
   *
   * @param ResourceName The name of the resource in the properties
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    // Set the symbolic name
    symbolicName = ResourceName;

    if (!symbolicName.equalsIgnoreCase(RESOURCE_KEY))
    {
      // we are relying on this name to be able to find the resource
      // later, so stop if it is not right
      throw new InitializationException("TransactionManagerFactory ModuleName should be <" + RESOURCE_KEY + ">",getSymbolicName());
    }

    // mark that we have been enabled
    enabled = true;
  }

  /**
   * retrieve a transaction manager given a pipeline name
   *
   * @param PipelineName
   * @return The transaction manager associated with the pipeline
   * @throws OpenRate.exception.InitializationException
   */
  public static TransactionManager getTransactionManager(String PipelineName) throws InitializationException
  {
    IEventInterface tmpEventIntf;
    TransactionManager tmpTM;

    if (enabled)
    {
      if (!transactionManagers.containsKey(PipelineName))
      {
        // Create the new Transaction Manager
        tmpTM = new TransactionManager();
        tmpTM.init(PipelineName);
        transactionManagers.put(PipelineName,tmpTM);

        // Now see if we have to register with the config manager
        if (tmpTM instanceof IEventInterface)
        {
          // Register
          tmpEventIntf = (IEventInterface)tmpTM;
          tmpEventIntf.registerClientManager();
        }
      }

      // Return the TM for the pipeline we have named
      return transactionManagers.get(PipelineName);
    }
    else
    {
      return null;
    }
  }

  /**
   * Cleanup the object pool that provides the connections.
   */
  @Override
  public void close()
  {
    Collection<TransactionManager> TMCollection;
    Iterator<TransactionManager>   iter;

    try
    {
      synchronized (lock)
      {
        TMCollection = transactionManagers.values();
        iter         = TMCollection.iterator();

        while (iter.hasNext())
        {
          TransactionManager TM = iter.next();
          TM.close();
        }

        transactionManagers.clear();
      }
    }
    catch (Exception e)
    {
      OpenRate.getOpenRateFrameworkLog().error("exception caught = " + e);
    }
  }

 /**
  * Return the resource symbolic name
  *
  * @return The resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
