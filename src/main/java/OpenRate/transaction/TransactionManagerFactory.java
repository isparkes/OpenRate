/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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
