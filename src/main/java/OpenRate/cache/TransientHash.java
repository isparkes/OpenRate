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

package OpenRate.cache;

import OpenRate.exception.InitializationException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A shared object which can be created as a resource.  This object can then be
 * retrieved from the ResourceContext and used to share data between threads.
 *
 * Because of this, we define the hash as a concurrent hash.
 */
public class TransientHash extends AbstractCache implements ICacheLoader
{
  /**
   * Hash map of data that is to be shared between modules.
   */
  protected ConcurrentHashMap<String,String> sharedHash = null;

 /**
  * Initalization of the transient object store
  *
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
          throws InitializationException
  {
    // Set the symbolic name
    setSymbolicName(CacheName);

    // initialise the hash object
    sharedHash = new ConcurrentHashMap<>();
  }

  /**
   * Set the shared data to null for cleanup.
   */
  public void close()
  {
    sharedHash.clear();
  }

  /**
   * Returns the sharedData.
   *
   * @return The shared data
   */
  public synchronized ConcurrentHashMap<String, String> getSharedData()
  {
    return sharedHash;
  }

  /**
   * Sets the sharedData.
   *
   * @param sharedData The shared data object
   */
  public synchronized void setSharedData(ConcurrentHashMap<String,String> sharedData)
  {
    this.sharedHash = sharedData;
  }

 /**
  * Puts a string into the hash
  *
  * @param Key The key to use
  * @param Value The value to store
  */
  public void putString(String Key, String Value)
  {
    sharedHash.put(Key, Value);
  }

 /**
  * Sees if the hash contains a key
  *
  * @param Key The key to search for
  * @return true if the hash contains the key, otherwise false
  */
  public boolean containsKey(String Key)
  {
    return sharedHash.containsKey(Key);
  }

 /**
  * Gets a value from the hash
  *
  * @param Key The key to retrieve the value for
  * @return The retrieved value
  */
  public String getString(String Key)
  {
    return sharedHash.get(Key);
  }

 /**
  * Clears the hash
  */
  public void clear()
  {
    sharedHash.clear();
  }
}
