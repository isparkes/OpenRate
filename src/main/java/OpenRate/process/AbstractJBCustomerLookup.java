/* ====================================================================
 * Limited Evaluation License:
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.JBCustomerCache;
import OpenRate.exception.InitializationException;
import OpenRate.lang.ProductList;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * This module implements the processing interface to the JBCustomerCache
 * object, used for linking OpenRate to JBilling. The interface with JBilling
 * is managed by a CustomerID (The base_user id in jbilling) and the
 * product list, which holds all of the products that the customer has for the
 * given service, login id, Subscription ID and cdr date.
 */
public abstract class AbstractJBCustomerLookup
    extends AbstractPlugIn
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractJBCustomerLookup.java,v $, $Revision: 1.23 $, $Date: 2013-05-13 18:12:10 $";

  private ICacheManager CM;

   /**
    * The reference to the customer cache, so we can access the methods in it
    */
   private JBCustomerCache CC;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
    throws InitializationException
  {
    String CacheObjectName;

    // Register ourself with the client manager
    setSymbolicName(ModuleName);

    // do the inherited initialisation
    super.init(PipelineName,ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");

    // Load up the customer information held in the Cached Object
    CM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CM == null)
    {
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Load up the mapping arrays
    setCC((JBCustomerCache)CM.get(CacheObjectName));

    if (getCC() == null)
    {
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    return null;
  }

 /**
  * This is called when a stream trailer record is encountered, and has the
  * meaning that the stream is ending
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    return null;
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * This returns a product list based on a login, a service and the validity of
  * the customer and of the individual products.
  *
  * @param alias The Login (Alias) used to identify the account
  * @param service The service identifier
  * @param subscription The subscription to get for
  * @param CDRDateUTC The date of the CDR
  * @return The list of products for the login-date-service combination
  * @throws java.lang.Exception
  */
  protected ProductList getProductList(String alias, String service, String subscription, long CDRDateUTC) throws Exception
  {
    ProductList tmpProductList;

    tmpProductList = getCC().getProducts(alias,service,subscription, CDRDateUTC);

    return tmpProductList;
  }

 /**
  * This returns a product list based on a login, a service and the validity of
  * the customer and of the individual products.
  *
  * @param custId The customer ID of the account
  * @param service The service identifier
  * @param subscription The subscription to get for
  * @param CDRDateUTC The date of the CDR
  * @return The list of products for the login-date-service combination
  * @throws java.lang.Exception
  */
  protected ProductList getProductList(Integer custId, String service, String subscription, long CDRDateUTC) throws Exception
  {
    ProductList tmpProductList;

    tmpProductList = getCC().getProducts(custId,service,subscription, CDRDateUTC);

    return tmpProductList;
  }

 /**
  * Returns the jbilling user id from the login we provide
  *
  * @param login The login
  * @param CDRDateUTC the date to check for
  * @return The JB user id
  */
  protected Integer getCustId(String login, long CDRDateUTC)
  {
    return getCC().getCustId(login,CDRDateUTC);
  }
  
 /**
  * Returns if we know a jbilling user id
  *
  * @param custId The customer ID to check for
  * @return If we know the JB user id
  */
  protected boolean getCustIdExists(Integer custId)
  {
    return getCC().getCustIdExists(custId);
  }

  /**
   * @return the CC
   */
  public JBCustomerCache getCC() {
    return CC;
  }

  /**
   * @param CC the CC to set
   */
  public void setCC(JBCustomerCache CC) {
    this.CC = CC;
  }
}
