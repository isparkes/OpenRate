/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

import OpenRate.cache.CustomerCache;
import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.lang.ProductList;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * This class is a somewhat degenerate example of a Filter, but it does show how
 * one would write a Filter. A filter implements a doWork() method that is
 * passed a set of records from the inbound channel. The Filter should process
 * the provided records and return the output records as a Collection. The
 * Filter may transform the records in-place, or it may toss the original
 * records and create completely new ones. It's entirely up to the Filter.
 */
public abstract class AbstractCustomerLookup
    extends AbstractPlugIn
{
  private ICacheManager CM;

  /**
   * The reference to the customer cache, so we can access the methods in it
   */
  protected CustomerCache CC;

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
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    CC = (CustomerCache)CM.get(CacheObjectName);

    if (CC == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
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
  * @param Alias The Login (Alias) used to identify the account
  * @return The list of products for the login-date-service combination
  * @throws java.lang.Exception
  */
  public String getCustId(String Alias) throws Exception
  {
    String custId;

    custId = CC.getCustId(Alias);

    return custId;
  }

 /**
  * This returns a product list based on a login, a service and the validity of
  * the customer and of the individual products.
  *
  * @param Login The Alias used to identify the account
  * @param Service The service identifier
  * @param CDRDate The date of the CDR
  * @return The list of products for the login-date-service combination
  * @throws java.lang.Exception
  */
  public ProductList getProductList(String Login, String Service, long CDRDate) throws Exception
  {
    ProductList tmpProductList;

    tmpProductList = CC.getProducts(Login,Service,CDRDate);

    return tmpProductList;
  }

 /**
  * This returns a product list based on a login, a service and the validity of
  * the customer and of the individual products.
  *
  * @param custId The CustId to search for
  * @param ERAKey The ERA Key to get the value for
  * @return The list of products for the login-date-service combination
  * @throws java.lang.Exception
  */
  public String getERA(String custId, String ERAKey) throws Exception
  {
    String ERAValue;

    ERAValue = CC.getERA(custId, ERAKey);

    return ERAValue;
  }

}
