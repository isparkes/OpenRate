

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
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    setCC((JBCustomerCache)CM.get(CacheObjectName));

    if (getCC() == null)
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
