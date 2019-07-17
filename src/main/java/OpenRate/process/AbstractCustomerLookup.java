

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
