

package OpenRate.utils;

import OpenRate.exception.InitializationException;

/**
 * PropertyUtils - Utility methods for dealing with Property files.
 */
public class PropertyUtils extends PropertyReader
{
  // Get the utilities for handling the XML configuration
  private static PropertyUtils propUtilsObj;

 /**
  * Creates a new instance of PropertyUtils can only be called with in the class
  */
  private PropertyUtils()
  {
    super();
  }

 /**
  * This utility function returns the singleton instance of PropertyUtils, and
  * initialises it if necessary.
  *
  * @return    the instance of PropertyUtils
  */
  public static PropertyUtils getPropertyUtils()
  {
    if(propUtilsObj == null)
    {
      propUtilsObj = new PropertyUtils();
    }

    return propUtilsObj;
  }

 /**
  * This utility function deallocates the singleton instance of PropertyUtils
  */
  public static void closePropertyUtils()
  {
    propUtilsObj = null;
  }

 /**
  * This utility function returns the value specified from the group specified,
  * meaning that we will look for the prefix, a "." and then the value provided
  * and return the value of that
  *
  * @param     pipeName the name of the pipeline to search in
  * @param     groupPrefix the group
  * @param     propertyName the property suffix to search for
  * @return    the value string that we are searching for if found, otherwise
  *            null
  */
  public String getPipelinePropertyValue(String     pipeName,
                                         String     groupPrefix,
                                         String     propertyName)
  {
    String         searchKey;
    String         valueFound;

    searchKey = pipeName+"."+groupPrefix+"."+propertyName;
    valueFound = this.getPropertyValue(searchKey);

    return valueFound;
  }

 /**
  * This utility function returns the value specified from the group specified,
  * meaning that we will look for the prefix, a "." and then the value provided
  * and return the value of that.
  *
  * @param pipeName The name of the pipe we want to get the property for
  * @param groupPrefix The name of the group
  * @param propertyName The property
  * @param defaultValue The default value we will return if nothing is found
  * @return The found or default value
  */
  public String getPipelinePropertyValueDef(String     pipeName,
                                            String     groupPrefix,
                                            String     propertyName,
                                            String     defaultValue)
  {
    String valueFound;

    valueFound = this.getPipelinePropertyValue(pipeName,groupPrefix,propertyName);

    if (valueFound == null) {
        valueFound = defaultValue;
    }

    return valueFound;
  }

  // ******************* Higher Level Functions ***********************

 /**
  * Get a property value for the batch input adapter for the given pipeline
  *
  * @param pipeName The name of the pipeline to retrieve from
  * @param moduleName The name of the module
  * @param propertyName The name of the property to retrieve
  * @return The property value
  */
  public String getBatchInputAdapterPropertyValue(String pipeName, String moduleName, String propertyName)
  {
    String valueFound;

    valueFound = getGroupPropertyValue(pipeName,"InputAdapter."+moduleName+"."+propertyName);

    return valueFound;
  }

 /**
  * Get a property value for the batch input adapter for the given pipeline
  *
  * @param pipeName The name of the pipeline to retrieve from
  * @param moduleName The name of the module
  * @param propertyName The name of the property to retrieve
  * @param defaultValue The default value of the property to return
  * @return The property value
  */
  public String getBatchInputAdapterPropertyValueDef(String pipeName, String moduleName, String propertyName, String defaultValue)
  {
    String valueFound;

    valueFound = getGroupPropertyValue(pipeName,"InputAdapter."+moduleName+"."+propertyName);

    if (valueFound == null) {
        valueFound = defaultValue;
    }

    return valueFound;
  }

 /**
  * Get a property value for the batch input adapter for the given pipeline
  *
  * @param pipeName The name of the pipeline to retrieve from
  * @param moduleName The name of the module
  * @param propertyName The name of the property to retrieve
  * @return The property value
  */
  public String getRTAdapterPropertyValue(String pipeName, String moduleName, String propertyName)
  {
    String valueFound;

    valueFound = getGroupPropertyValue(pipeName,"RTAdapter."+moduleName+"."+propertyName);

    return valueFound;
  }

 /**
  * Get a property value for the batch input adapter for the given pipeline
  *
  * @param pipeName The name of the pipeline to retrieve from
  * @param moduleName The name of the module
  * @param propertyName The name of the property to retrieve
  * @param defaultValue The default value of the property to return
  * @return The property value
  */
  public String getRTAdapterPropertyValueDef(String pipeName, String moduleName, String propertyName, String defaultValue)
  {
    String valueFound;

    valueFound = getRTAdapterPropertyValue(pipeName, moduleName, propertyName);

    if (valueFound == null) {
        valueFound = defaultValue;
    }

    return valueFound;
  }

  /**
   * Get a property value for a plugin
   *
   * @param PipeName The name of the pipeline
   * @param moduleName The name of the module
   * @param PropertyName The name of the property to get
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getPluginPropertyValue(String PipeName,
          String moduleName,
          String PropertyName)
          throws InitializationException
  {
      // This is the symbolic name for tieing the module stack config items
      // together
      String valueFound;
      String ModuleSymKey;

      ModuleSymKey = PipeName + ".Process." + moduleName + "." + PropertyName;
      valueFound = this.getPropertyValue(ModuleSymKey);

      return valueFound;
  }

  /**
   * Get a property value for a plugin, giving a default
   *
   * @param pipeName The name of the pipeline
   * @param moduleName The name of the module
   * @param propertyName The name of the property to get
   * @param defaultValue The default value if no key is found
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getPluginPropertyValueDef(String pipeName,
          String moduleName,
          String propertyName,
          String defaultValue)
          throws InitializationException {
      String valueFound;

      valueFound = this.getPluginPropertyValue(pipeName, moduleName, propertyName);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }

  /**
   * Get a property value for an output adapter
   *
   * @param pipeName The name of the pipeline
   * @param moduleName The name of the module
   * @param propertyName The name of the property to get
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getBatchOutputAdapterPropertyValue(String pipeName, String moduleName, String propertyName) throws InitializationException {
      String valueFound;
      String ModuleSymKey;
      ModuleSymKey = pipeName + ".OutputAdapter." + moduleName + "." + propertyName;
      valueFound = this.getPropertyValue(ModuleSymKey);

      return valueFound;
  }

  /**
   * Get a property value for an output adapter, giving a default
   *
   * @param pipeName The name of the pipeline
   * @param moduleName The name of the module
   * @param propertyName The name of the property to get
   * @param defaultValue The default value if no key is found
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getBatchOutputAdapterPropertyValueDef(String pipeName, String moduleName, String propertyName, String defaultValue) throws InitializationException {
      String valueFound;
      String ModuleSymKey;
      ModuleSymKey = pipeName + ".OutputAdapter." + moduleName + "." + propertyName;
      valueFound = this.getPropertyValue(ModuleSymKey);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }

  /**
   * Get a property value for a resource
   *
   * @param resourceName The name of the resource
   * @param propertyName The name of the property to get
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getResourcePropertyValue(String resourceName, String propertyName) throws InitializationException {
      String valueFound;
      String ModuleSymKey;

      ModuleSymKey = "Resource." + resourceName + "." + propertyName;
      valueFound = this.getPropertyValue(ModuleSymKey);

      return valueFound;
  }

  /**
   * Get a property value for a resource, giving a default
   *
   * @param resourceName The name of the resource
   * @param propertyName The name of the property to get
   * @param defaultValue The default value if no key is found
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getResourcePropertyValueDef(String resourceName, String propertyName, String defaultValue) throws InitializationException {
      String valueFound;
      valueFound = this.getResourcePropertyValue(resourceName,propertyName);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }

  /**
   * Get a property value for a data cache
   *
   * @param cacheManName The name of the cache manager
   * @param CacheName The name of the cache
   * @param propertyName The name of the property to get
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getDataCachePropertyValue(String cacheManName, String CacheName, String propertyName) throws InitializationException {
      String valueFound;
      String ModuleSymKey;

      ModuleSymKey = "Resource."+cacheManName+".CacheableClass."+CacheName+"."+propertyName;
      valueFound = this.getPropertyValue(ModuleSymKey);

      return valueFound;
  }

  /**
   * Get a property value for a data cache, giving a default
   *
   * @param cacheManName The name of the cache manager
   * @param cacheName The name of the cache
   * @param propertyName The name of the property to get
   * @param defaultValue The default value if no key is found
   * @return The property value
   * @throws OpenRate.exception.InitializationException
   */
  public String getDataCachePropertyValueDef(String cacheManName, String cacheName, String propertyName, String defaultValue) throws InitializationException {
      String valueFound;
      valueFound = this.getDataCachePropertyValue(cacheManName,cacheName,propertyName);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }

  /**
   * helper method for retrieving a property for the specific data source.
   *
   * @param dataSourceName The name of the data source to get the value for
   * @param propertyName - the attribute name which is being requested.
   * @return String - the value of the request attribute.
   * @throws InitializationException
   */
  public String getDataSourcePropertyValue(String dataSourceName, String propertyName) throws InitializationException
  {
    String valueFound;
    String ModuleSymKey;

    ModuleSymKey = "DataSourceFactory.DataSource." + dataSourceName;
    valueFound = this.getResourcePropertyValue(ModuleSymKey,propertyName);

    return valueFound;
  }

  /**
   * helper method for retrieving a property for the specific data source.
   *
   * @param dataSourceName The name of the data source to get the value for
   * @param propertyName - the attribute name which is being requested.
   * @param defaultValue The default value to return if no configuration is found
   * @return String - the value of the request attribute.
   * @throws InitializationException
   */
  public String getDataSourcePropertyValueDef(String dataSourceName, String propertyName, String defaultValue) throws InitializationException
  {
      String valueFound;
      valueFound = this.getDataSourcePropertyValue(dataSourceName,propertyName);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }

  /**
   * helper method for retrieving a property for the framework.
   *
   * @param propertyName - the attribute name which is being requested.
   * @return String - the value of the request attribute.
   * @throws InitializationException
   */
  public String getFrameworkPropertyValue(String propertyName) throws InitializationException
  {
    String valueFound;

    valueFound = this.getPropertyValue(propertyName);

    return valueFound;
  }

  /**
   * helper method for retrieving a property for the framework.
   *
   * @param propertyName - the attribute name which is being requested.
   * @param defaultValue The default value to return if no configuration is found
   * @return String - the value of the request attribute.
   * @throws InitializationException
   */
  public String getFrameworkPropertyValueDef(String propertyName, String defaultValue) throws InitializationException
  {
      String valueFound;
      valueFound = this.getFrameworkPropertyValue(propertyName);

      if (valueFound == null) {
          valueFound = defaultValue;
      }

      return valueFound;
  }
}
