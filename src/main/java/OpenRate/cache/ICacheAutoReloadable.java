
package OpenRate.cache;

/**
 * Interface used by the caching system for ICacheAutoReloadable
 * objects.
 */
public interface ICacheAutoReloadable
{
  /**
   * Get the period (seconds) of the cache auto reloading
   *
   * @return The configured period
   */
  long getAutoReloadPeriod();

  /**
   * Set the period (seconds) of the cache auto reloading
   *
   * @param autoReloadPeriod The new period in seconds
   */
  void setAutoReloadPeriod(long autoReloadPeriod);

  /**
   * Get the last time that this cache was reloaded
   *
   * @return The last reload time
   */
  long getLastReloadUTC();

 /**
  * Set the time that this cache was last reloaded
  *
  * @param lastReloadUTC The reload time
  */
  void setLastReloadUTC(long lastReloadUTC);

  /**
   * See if this cache has been excluded from auto reloading
   *
   * @return true if excluded, otherwise false
   */
  boolean getExcludeFromAutoReload();
}
