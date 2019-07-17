package OpenRate.adapter;

import OpenRate.exception.InitializationException;

/**
 * Base interface for all adapters. Defines the methods for initialising and
 * handling exceptions in all adapters.
 *
 */
public interface IAdapter {

  /**
   * Initialise the module. Called during pipeline creation.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  public void init(String PipelineName, String ModuleName)
          throws InitializationException;

  /**
   * Perform any cleanup. Called by OpenRateApplication during application
   * shutdown. Since the application is shutting down when cleanup is called,
   * there are no checked exceptions thrown from this method. Cleanup failures
   * should be able to be ignored.
   */
  public void cleanup();
}
