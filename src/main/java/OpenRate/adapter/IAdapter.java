/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
