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
package OpenRate.logging;

/**
 * AstractLogger class. This abstract class needs to be overridden by a
 * concrete implementation instance for use. In normal cases this will be via
 * the log4j logger.
 */
public abstract class AstractLogger implements ILogger
{
  // This is the symbolic name of the resource
  private String symbolicName;

  /**
   * @param symbolicName the SymbolicName to set
   */
  public void setSymbolicName(String symbolicName) {
    this.symbolicName = symbolicName;
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
