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

package OpenRate.cache;

import OpenRate.exception.ExceptionHandler;

/**
 * This class encapsulates the most fundamental elements of a cache class, thus
 * freeing up descendents from having to define these anew.
 *
 * @author TGDSPIA1
 */
public abstract class AbstractCache
           implements ICacheable
{
  // The symbolic module name of the class stack
  private String symbolicName;

  // The exception handler that we use for reporting errors
  private ExceptionHandler handler;

  // used to simplify logging and exception handling
  public String message;
  
  /**
   * @return the symbolicName
   */
  public String getSymbolicName() {
    return symbolicName;
  }

  /**
   * @param symbolicName the symbolicName to set
   */
  public void setSymbolicName(String symbolicName) {
    this.symbolicName = symbolicName;
  }
  
  /**
   * @return the handler
   */
  public ExceptionHandler getHandler() {
    return handler;
  }

  /**
   * @param handler the handler to set
   */
    @Override
  public void setHandler(ExceptionHandler handler) {
    this.handler = handler;
  }
}
