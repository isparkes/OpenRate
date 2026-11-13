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
package OpenRate.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * ASN1Exception
 *
 * @author Magnus
 */
public class ASN1Exception extends NestableException
{
 /**
  * constructor
  */
  public ASN1Exception()
  {
    super();
  }

  /**
   * Create an exception with a string argument
   *
   * @param arg0 The string argument
   */
  public ASN1Exception(String arg0)
  {
    super(arg0);
  }

  /**
   * Create an exception with a throwable argument
   *
   * @param arg0 The throwable argument
   */
  public ASN1Exception(Throwable arg0)
  {
    super(arg0);
  }

  /**
   * Create an exception with string and throwable arguments
   *
   * @param arg0 The string argument
   * @param arg1 The throwable argument
   */
  public ASN1Exception(String arg0, Throwable arg1)
  {
    super(arg0, arg1);
  }
}
