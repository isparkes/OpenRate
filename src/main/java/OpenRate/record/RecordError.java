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


package OpenRate.record;

/**
 * Errors are added to the record at any time during processing.
 */
public class RecordError extends AbstractError
{
  private static final long serialVersionUID = -5050358890913890453L;

 /**
  * Constructor for RecordError, providing the type as well as the message and
  * the module name
  *
  * @param msg The message to attach to the error
  * @param type The error type
  * @param moduleName The name of the calling module
  */
  public RecordError(String msg, ErrorType type, String moduleName, String errorDescription)
  {
    super(msg, type, moduleName, errorDescription);
  }

 /**
  * Constructor for RecordError, providing the type as well as the message and
  * the module name
  *
  * @param msg The message to attach to the error
  * @param type The error type
  * @param moduleName The name of the calling module
  */
  public RecordError(String msg, ErrorType type, String moduleName)
  {
    super(msg, type, moduleName);
  }

 /**
  * Constructor for RecordError, providing the type as well as the message
  *
  * @param msg The message to attach to the error
  * @param type The error type
  */
  public RecordError(String msg, ErrorType type)
  {
    super(msg, type);
  }

  /**
   * Constructor for RecordError, uses the default special type
   *
   * @param msg The message to attach to the error. The type will be "SPECIAL"
   */
  public RecordError(String msg)
  {
    super(msg, ErrorType.SPECIAL);
  }
}
