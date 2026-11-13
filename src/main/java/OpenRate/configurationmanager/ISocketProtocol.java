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


package OpenRate.configurationmanager;

/**
 * @author = g.z.
 * Interface for Socket protocols
 */
public interface ISocketProtocol
{
 /**
  * Processes the input and returns the reply of the protocol
  *
  * @param input The input to process
  * @return The retuen string
  */
  public String processInput(String input);
}
