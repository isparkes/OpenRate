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
 * This is a bean object containing attributes necessary to manipulate <br/>
 * the socket listener. The member variable connectionNumber is the one<br/>
 * defining how many concurrent connections are allowed. While the boolean<br/>
 * loop is responsible for making the while loop in OpenRateSocket's<br/>
 * startListener() method continuously loop. When set to false, the loop stops.
 */
public class SocketConnectionData
{
  //number of allowed concurrent connections
  private int connectionNumber = 0;

  //boolean value for the listener's while looping.
  private boolean loop = true;
  
  /**
   * add a connection to the number managed
   */
  public void incrementCount()
  {
    connectionNumber++;
  }

  /**
   * remove a connection to the number managed
   */
  public void decrementCount()
  {
    connectionNumber--;
  }

  /**
   * getter method of connectionNumber
   *
   * @return The connection number
   */
  public int getConnectionNumber()
  {
    return connectionNumber;
  }

  /**
   * setter method of connectionNumber
   *
   * @param connectionNumber The new connection number
   */
  public void setConnectionNumber(int connectionNumber)
  {
    this.connectionNumber = connectionNumber;
  }

  /**
   * getter method of loop
   *
   * @return if the loop is true
   */
  public boolean isLoop()
  {
    return loop;
  }

  /**
   * setter of loop
   *
   * @param loop set the loop value
   */
  public void setLoop(boolean loop)
  {
    this.loop = loop;
  }
}
