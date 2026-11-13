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
 * This class maps the initial and updated value of the RUM for the RatingRecord
 * for RUM based rating.
 *
 * @author ian
 */
public class RUMInfo
{
 /**
  * Name of the RUM
  */
  public String RUMName = null;

 /**
  * The original RUM quantity
  */
  public double OrigQuantity = 0;

 /**
  * The current RUM quantity
  */
  public double RUMQuantity = 0;

  /**
   * Creates a new instance of the RUMInfo block
   *
   * @param RUM The name of the RUM to
   * @param Quantity The value to set the RUM to
   */
  public RUMInfo(String RUM, double Quantity)
  {
    RUMName = RUM;
    OrigQuantity = Quantity;
    RUMQuantity = Quantity;
  }
}
