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


package OpenRate.lang;

import OpenRate.CommonConfig;


/**
 * A counter is a balance counter, that holds the record ID, the validity period
 * and the current balance.
 *
 * @author ian
 */
public class Counter
{
  /**
   * used to hold the current balance
   */
  public double CurrentBalance = 0;

  /**
   * The unique id of this counter
   */
  public long   RecId    = 0;

  /**
   * the validity start and end of this counter, default for ever
   */
  public long   validFrom = CommonConfig.LOW_DATE;

  /**
   * the validity start and end of this counter, default for ever
   */
  public long   validTo = CommonConfig.HIGH_DATE;

 /** Creates a new instance of Counter */
  public Counter()
  {
  }
}
