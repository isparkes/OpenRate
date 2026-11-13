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

/**
 * This class is used to track the assembly of voice or data partials, and allows
 * persistence on system startup or shutdown. It manages the accumulated totals
 * we well as the state of the context.
 *
 * @author ian
 */
public class AssemblyCtx
{
  /**
   * the cumulated duration
   */
  public double totalDuration = 0;

  /**
   * the cumulated volume (uplink and downlink)
   */
  public double totalData = 0;

  /**
   * the cumulated uplink
   */
  public double uplink = 0;

  /**
   * the cumulated downlink
   */
  public double downlink = 0;

  /**
   * Ctx state: 1 = start, 2 = intermediate, 3 = closed
   */
  public int    state = 0;

  /**
   * the start date of the first partial
   */
  public long   StartDate = 0;

  /**
   * the date the context was closed, for purging
   */
  public long   ClosedDate = 0;
}
