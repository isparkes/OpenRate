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

import java.util.ArrayList;

/**
 * These are used to pass the results of rating back to the module so that
 * RUM consumption can be handled, and diagnostics can be written
 *
 * @author ian
 */
public class RatingResult
{
 /**
  * the total of all tiers rated
  */
  public double RatedValue = 0;

 /**
  * the amount of RUM consumed
  */
  public double RUMUsed = 0;

 /**
  * the amount of RUM consumed rounded to the next beat
  */
  public double RUMUsedRounded = 0;

 /**
  * the breakdown of the individual rating steps
  */
  public  ArrayList<RatingBreakdown> breakdown;
}

