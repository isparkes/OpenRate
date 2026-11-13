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
 * This holds the information about the various segments involved in a time match
 * in the case that we pass from one band to another.
 *
 * @author afzaal
 */
public class TimePacket
{
  /**
   * Used for numbering the packets
   */
  public int packetNumber;
  
 /**
  * The day of week of the segment
  */
  public int dayofWeek;

 /**
  * The start time (hours * 60 + minutes) of the segment
  */
  public int startTime;

 /**
  * The start second of the segment
  */
  public int startSecond;

 /**
  * The end time (hours * 60 + minutes) of the segment
  */
  public int endTime;

 /**
  * The end second of the segment
  */
  public int endSecond;

 /**
  * The duration (seconds) second of the segment
  */
  public int duration;

 /**
  * The total original duration of the event
  */
  public int totalDuration;

 /**
  * The time model applied to the segment
  */
  public String timeModel;

 /**
  * The time result found for the segment
  */
  public String timeResult;
  
 /**
  * The selected price model group
  */
  public String  priceGroup = null;

 /**
  * The selected price model
  */
  public String  priceModel = null;

  public TimePacket(TimePacket toClone) {
    this.packetNumber = toClone.packetNumber;
    this.dayofWeek = toClone.dayofWeek;
    this.duration = toClone.duration;
    this.startTime = toClone.startTime;
    this.startSecond = toClone.startSecond;
    this.endTime = toClone.endTime;
    this.endSecond = toClone.endSecond;
    this.timeModel = toClone.timeModel;
    this.timeResult = toClone.timeResult;
    this.totalDuration = toClone.totalDuration;
    this.priceGroup = toClone.priceGroup;
    this.priceModel = toClone.priceModel;
  }
  
  public TimePacket() {
  }
  
  /**
   * Create a clone of this charge packet
   *
   * @return The cloned Charge Packet
   */
  public TimePacket Clone() {
    return new TimePacket(this);
  }

}
