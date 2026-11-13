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


package OpenRate.transaction;

/**
 * The ISyncPoint interface allows the interaction with the framework for the
 * purpose of managing sync points
 */
public interface ISyncPoint
{
 /**
  * Sync status when the pipeline is running normally
  */
  public static final int SYNC_STATUS_NORMAL_RUN = 0;

 /**
  * Sync status when one of the modules has flagged a sync request
  */
  public static final int SYNC_STATUS_SYNC_FLAGGED = 1;

 /**
  * Sync status when the all modules have accepted the sync request
  */
  public static final int SYNC_STATUS_SYNC_REQUESTED = 2;

 /**
  * Sync status when all pipelines have stopped current transactions
  */
  public static final int SYNC_STATUS_SYNC_REACHED = 3;

 /**
  * Sync status when the command is being processed
  */
  public static final int SYNC_STATUS_SYNC_PROCESSING = 4;

 /**
  * Sync status when the sync processing has finished and we are ready to go
  */
  public static final int SYNC_STATUS_SYNC_FINISHED = 5;

 /**
  * This is the interface that is used to control the sync point processing.
  *
  * @return The current sync status
  */
  public int getSyncStatus();

 /**
  * This is the interface that is used to control the sync point processing
  *
  * @param newStatus The new sync status
  */
  public void setSyncStatus(int newStatus);

 /**
  * This is the interface that is used to control the sync point processing
  *
  * @return The symbolic name
  */
  public String getSymbolicName();
}
