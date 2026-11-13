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

package OpenRate.process;

import OpenRate.record.IRecord;

/**
 * AbstractStublPlugIn hides the procHeader and ProcTrailer methods that are
 * largely irrelevant when dealing with non-transactional processing.
 *
 * It is intended that custom plug-ins be based on this.
 */
public abstract class AbstractStubPlugIn extends AbstractPlugIn {

  /**
   * Stub out the procHeader
   *
   * @param r The header record
   * @return The unmodified header record
   */
  @Override
  public IRecord procHeader(IRecord r) {
    return r;
  }

  /**
   * Stub out the procTrailer
   *
   * @param r The trailer record
   * @return The unmodified trailer record
   */
  @Override
  public IRecord procTrailer(IRecord r) {
    return r;
  }

}
