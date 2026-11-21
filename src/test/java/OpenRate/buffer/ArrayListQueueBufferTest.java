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

package OpenRate.buffer;

/**
 * The default buffer, named by CommonConfig.DEFAULT_BUFFER_TYPE, so this is
 * the one almost every pipeline runs on.
 *
 * @author ian
 */
public class ArrayListQueueBufferTest extends AbstractQueueBufferTest {

  @Override
  protected IBuffer createBuffer() {
    return new ArrayListQueueBuffer();
  }
}
