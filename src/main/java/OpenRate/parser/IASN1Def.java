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
package OpenRate.parser;

/**
 * Definition of ASN1 types and interface, used for abstraction of ASN.1
 * specifications.
 *
 * @author Magnus
 */
public interface IASN1Def
{
  /**
   * Init
   */
  public void initASN1();

  /**
   * Initialse the tags
   */
  public void initTags();

  /**
   * Get the tag name for the path of the tag. Note that we don't usually do
   * this directly on the individual tag id of the element, although theoretically
   * this is the right way. Many formats re-use tags within structures, and
   * therefore we have to know the whole path before we can resolve the
   * exact tag meaning.
   *
   * @param tagId The id of the tag to get
   * @return The name
   */
  public String getTagName(String tagId);

  /**
   * Get the type of a tag
   *
   * @param tagId The id of the tag to get
   * @return The type
   */
  public int getTagType(String tagId);
}
