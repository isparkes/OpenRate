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

package OpenRate.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

/**
 * A file name filter that matches names against a shell style glob pattern.
 *
 * This replaces the Jakarta ORO GlobFilenameFilter that OpenRate used
 * previously, and keeps ORO's STAR_CANNOT_MATCH_NULL semantics: a '*' must
 * match at least one character, so "pref*suf" does not match "prefsuf".
 *
 * Supported wildcards:
 * <ul>
 * <li>'*' - one or more characters</li>
 * <li>'?' - exactly one character</li>
 * </ul>
 * All other characters are matched literally.
 */
public class GlobFilenameFilter implements FilenameFilter {

  private final Pattern pattern;

  /**
   * Create a filter for the given glob expression.
   *
   * @param glob the glob expression, for example "input_*.dat"
   */
  public GlobFilenameFilter(String glob) {
    this.pattern = Pattern.compile(globToRegex(glob));
  }

  /**
   * Test whether the given file name matches the glob.
   *
   * @param dir the directory holding the file (not used)
   * @param name the file name to test
   * @return true if the name matches the glob
   */
  @Override
  public boolean accept(File dir, String name) {
    return (name != null) && pattern.matcher(name).matches();
  }

  /**
   * Translate a glob expression into an equivalent regular expression.
   *
   * @param glob the glob expression
   * @return the regular expression source
   */
  private static String globToRegex(String glob) {
    StringBuilder regex = new StringBuilder("^");

    for (int idx = 0; idx < glob.length(); idx++) {
      char current = glob.charAt(idx);

      switch (current) {
        case '*':
          // ORO's STAR_CANNOT_MATCH_NULL_MASK: '*' needs at least one char
          regex.append(".+");
          break;
        case '?':
          regex.append('.');
          break;
        default:
          // Everything else is literal, so escape any regex metacharacter
          if ("\\.[]{}()<>+-=!|^$".indexOf(current) >= 0) {
            regex.append('\\');
          }
          regex.append(current);
          break;
      }
    }

    return regex.append('$').toString();
  }
}
