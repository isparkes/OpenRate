/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

package OpenRate.lang;

import java.util.ArrayList;

/**
 * Class to build and search a tree list, for example in best match searches
 */
public class DigitTree
{
  /**
   * The default return when there is no digit tree match
   */
  public static final String NO_DIGIT_TREE_MATCH = "NOMATCH";

  private Node               root                = new Node();
  private int                nodeCount           = 0;

 /**
  * Default constructor
  */
  public DigitTree()
  {
  }

 /**
  * Add a prefix to the digit tree.
  *
  * @param prefix The prefix to add to the digit tree
  * @param Results The results to return for this tag
  */
  public void addPrefix(String prefix, ArrayList<String> Results)
  {
    char[] numberChars = prefix.toCharArray();

    Node   node        = root;

    for (int i = 0; i < numberChars.length; i++)
    {
      int number = numberChars[i] - '0';

      if (node.children[number] == null)
      {
        node.children[number] = new Node();
        nodeCount++;
      }

      node = node.children[number];
    }

    node.Results = Results;
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param phonenumber The prefix to match
  * @return The short result to return in the case of a match
  */
  public String match(String phonenumber)
  {
    char[] numberChars = phonenumber.toCharArray();

    Node   node      = root;
    Node   bestNode  = root;

    for (int i = 0; i != numberChars.length; i++)
    {
      int number = numberChars[i] - '0';

      if (node.children[number] == null)
      {
        // No more children - return what we have got so far
        break;
      }

      node         = node.children[number];
      if (node.Results != null)
      {
        bestNode   = node;
      }
    }

    return bestNode.Results.get(0);
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param phonenumber The prefix to match
  * @return The results list to return in the case of a match
  */
  public ArrayList<String> matchWithChildData(String phonenumber)
  {
    char[] numberChars = phonenumber.toCharArray();

    Node   node        = root;
    Node   bestNode    = root;

    for (int i = 0; i != numberChars.length; i++)
    {
      int number = numberChars[i] - '0';

      if (node.children[number] == null)
      {
        // No more children - return what we have got so far
        break;
      }

      node         = node.children[number];
      if (node.Results != null)
      {
        bestNode   = node;
      }
    }

    // No match found, so return the default root value
    return bestNode.Results;
  }

 /**
  * Definition of the node object
  */
  private class Node
  {
    private ArrayList<String> Results = null;
    private Node[] children = new Node[10];
  }

  /**
   * Get the number of elements in the cache
   *
   * @return The number of elements
   */
  public int size()
  {
    return nodeCount;
  }
}
