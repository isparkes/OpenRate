

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

  private Node               root;
  private ArrayList<String>  nullResultList;
  private int                nodeCount           = 0;

 /**
  * Default constructor - sets up the root node
  */
  public DigitTree()
  {
    // Set up the root node so that it returns NOMATCH by default
    root = new Node();
    
    // Set up the null node return result
    nullResultList = new ArrayList();
    nullResultList.add(NO_DIGIT_TREE_MATCH);
  }

 /**
  * Add a prefix to the digit tree.
  *
  * @param prefix The prefix to add to the digit tree
  * @param resultList The results to return for this tag
  */
  public void addPrefix(String prefix, ArrayList<String> resultList)
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

    node.Results = resultList;
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param prefix The prefix to match
  * @return The short result to return in the case of a match
  */
  public String match(String prefix)
  {
    char[] numberChars = prefix.toCharArray();

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

    // return the best match we got - sometimes this is no match at all
    if (bestNode.Results == null)
    {
      return NO_DIGIT_TREE_MATCH;
    }
    else
    {
      return bestNode.Results.get(0);
    }
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param prefix The prefix to match
  * @return The results list to return in the case of a match
  */
  public ArrayList<String> matchWithChildData(String prefix)
  {
    char[] numberChars = prefix.toCharArray();

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
    // return the best match we got - sometimes this is no match at all
    if (bestNode.Results == null)
    {
      return nullResultList;
    }
    else
    {
      return bestNode.Results;
    }
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
