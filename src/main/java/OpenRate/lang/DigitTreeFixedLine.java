

package OpenRate.lang;

import java.util.ArrayList;

/**
 * Class to build and search a tree list, for example in best match searches
 */
public class DigitTreeFixedLine
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
  public DigitTreeFixedLine()
  {
  }

 /**
  * Add a prefix pair to the digit tree.
  *
  * @param ANum The A prefix to add to the digit tree
  * @param BNum The B prefix to add to the digit tree
  * @param Results The results to return for this tag
  */
  public void addPrefix(String ANum, String BNum, ArrayList<String> Results)
  {
    char[] numberChars = ANum.toCharArray();

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
    node.assBNum = BNum;
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param ANum The A prefix to add to the digit tree
  * @param BNum The B prefix to add to the digit tree
  * @return Results The results to return for this tag
  */
  public String match(String ANum, String BNum)
  {
    int partialIndex;
    Node tmpNode;

    // This will store the B number results we match on the way
    ArrayList<Node> partialResults = new ArrayList<>();

    // This is the prefix to search
    char[] numberChars = ANum.toCharArray();

    Node   node     = root;
    Node   bestNode = root;

    for (int i = 0; i != numberChars.length; i++)
    {
      int number = numberChars[i] - '0';

      if (node.children[number] == null)
      {
        // finished the partial checking, now evaluate the b number parts
        for (partialIndex = partialResults.size() - 1 ; partialIndex >= 0 ; partialIndex--)
        {
          tmpNode = partialResults.get(partialIndex);
          if (BNum.startsWith(tmpNode.assBNum))
          {
            return tmpNode.Results.get(0);
          }
        }

        return bestNode.Results.get(0);
      }

      node         = node.children[number];
      if (node.Results != null)
      {
        bestNode   = node;
        partialResults.add(node);
      }
    }

    // No match found, so return the default root value
    return bestNode.Results.get(0);
  }

 /**
  * Work down the digit tree to find the best match. We remember the previous
  * best result as we go.
  *
  * @param ANum The A prefix to add to the digit tree
  * @param BNum The B prefix to add to the digit tree
  * @return Results The results to return for this tag
  */
  public ArrayList<String> matchWithChildData(String ANum, String BNum)
  {
    int partialIndex;
    Node tmpNode;

    // This will store the B number results we match on the way
    ArrayList<Node> partialResults = new ArrayList<>();

    // This is the prefix to search
    char[] numberChars = ANum.toCharArray();

    Node   node     = root;
    Node   bestNode = root;

    for (int i = 0; i != numberChars.length; i++)
    {
      int number = numberChars[i] - '0';

      if (node.children[number] == null)
      {
        // finished the partial checking, now evaluate the b number parts
        for (partialIndex = partialResults.size() - 1 ; partialIndex >= 0 ; partialIndex--)
        {
          tmpNode = partialResults.get(partialIndex);
          if (BNum.startsWith(tmpNode.assBNum))
          {
            return tmpNode.Results;
          }
        }

        return bestNode.Results;
      }

      node         = node.children[number];
      if (node.Results != null)
      {
        bestNode   = node;
        partialResults.add(node);
      }
    }

    // No match found, so return the default root value
    return bestNode.Results;
  }

  private class Node
  {
    private ArrayList<String> Results = null;
    private String            assBNum = "";
    private Node[]            children = new Node[10];
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
