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
