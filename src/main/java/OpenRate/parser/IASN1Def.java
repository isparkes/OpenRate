/* ====================================================================
 * Limited Evaluation License:
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
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: IASN1Def.java,v $, $Revision: 1.8 $, $Date: 2013-05-13 18:12:12 $";

  /**
   * The ASN.1 definition class
   */
  public class ASN1TypeClass {
    /**
     * The tag value
     */
    public int      tag;

    /**
     * The tag name
     */
    public String   name;

    /**
     * The tag length
     */
    public int      length;

    /**
     * The tag type
     */
    public int      type;

    /**
     * The tag block key
     */
    public int      blockKey;

    /**
     * Create a new ASN.1 type class
     *
     * @param tag The tag id
     * @param name The tag name
     * @param type The tag type
     * @param blockKey The block key
     * @param length The length
     */
    public ASN1TypeClass(int tag, String name, int type, int blockKey, int length) {
        this.tag = tag;
        this.name = name;
        this.length = length;
        this.type = type;
        this.blockKey = blockKey;
      }
    }

  /**
   * Init
   */
  public void initASN1();

  /**
   * Initialse the tags
   */
  public void initTags();

  /**
   * Get the tag name
   *
   * @param tagId The id of the tag to get
   * @return The name
   */
  public String getTagName(int tagId);

  /**
   * Get the length of a tag
   *
   * @param tagId The id of the tag to get
   * @return Th length
   */
  public int getLength(int tagId);

  /**
   * Get the type of a tag
   *
   * @param tagId The id of the tag to get
   * @return The type
   */
  public int getType(int tagId);

  /**
   * Get the type name of a tag
   *
   * @param tagId The id of the tag to get
   * @return The type name
   */
  public String getTypeName(int tagId);
}
