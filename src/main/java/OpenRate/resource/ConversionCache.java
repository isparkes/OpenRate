/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

package OpenRate.resource;

import OpenRate.exception.InitializationException;
import OpenRate.utils.ConversionUtils;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ConversionCache provides access to conversion objects, with the aim of
 * making record objects lighter by re-use of shared conversion objects. The
 * conversion object is particularly heavy during creation, and is used often.
 * This cache therefore gives a reasonable increase in performance.
 */
public class ConversionCache implements IResource
{
  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "ConversionCache";

  // This is the symbolic name of the resource
  private String symbolicName;

  // cache Categories
  private static ConcurrentHashMap<String, ConversionUtils> ConversionManagers = new ConcurrentHashMap<>();

  /**
   * default constructor - protected
   */
  public ConversionCache()
  {
    super();
  }

  /**
   * Perform whatever initialization is required of the resource.
   * This method should only be called once per application instance.
   *
   * @param ResourceName The name of the resource in the properties
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    if (ResourceName.equals(RESOURCE_KEY) == false)
    {
      throw new InitializationException("Conversion Cache must be called ConversionCache",getSymbolicName());
    }
    else
    {
      symbolicName = ResourceName;
    }
  }

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close()
  {
    ConversionManagers.clear();
  }

  /**
   * Utility to return the reference to the conversion resource
   *
   * @param type The type string of the conversion object to return
   * @return The conversion object
   * @throws ConfigurationException
   */
  public ConversionUtils getConversionObject(String type)
  {
    if (ConversionManagers.containsKey(type))
    {
      // just return it
      return ConversionManagers.get(type);
    }
    else
    {
      // create it and return it
      ConversionUtils tmpConv = new ConversionUtils();
      ConversionManagers.put(type, tmpConv);
      return tmpConv;
    }
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
