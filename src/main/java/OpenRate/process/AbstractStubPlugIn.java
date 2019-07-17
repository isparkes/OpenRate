
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
