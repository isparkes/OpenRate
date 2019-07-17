
package OpenRate;

/**
 * Keys common to all applications
 *
 */
public class CommonConfig {

  /**
   * the key to the program name. the program name is used to retrieve the
   * configuration info
   */
  public static final String PROG_NAME = "OpenRate";

  /**
   * the key to the common environment name. the common environment name used to
   * store shared configuration data.
   */
  public static final String ALL_ENV = "common";

  /**
   * This is the main key for retrieving the module symbolic name, which is used
   * in the configuration processing for the module stack
   */
  public static final String MODULE_SYMBOLIC_NAME = "ModuleName";

  /**
   * The OpenRate FIFO buffer used between plugins to pass records down the
   * pipeline. Normally you will not need to change this and can leave it as the
   * default value. If you want to change this, you can set the pipeline
   * configuration property defined by this tag.
   */
  public static final String BUFFER_TYPE = "BufferClassName";

  /**
   * The default FIFO buffer implementation to use, in the case that no other
   * value has been set. This buffer type works well in all situations. Only
   * specific situations where you wish to enforce special ordering rules or
   * other strange requirements will lead you to change this.
   */
  public static final String DEFAULT_BUFFER_TYPE = "OpenRate.buffer.ArrayListQueueBuffer";

  /**
   * Output adapter performance setting. This specifies the configuration
   * property name for setting the number of threads to allocate for output
   * handling. ** Currently not implemented **
   */
  public static final String OA_MAX_THREAD_CNT = "OutputAdapter.ThreadCount";

  /**
   * Output adapter performance setting. This specifies the default number of
   * threads to use for each output adapter. ** Currently set by default to 1 **
   */
  public static final String OA_THREAD_DEFAULT = "1";

  /**
   * This defines the property key for the maximum sleep time for an output
   * adapter.
   */
  public static final String MAX_SLEEP = "MaxSleep";

  /**
   * this defines the default value for the maximum sleep time for an output
   * adapter.
   */
  public static final String DEFAULT_MAX_SLEEP = "50";

  /**
   * key used by ResourceContext to find the Data Cache controller & used by
   * clients to retrieve the currently loaded Controller
   */
  public static final String DATA_CACHE_KEY = "Resource.DataCacheController";

  /**
   * The key to use in the properties file for getting the configured batch
   * size.
   */
  public static final String BATCH_SIZE = "BatchSize";

  /**
   * This defines the default block size to request from the supplier FIFO on
   * each request. The default value works well for batch processing
   * applications. If you are more interested in real time applications, you
   * should reduce this value.
   */
  public static final String DEFAULT_BATCH_SIZE = "5000";

  /**
   * This defines the key used for setting the buffer size.
   */
  public static final String BUFFER_SIZE = "BufferSize";

  /**
   * This defines the default maximum number of records that a buffer can hold
   * before it starts to postpone the acceptance of new records (which of course
   * has an impact on processing performance).
   *
   * This is primarily used to limit the amount of memory consumed by records in
   * a processing pipeline, where the processing chain is not well balanced.
   */
  public static final String DEFAULT_BUFFER_SIZE = "10000";

  /**
   * Defines the properties key for the number of threads to use for a
   * processing plugin.
   */
  public static final String NUM_PROCESSING_THREADS = "Threads";

  /**
   * Defines the default value for the number of threads to use for a processing
   * module by default.
   */
  public static final String NUM_PROCESSING_THREADS_DEFAULT = "1";

  /**
   * Defines the response string for the ECI when a non-dynamic parameter is
   * changed
   */
  public static final String NON_DYNAMIC_PARAM = "This parameter cannot be set at run time";

  /**
   * The active setting is used to read the active status of modules from the
   * configuration. If modules are not active, they do no work in the
   * processing.
   */
  public static final String ACTIVE = "Active";

  /**
   * The default active status for a dump file.
   */
  public static final String DEFAULT_ACTIVE = "True";

  /**
   * Defines the default UTC value of a low date (big bang)
   */
  public static final long LOW_DATE = 0L;

  /**
   * Defines the default UTC value of a high date (end of the universe)
   */
  public static final long HIGH_DATE = 10413792000L;

  /**
   * Defines the default date format for OpenRate
   */
  public static final String OR_DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  /**
   * Module statistics command
   */
  public static final String STATS = "Stats";

  /**
   * Module statistics reset command
   */
  public static final String STATS_RESET = "StatsReset";

}
