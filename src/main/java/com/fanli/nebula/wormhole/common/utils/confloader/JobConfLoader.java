package com.fanli.nebula.wormhole.common.utils.confloader;

import com.fanli.nebula.wormhole.common.config.JobConf;

/**
 * Created by zcfrank1st on 3/18/15.
 */
public interface JobConfLoader {
    boolean initLoader();
    JobConf loadJobConf(String... args);
}
