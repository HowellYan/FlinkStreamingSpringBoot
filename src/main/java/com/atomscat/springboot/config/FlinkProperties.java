package com.atomscat.springboot.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "flink-properties")
@Data
public class FlinkProperties {

    @NotNull
    private String jobName;

    @NotNull
    private String jobManagerUrl;

    private int jobManagerPort;

    @NotNull
    private List<String> remoteEnvJarFiles;

    private long maxClientRestRequestSizeBytes;

    private boolean terminate;

    private long terminationGracePeriodMs;
}
