package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;


public class FsSourceConnectorConfig extends AbstractConfig {

    public static final String FS_URIS = "fs.uris";
    private static final String FS_URIS_DOC = "Comma-separated URIs of the FS(s).";

    public static final String TOPIC_PREFIX = "topic.prefix";
    private static final String TOPIC_DOC = "Topic to copy data to.";

    public static final String REPLAY_FILE = "policy.fs.replayFile";
    private static final String REPLAY_FILE_DOC = "T/F depending on if we are replaying a file.";

    public FsSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FS_URIS, Type.LIST, Importance.HIGH, FS_URIS_DOC)
                .define(TOPIC_PREFIX, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(REPLAY_FILE, Type.BOOLEAN, Importance.HIGH, REPLAY_FILE_DOC);
    }

    public List<String> getFsUris() {
        return this.getList(FS_URIS);
    }

    public String getTopicPrefix() {
        return this.getString(TOPIC_PREFIX);
    }

    public Boolean replayFile() {
        return this.getBoolean(REPLAY_FILE);
    }
}