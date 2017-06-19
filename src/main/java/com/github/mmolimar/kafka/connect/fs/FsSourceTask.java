package com.github.mmolimar.kafka.connect.fs;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class FsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new FsSourceTaskConfig(properties);

            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + "is not a sublass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + "is not a sublass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(properties);
            policy = ReflectionUtils.makePolicy(policyClass, taskConfig);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start FsSourceConnector:", t);
            throw new ConnectException("A problem has occurred reading configuration:" + t.getMessage());
        }

        stop = new AtomicBoolean(false);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (stop != null && !stop.get() && !policy.hasEnded()) {
            log.trace("Polling for new data");

            final List<SourceRecord> results = new ArrayList<>();
            List<FileMetadata> files = filesToProcess();
            files.forEach(metadata -> {
                try {
                    log.info("Processing records for file {}", metadata);
                    FileReader reader = policy.offer(metadata, context.offsetStorageReader());
                    while (reader.hasNext()) {
                        Struct readerStruct = reader.next();
                        String topic        = getTopic(readerStruct);

                        results.add(convert(metadata, reader.currentOffset(), readerStruct, topic));
                    }
                } catch (ConnectException | IOException e) {
                    //when an exception happens reading a file, the connector continues
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                }
            });
            return results;
        }

        return null;
    }

    private List<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0)
                    .collect(Collectors.toList());
        } catch (IOException | ConnectException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrive files to process from FS: " + policy.getURIs() + ". Keep going...", e);
            return Collections.EMPTY_LIST;
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, Offset offset, Struct struct, String topic) {
        return new SourceRecord(
                new HashMap<String, Object>() {
                    {
                        put("path", metadata.getPath());
                        //TODO manage blocks
                        //put("blocks", metadata.getBlocks().toString());
                    }
                },
                Collections.singletonMap("offset", offset.getRecordOffset()),
                topic,
                struct.schema(),
                struct
        );
    }

    private String getTopic(Struct message) {
        JsonParser jsonParser = new JsonParser();
        StringReader reader   = new StringReader(message.get("value").toString());
        JsonReader jsonReader = new JsonReader(reader);
        jsonReader.setLenient(true);

        JsonObject messageObject = (JsonObject) jsonParser.parse(jsonReader);
        String     topicSuffix   = "." + messageObject.get("network").getAsString() +
                                   "." + messageObject.get("tag").getAsString();

        return config.getTopicPrefix() + topicSuffix;
    }

    @Override
    public void stop() {
        if (stop != null) {
            stop.set(true);
        }
        if (policy != null) {
            policy.interrupt();
        }
    }
}