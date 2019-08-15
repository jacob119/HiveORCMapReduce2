package org.jacob.hivemr.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Map;


public class Utils {
    public static void deleteDirectory(Configuration conf, String path) throws IOException {
        Path output = new Path(path);
        FileSystem hdfs = FileSystem.get(URI.create("fs.defaultFS"), conf);
        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
    }

    public static void printConf(Configuration conf){
        for (Map.Entry<String, String> stringStringEntry : conf) {
            System.out.println(stringStringEntry.getKey() + " = " + stringStringEntry.getValue());
        }
    }
}
