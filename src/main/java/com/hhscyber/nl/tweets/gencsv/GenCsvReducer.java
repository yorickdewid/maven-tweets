package com.hhscyber.nl.tweets.gencsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author eve
 */
public class GenCsvReducer extends Reducer<ImmutableBytesWritable, Result, NullWritable, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        generateCsvFile(values, conf);
    }

    /**
     *
     * @param key
     * @param result
     */
    private static ArrayList<String> getKnownAndUnknownLocations(Result result) {
        //boolean bool_known = hbasehelper.HbaseHelper.createBooleanFromRawHbase(result, "location", "known");
        String city = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_city");
        String country = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_country");
        String category = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "category");
        String state = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_state");
        String county = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_county");
        String longitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_longitude");
        String latitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_latitude");
        String created_at = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "created_at_safe");

        ArrayList<String> tmp = new ArrayList<>();
        tmp.add(city);
        tmp.add(country);
        if (state.equals("")) {
            tmp.add(county);
        } else {
            tmp.add(state);
        }
        tmp.add(longitude);
        tmp.add(latitude);
        tmp.add(category);
        tmp.add(created_at);
        return tmp;
    }

    private static void generateCsvFile(Iterable<Result> values, Configuration conf) {
        try {
            StringBuilder sb = new StringBuilder();
            FileSystem hdfs = FileSystem.get(conf);
            Path baseOutputPath = new Path(conf.get("outputpath"));
            Path newFilePath = Path.mergePaths(baseOutputPath, new Path("/geo.csv"));
            try {
                //create column identifiers
                sb.append("city");
                sb.append(",");
                sb.append("country");
                sb.append(",");
                sb.append("state");
                sb.append(",");
                sb.append("longitude");
                sb.append(",");
                sb.append("latitude");
                sb.append(",");
                sb.append("category");
                sb.append(",");
                sb.append("created_at");
                sb.append("\n");

                //write data
                for (Result value : values) {
                    String text = hbasehelper.HbaseHelper.createStringFromRawHbase(value, "content", "text");
                    String verified = hbasehelper.HbaseHelper.createStringFromRawHbase(value, "profile", "verified");
                    ArrayList<String> locations = getKnownAndUnknownLocations(value);
                    for (String location : locations) {
                        if (location.equals("null")) {
                            sb.append("");
                        } else {
                            sb.append(location);
                        }
                        sb.append(",");
                    }
                    sb.append("\n");
                }
                byte[] byt = sb.toString().getBytes();

                try (FSDataOutputStream fsOutStream = hdfs.create(newFilePath)) {
                    fsOutStream.write(byt);
                }
                // nooit zomaar vage exceptions afvangen, bovenstaande gooit nooit een
                // exception, en bij outofmemory krijg je toch wel een stacktrace
            } catch (Exception ex) {
                Logger.getLogger(GenCsvReducer.class.getName()).log(Level.SEVERE, null, ex);
            }

        } catch (IOException ex) {
            Logger.getLogger(GenCsvReducer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
