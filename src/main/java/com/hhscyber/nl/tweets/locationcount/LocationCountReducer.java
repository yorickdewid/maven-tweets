package com.hhscyber.nl.tweets.locationcount;

import java.awt.Image;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class LocationCountReducer extends TableReducer<ImmutableBytesWritable, Result, Put> {

    public static countObject count = new countObject();

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result result : values) {
            getKnownAndUnknownLocations(result);
        }
    }

    /**
     *
     * @param key
     * @param result
     */
    private static void getKnownAndUnknownLocations(Result result) {
        boolean bool_known = hbasehelper.HbaseHelper.createBooleanFromRawHbase(result, "location", "known");
        String city = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "city");
        String county = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "county");
        String country = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "country");
        String state = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "state");
        String longitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "longitude");
        String latitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "location", "latitude");
        count.totalLocations++;
        if (bool_known) {
            count.knownLocations++;
        } else {
            count.unknownLocations++;
        }
        if (!city.equals("")) {
            count.amountOfCities++;
        }
        if (!county.equals("")) {
            count.amountOfCounties++;
        }
        if (!country.equals("")) {
            count.amountOfCountries++;
        }
        if (!state.equals("")) {
            count.amountOfState++;
        }
        if (!longitude.equals("") && !latitude.equals("")) {
            count.amountOfGeo++;
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println(count.toString());
        System.out.println("Percentage of known locations:" + (((double)count.knownLocations / (double)count.totalLocations) * 100.00));
        System.out.println(count.getOptionalFields());
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
