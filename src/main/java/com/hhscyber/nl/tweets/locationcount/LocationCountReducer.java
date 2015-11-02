package com.hhscyber.nl.tweets.locationcount;

import java.io.IOException;
import java.util.ArrayList;
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
    public static ArrayList<countryObject> countries = new ArrayList<>();

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
        boolean bool_known = hbasehelper.HbaseHelper.createBooleanFromRawHbase(result, "content", "location_known");
        String city = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_city");
        String county = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_county");
        String country = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_country");
        String state = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_state");
        String longitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_longitude");
        String latitude = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "location_latitude");
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
            int index = -1;
            for (countryObject countryObj : countries) {
                if (countryObj.countryName.toLowerCase().equals(country.toLowerCase())) {
                    index = countries.indexOf(countryObj);
                }
            }
            if (index != -1) {
                countries.get(index).count++;
            } else {
                countryObject tmpobj = new countryObject();
                tmpobj.countryName = country;
                tmpobj.count++;
                countries.add(tmpobj);
            }
            count.amountOfCountries++;
        }
        if (!state.equals("")) {
            count.amountOfState++;
        }
        if (!longitude.equals("0.0") && !latitude.equals("0.0")) {
            count.amountOfGeo++;
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println(count.toString());
        System.out.println("Percentage of known locations:" + (((double) count.knownLocations / (double) count.totalLocations) * 100.00));
        System.out.println(count.getOptionalFields());
        for (countryObject obj : countries) {
            System.out.println("Country : " + obj.countryName + " , " + obj.count);
        }
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
