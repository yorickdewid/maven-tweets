/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.location2;

import java.util.ArrayList;

/**
 *
 * @author dev
 */
public class LocationObject {

    Double[][][] geo = null;
    ArrayList<String> cities = null;
    ArrayList<String> counties = null;
    ArrayList<String> countries = null;
    ArrayList<String> states = null;
    ArrayList<Boolean> known = null;

    public Double[][][] getGeo() {
        return geo;
    }

    public void setGeo(Double[][][] geo) {
        this.geo = geo;
    }

    public ArrayList<String> getCities() {
        return cities;
    }

    public void setCities(ArrayList<String> cities) {
        this.cities = cities;
    }

    public ArrayList<String> getCounties() {
        return counties;
    }

    public void setCounties(ArrayList<String> counties) {
        this.counties = counties;
    }

    public ArrayList<String> getStates() {
        return states;
    }

    public void setStates(ArrayList<String> states) {
        this.states = states;
    }

    public ArrayList<Boolean> getKnown() {
        return known;
    }

    public void setKnown(ArrayList<Boolean> known) {
        this.known = known;
    }

    public void setCountries(ArrayList<String> countries) {
        this.countries = countries;
    }

    public ArrayList<String> getCountries() {
        return countries;
    }

}
