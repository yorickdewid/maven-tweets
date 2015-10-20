/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.test;

import com.hhscyber.nl.libsvm.svm;
import com.hhscyber.nl.libsvm.svm_model;
import com.hhscyber.nl.libsvm.svm_node;
import com.hhscyber.nl.libsvm.svm_parameter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 *
 * @author eve
 */
public class SVMPredict {

    private svm_model model;

    public SVMPredict(InputStream is) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        this.model = svm.svm_load_model(br);
    }

    public boolean predict(String line) throws IOException {
        int predict_probability = 0;

        int svm_type = svm.svm_get_svm_type(this.model);
        double[] prob_estimates = null;

        if (line == null) {
            return false;
        }

        StringTokenizer st = new StringTokenizer(line, " \t\n\r\f:");

        double target = atof(st.nextToken());
        int m = st.countTokens() / 2;
        svm_node[] x = new svm_node[m];
        for (int j = 0; j < m; j++) {
            x[j] = new svm_node();
            x[j].index = atoi(st.nextToken());
            x[j].value = atof(st.nextToken());
        }

        double v;
        if (predict_probability == 1 && (svm_type == svm_parameter.C_SVC || svm_type == svm_parameter.NU_SVC)) {
            v = svm.svm_predict_probability(model, x, prob_estimates);
        } else {
            v = svm.svm_predict(model, x);
        }

        return v == target;
    }

    private static double atof(String s) {
        return Double.valueOf(s).doubleValue();
    }

    private static int atoi(String s) {
        return Integer.parseInt(s);
    }
}
