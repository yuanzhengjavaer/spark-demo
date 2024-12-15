package com.nri;

import org.apache.ignite.Ignition;

import java.io.FileNotFoundException;

public class IgniteEmbeddedNode {
    public static void main(String[] args) {

        Ignition.start("config/example-default-server.xml");

    }
}
