package sics.seiois.mlsserver.biz.der.mining.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class DataTransfer implements Serializable {

    private static final long serialVersionUID = 771087336268486077L;
    public static transient HashMap<String, ParsedColumnLight<?>> parsedColumnLightHashMap = new HashMap<>();
    public static transient HashMap<String, ArrayList<ImmutablePair<Integer, Integer>>> mlData = new HashMap<>();


}
