package sics.seiois.mlsserver.biz.der.metanome.mlsel;

import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.operands.ColumnOperand;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSelection {
    private static final Logger log = LoggerFactory.getLogger(MLSelection.class);

    private double[][] coor;
    public int maxCol;
    private HashSet<String> mlsel;
    private boolean nomlsel;
    public static String concat = "<--prediction-->";
    public static String delimiter = ":";

    public MLSelection() {
        this.nomlsel = false;
    }

    public MLSelection(int maxCol) {
        this.maxCol = maxCol;
        this.coor = new double[maxCol][maxCol];
        for (int i = 0; i < maxCol; i++)
            for (int j = 0; j < maxCol; j++)
                this.coor[i][j] = 1.0; //false;

    }

    public double checkCorr(Predicate p_1, Predicate p_2) {
        if (coor == null) return 0.5;
        return 0.5;
    }

    public void readMLSelFile(String filename) {
        mlsel = new HashSet<>();
        if (filename == null) {
            return;
        }
        if ("nomlsel".equals(filename)) {
            this.nomlsel = true;
            return;
        }

        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(filename);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;

            while ( (line = br.readLine()) != null) {
                String[] kk = line.split(delimiter);
                if (kk.length > 1) {
                    for (int ii = 1; ii < kk.length; ii++) {
                        mlsel.add(kk[0].trim() + concat + kk[ii].trim());
                    }
                }
            }
            log.info("#### read predicate relation file success,mlsel.size()={}", mlsel.size());

        } catch (IOException e) {
            log.error("#### read predicate relation file exception", e);
        } finally{
            try {
                if(br != null){
                    br.close();
                }
            } catch (Exception e) {
                log.error("BufferedReader close error", e);
            }

            try {
                if(fr != null){
                    fr.close();
                }
            } catch (Exception e) {
                log.error("FileReader close error", e);
            }
        }
    }

    public void configure(String mlsel_file) {
        this.readMLSelFile(mlsel_file);
    }

    public boolean checkCoor(String lhs, String rhs) {

        if (this.nomlsel == true) {
            return true;
        }

        if (mlsel.contains(rhs + concat + lhs)) {
            return true;
        }
        return false;
    }

    public boolean checkCoor_bak(ColumnOperand lhs, ColumnOperand rhs) {
        String lhs_s = lhs.getColumn().getTableName() + "." + lhs.getColumn().getName();
        String rhs_s = rhs.getColumn().getTableName() + "." + rhs.getColumn().getName();
        return checkCoor(lhs_s, rhs_s);
    }

    public boolean checkCoor_bak(Predicate lhs, Predicate rhs) {
        return checkCoor_bak(lhs.getOperand1(), rhs.getOperand1()) &&
                checkCoor_bak(lhs.getOperand2(), rhs.getOperand2());
        /*
        return checkCoor(lhs.getOperand1(), rhs.getOperand1()) ||
            checkCoor(lhs.getOperand1(), rhs.getOperand2()) ||
            checkCoor(lhs.getOperand2(), rhs.getOperand1()) ||
            checkCoor(lhs.getOperand2(), rhs.getOperand2());
         */
    }

    public boolean checkCoor(Predicate lhs, Predicate rhs) {
        return this.checkCoor(lhs.toString().trim(), rhs.toString().trim());
    }

    public void set(ArrayList<Integer> lhss, int rhs) {
        for (int i = 0; i < lhss.size(); i++) {
            coor[rhs][i] = 1.0;
        }
    }

}
