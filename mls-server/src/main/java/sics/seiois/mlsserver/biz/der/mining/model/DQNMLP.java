package sics.seiois.mlsserver.biz.der.mining.model;

import org.ejml.simple.SimpleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
    two-hidden layers MLP
 */
public class DQNMLP {
    private int feature_num;
    private int action_num;

    public int getFeature_num() {
        return this.feature_num;
    }

    public int getAction_num() {
        return this.action_num;
    }

    // matrix
    //private SimpleMatrix matrix_1;
    //private SimpleMatrix matrix_2;
    private List<SimpleMatrix> matrices;

    public DQNMLP(int feature_num, int action_num, List<double[][]> hiddenMatrices) {
        this.feature_num = feature_num;
        this.action_num = action_num;
        this.matrices = new ArrayList<>();
        for (int i = 0; i < hiddenMatrices.size(); i++) {
            double[][] mat = hiddenMatrices.get(i);
            SimpleMatrix matrix = new SimpleMatrix(mat);
            this.matrices.add(matrix);
        }
    }

    public DQNMLP(String model_path) {
        this.matrices = new ArrayList<>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(model_path);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;
            boolean beginMatrix = true;
            while ( (line = br.readLine()) != null) {
                if (line.trim().equals("")) {
                    continue;
                }
                int row = 0, col = 0;
                if(beginMatrix) {
                    String[] info = line.split(" ");
                    row = Integer.parseInt(info[0].trim());
                    col = Integer.parseInt(info[1].trim());
                }
                double[][] mat = new double[row][col];
                for (int r = 0; r < row; r++) {
                    line = br.readLine();
                    String[] info = line.split(" ");
                    for (int c = 0; c < info.length; c++) {
                        mat[r][c] = Double.parseDouble(info[c].trim());
                    }
                }
                SimpleMatrix matrix = new SimpleMatrix(mat);
                this.matrices.add(matrix);
            }
            // the input and ouput dimensions of DQN
            this.feature_num = this.matrices.get(0).numRows();
            this.action_num = this.matrices.get(this.matrices.size() - 1).numCols();

        } catch (IOException e) {
            logger.error("IOException error of model", e);
        }
    }

    public DQNMLP(int feature_num, int action_num, String model_path) {
        this.feature_num = feature_num;
        this.action_num = action_num;
        this.matrices = new ArrayList<>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(model_path);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;
            boolean beginMatrix = true;
            while ( (line = br.readLine()) != null) {
                int row = 0, col = 0;
                if(beginMatrix) {
                    String[] info = line.split(" ");
                    row = Integer.parseInt(info[0].trim());
                    col = Integer.parseInt(info[1].trim());
                }
                double[][] mat = new double[row][col];
                for (int r = 0; r < row; r++) {
                    line = br.readLine();
                    String[] info = line.split(" ");
                    for (int c = 0; c < info.length; c++) {
                        mat[r][c] = Double.parseDouble(info[c].trim());
                    }
                }
                SimpleMatrix matrix = new SimpleMatrix(mat);
                this.matrices.add(matrix);
            }

        } catch (IOException e) {
            logger.error("IOException error of model", e);
        }
    }

    /**
     * Applies ReLU to each of the entries in the matrix.  Returns a new matrix.
     */
    private SimpleMatrix ReLU(SimpleMatrix input) {
        SimpleMatrix output = new SimpleMatrix(input);
        for (int i = 0; i < output.numRows(); ++i) {
            for (int j = 0; j < output.numCols(); ++j) {
                output.set(i, j, Math.max(0, output.get(i, j)));
            }
        }
        return output;
    }

    /*
        input: [num_of_input, feature_num (2 * num_of_predicates)]
        output: [num_of_input, num_action], each element is the Q-value of P_next
     */
    public double[][] run(double[][] features) {
        SimpleMatrix input = new SimpleMatrix(features);
        SimpleMatrix temp = input;
        for(int i = 0; i < this.matrices.size() - 2; i+=2) {
            temp = temp.mult(this.matrices.get(i));
            //logger.info("The dimension 1 of temp is {} X {}", temp.numRows(), temp.numCols());
            temp = temp.plus(this.matrices.get(i+1));
            //logger.info("The dimension 2 of temp is {} X {}", temp.numRows(), temp.numCols());
            temp = this.ReLU(temp);
        }
        temp = temp.mult(this.matrices.get(this.matrices.size() - 2));
        temp = temp.plus(this.matrices.get(this.matrices.size() - 1));

        double[][] Qvalues = new double[temp.numRows()][temp.numCols()];
        for (int r = 0; r < temp.numRows(); r++) {
            for (int c = 0; c < temp.numCols(); c++) {
                Qvalues[r][c] = temp.get(r, c);
            }
        }
        return Qvalues;
    }

    private static final Logger logger = LoggerFactory.getLogger(DQNMLP.class);
}
