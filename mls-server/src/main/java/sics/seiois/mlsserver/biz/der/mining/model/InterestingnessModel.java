package sics.seiois.mlsserver.biz.der.mining.model;

import de.metanome.algorithm_integration.Operator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ejml.simple.SimpleMatrix;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/*
    implement the java code of rule interestingness model
    input: one rule
 */
public class InterestingnessModel implements Serializable {

    private HashMap<String, Integer> vobsHash;

    private SimpleMatrix tokensEmbedMatrix;             // shape (vob_size, token_embedding_size)
    private SimpleMatrix weightPredicates;              // shape (3, token_embedding_size)
    private SimpleMatrix weightREEsEmbed;               // shape (token_embedding_Size * 2, rees_embedding_size)
    private SimpleMatrix weightInterest;                // shape (ree_embedding_size, 1)
    private SimpleMatrix weightObjSubj;                  // shape (4, 1)
    private SimpleMatrix weightUBSubj;                  // shape (1, 1)

    static public int UNIT_PREDICATE = 5;
    static public int MAX_LHS_PREDICATES = 10;
    static public int MAX_RHS_PREDICATES = 1;
    static public String PADDING_VALUE = "PAD";

    private void loadModel(String modelPAthFile) throws  IOException {
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(modelPAthFile));
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);

        ArrayList<SimpleMatrix> matrices = new ArrayList<>();
        String line = null;
        boolean beginMatrix = true;
        while ( (line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            int row = 0, col = 0;
            if (beginMatrix) {
                String[] info = line.split(" ");
                row = Integer.parseInt(info[0].trim());
                col = Integer.parseInt(info[1].trim());
            }
            double[][] mat = new double[row][col];
            for (int r = 0; r <row; r++) {
                line = bReader.readLine();
                String[] info = line.split(" ");
                for (int c = 0; c < info.length; c++) {
                    mat[r][c] = Double.parseDouble(info[c].trim());
                }
            }
            SimpleMatrix matrix = new SimpleMatrix(mat);
            matrices.add(matrix);
        }
        // assign to matrices
        this.tokensEmbedMatrix = matrices.get(0);
        this.weightPredicates = matrices.get(1);
        this.weightREEsEmbed = matrices.get(2);
        this.weightInterest = matrices.get(3);
        this.weightObjSubj = matrices.get(4);
        this.weightUBSubj = matrices.get(5);
        // weights of features are non-negative
        this.weightObjSubj = this.weightObjSubj.elementMult(this.weightObjSubj);
        this.weightUBSubj = this.weightUBSubj.elementMult(this.weightUBSubj);
    }

    /*
        read "tokensEmbedMatrix", "weightPredicates", "weightREEsEmbed", "weightInterest" one by one
     */
    private void loadModel(String modelPathFile, FileSystem hdfs) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(modelPathFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        ArrayList<SimpleMatrix> matrices = new ArrayList<>();
        String line = null;
        boolean beginMatrix = true;
        while ( (line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            int row = 0, col = 0;
            if (beginMatrix) {
                String[] info = line.split(" ");
                row = Integer.parseInt(info[0].trim());
                col = Integer.parseInt(info[1].trim());
            }
            double[][] mat = new double[row][col];
            for (int r = 0; r <row; r++) {
                line = bReader.readLine();
                String[] info = line.split(" ");
                for (int c = 0; c < info.length; c++) {
                    mat[r][c] = Double.parseDouble(info[c].trim());
                }
            }
            SimpleMatrix matrix = new SimpleMatrix(mat);
            matrices.add(matrix);
        }

        // assign to matrices
        this.tokensEmbedMatrix = matrices.get(0);
        this.weightPredicates = matrices.get(1);
        this.weightREEsEmbed = matrices.get(2);
        this.weightInterest = matrices.get(3);
        this.weightObjSubj = matrices.get(4);
        this.weightUBSubj = matrices.get(5);
        // weights of features are non-negative
        this.weightObjSubj = this.weightObjSubj.elementMult(this.weightObjSubj);
        this.weightUBSubj = this.weightUBSubj.elementMult(this.weightUBSubj);
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

    private SimpleMatrix onePredicateEmbed(ArrayList<SimpleMatrix> embeddingListPredicate) {
        SimpleMatrix operand1 = embeddingListPredicate.get(0).plus(embeddingListPredicate.get(1));
        SimpleMatrix operand2 = embeddingListPredicate.get(3).plus(embeddingListPredicate.get(4));
        SimpleMatrix operator = embeddingListPredicate.get(2);
        SimpleMatrix temp = operand1.elementMult(this.weightPredicates.extractVector(true, 0));
        temp = temp.plus(operand2.elementMult(this.weightPredicates.extractVector(true, 1)));
        temp = temp.plus(operator.elementMult(this.weightPredicates.extractVector(true, 2)));
        return this.ReLU(temp);
    }

    private SimpleMatrix clauseEmbed(ArrayList<SimpleMatrix> embeddingsC) {
        int predicate_num = (int)(embeddingsC.size() / UNIT_PREDICATE);
        ArrayList<SimpleMatrix> embeddingListPredicate = new ArrayList<>();
        // consider the first predicate
        int start = 0, end = UNIT_PREDICATE;
        for (int i = start; i < end; i++) {
            embeddingListPredicate.add(embeddingsC.get(i));
        }
        SimpleMatrix res = this.onePredicateEmbed(embeddingListPredicate);
        for (int i = 1; i < predicate_num; i++) {
            start = i * UNIT_PREDICATE;
            end = (i + 1) * UNIT_PREDICATE;
            for (int j = start, z = 0; j < end; j++, z++) {
                embeddingListPredicate.set(z, embeddingsC.get(j));
            }
            res.plus(this.onePredicateEmbed(embeddingListPredicate));
        }
        return res;
    }

    private ArrayList<SimpleMatrix> extractEmbeddings(int[] tokenIDs) {
        ArrayList<SimpleMatrix> res = new ArrayList<>();
        for (int i = 0; i < tokenIDs.length; i++) {
            int tid = tokenIDs[i];
            res.add(this.tokensEmbedMatrix.extractVector(true, tid));
        }
        return res;
    }

    /*
        t0.A = t1.B     =>  [t0, A, =, t1, B]
        ML(t0.A, t1.B)  =>  [t0, A, ML, t1, B]
        t0.A = c        =>  [t0, A, =, t0, c]
     */
    private int[] transformFeature(ArrayList<Predicate> selectedPredicates, int max_num_predicates) {
        int[] features = new int[max_num_predicates * UNIT_PREDICATE];
        int count = 0;
        for (Predicate p : selectedPredicates) {
            // 1. check whether p is a constant predicate
            if (p.isConstant()) {
                String index = "t" + Integer.toString(p.getIndex1());
                String attr1 = p.getOperand1().getColumn().getName();
                String operator = p.getOperator().toString();
                String constant = p.getConstant();
                features[count++] = this.vobsHash.get(index);
                features[count++] = this.vobsHash.get(attr1);
                features[count++] = this.vobsHash.get(operator);
                features[count++] = this.vobsHash.get(index);
                features[count++] = this.vobsHash.get(constant);
            } else if(p.isML()) {
                String index1 = "t" + Integer.toString(p.getIndex1());
                String attr1 = p.getOperand1().getColumn().getName();
                String operator = p.getMlOption();
                String index2 = "t" + Integer.toString(p.getIndex2());
                String attr2 = p.getOperand2().getColumn().getName();
                features[count++] = this.vobsHash.get(index1);
                features[count++] = this.vobsHash.get(attr1);
                features[count++] = this.vobsHash.get(operator);
                features[count++] = this.vobsHash.get(index2);
                features[count++] = this.vobsHash.get(attr2);
            } else {
                String index1 = "t" + Integer.toString(p.getIndex1());
                String attr1 = p.getOperand1().getColumn().getName();
                String operator = p.getOperator().toString();
                String index2 = "t" + Integer.toString(p.getIndex2());
                String attr2 = p.getOperand2().getColumn().getName();
                features[count++] = this.vobsHash.get(index1);
                features[count++] = this.vobsHash.get(attr1);
                features[count++] = this.vobsHash.get(operator);
                features[count++] = this.vobsHash.get(index2);
                features[count++] = this.vobsHash.get(attr2);
            }
        }
        for (int i = count; i < features.length; i++) {
            features[i] = this.vobsHash.get(PADDING_VALUE);
        }
        return features;
    }

    /*
        reeObj: [1, 3], 3 is the number of objective features
     */
    public double run(ArrayList<Predicate> reeLHS, Predicate rhs, double[][] reeObj) {
        SimpleMatrix objFeas = new SimpleMatrix(reeObj);
        ArrayList<Predicate> reeRHS = new ArrayList<>();
        reeRHS.add(rhs);
        int[] ree_lhs = this.transformFeature(reeLHS, MAX_LHS_PREDICATES);
        int[] ree_rhs = this.transformFeature(reeRHS, MAX_RHS_PREDICATES);
        // 1. get embeddings of all token embeddings
        ArrayList<SimpleMatrix> tokenEmbedsLHS = this.extractEmbeddings(ree_lhs);
        ArrayList<SimpleMatrix> tokenEmbedsRHS = this.extractEmbeddings(ree_rhs);
        // 2. get embeddings of LHS and RHS
        SimpleMatrix lhsEmbed = this.clauseEmbed(tokenEmbedsLHS);
        SimpleMatrix rhsEmbed = this.clauseEmbed(tokenEmbedsRHS);
        // 3. get embeddings of REE
        SimpleMatrix reeEmbed = this.concatTwoByCol(lhsEmbed, rhsEmbed);
        reeEmbed = this.ReLU(reeEmbed.mult(this.weightREEsEmbed));
        // 4. compute the rule interestingness subjective score
        SimpleMatrix subjectiveScore = reeEmbed.mult(this.weightInterest);
        // 5. compute the rule interestingness subjective score with UB
        subjectiveScore = this.weightUBSubj.minus(this.ReLU(subjectiveScore));
        // 6. compute the final rule interestingness score
        SimpleMatrix features = this.concatTwoByCol(objFeas, subjectiveScore);
        SimpleMatrix score = features.mult(this.weightObjSubj);
        return score.get(0, 0);
    }

    /*
        [one; two] aligned by columns
     */
    private SimpleMatrix concatTwoByCol(SimpleMatrix one, SimpleMatrix two) {
        int row = one.numRows();
        int col = one.numCols() + two.numCols();
        SimpleMatrix res = new SimpleMatrix(row, col);
        for (int r = 0; r < row; r++) {
            for (int c = 0; c < one.numCols(); c++) {
                res.set(r, c, one.get(r, c));
            }
            for (int c = 0; c < two.numCols(); c++) {
                res.set(r, c + one.numCols(), two.get(r, c));
            }
        }
        return res;
    }

    /*
        tokenToIDFile:  Token1: 1; Token2: 2; ...
        model_path: the model path of interestingness model
     */
    public InterestingnessModel(String tokenToIDFile, String model_path, FileSystem hdfs) {
        try {
            // load token vob
            this.vobsHash = this.loadVobsID(tokenToIDFile, hdfs);
            // load model parameters
            this.loadModel(model_path, hdfs);
        } catch (Exception e) {

        }
    }

    public InterestingnessModel(String tokenToIDFile, String model_path) {
        try {
            // load token vob
            this.vobsHash = this.loadVobsID(tokenToIDFile);
            // load model parameters
            this.loadModel(model_path);
        } catch (Exception e) {

        }
    }


    private HashMap<String, Integer> loadVobsID(String tokenToIDFile) throws IOException {

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(tokenToIDFile));
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);

        HashMap<String, Integer> vobsIDs = new HashMap<>();
        String line = null;
        while ((line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            String[] info = line.split(" "); // the first one is the token, and the second one is the ID
            String token = ""; // info[0];
            for (int z = 0; z < info.length - 1; z++) {
                token += info[z] + " ";
            }
            token = token.trim();
            int ID = Integer.parseInt(info[info.length - 1].trim());
            if (!vobsIDs.containsKey(token)) {
                vobsIDs.put(token, ID);
            }
        }

        // change the toString of OPERATOR
        // 1. ==
        if (vobsIDs.containsKey("==")) {
            int id_ = vobsIDs.get("==");
            String op = Operator.EQUAL.toString();
            vobsIDs.remove("==");
            vobsIDs.put(op, id_);
        }
        // 2. !=
        if (vobsIDs.containsKey("<>")) {
            int id_ = vobsIDs.get("<>");
            String op = Operator.UNEQUAL.toString();
            vobsIDs.remove("<>");
            vobsIDs.put(op, id_);
        }

        return vobsIDs;
    }

    private HashMap<String, Integer> loadVobsID(String tokenToIDFile, FileSystem hdfs) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(tokenToIDFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);

        HashMap<String, Integer> vobsIDs = new HashMap<>();
        String line = null;
        while ((line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            String[] info = line.split(" "); // the first one is the token, and the second one is the ID
            String token = ""; // info[0];
            for (int z = 0; z < info.length - 1; z++) {
                token += info[z] + " ";
            }
            token = token.trim();
            int ID = Integer.parseInt(info[info.length - 1].trim());
            if (!vobsIDs.containsKey(token)) {
                vobsIDs.put(token, ID);
            }
        }

        // change the toString of OPERATOR
        // 1. ==
        if (vobsIDs.containsKey("==")) {
            int id_ = vobsIDs.get("==");
            String op = Operator.EQUAL.toString();
            vobsIDs.remove("==");
            vobsIDs.put(op, id_);
        }
        // 2. !=
        if (vobsIDs.containsKey("<>")) {
            int id_ = vobsIDs.get("<>");
            String op = Operator.UNEQUAL.toString();
            vobsIDs.remove("<>");
            vobsIDs.put(op, id_);
        }

        return vobsIDs;
    }

    // get the objective and subjective weights
    // first 3 objective features and one subjective ones
    public ArrayList<Double> getObjectiveWeights() {
        ArrayList<Double> weights = new ArrayList<>();
        for (int i = 0; i < this.weightObjSubj.numRows(); i++) {
            weights.add(this.weightObjSubj.get(i, 0));
        }
        return weights;
    }

    // get the UB of subjective score
    public double getUBSubjectiveScore() {
        return this.weightUBSubj.get(0, 0);
    }

}
