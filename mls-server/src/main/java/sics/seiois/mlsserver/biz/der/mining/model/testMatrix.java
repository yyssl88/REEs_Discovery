package sics.seiois.mlsserver.biz.der.mining.model;

public class testMatrix {

    public static void main(String[] args) {

        String model_file = "D:/REE/REE_discovery_code_and_plot/REEs_model_data/model.txt";
        DQNMLP dqnmlp = new DQNMLP(model_file);

        int feature_num = dqnmlp.getFeature_num();
        int action_num = dqnmlp.getAction_num();

        double[][] features = new double[1][feature_num];
        features[0][10] = 1.0;
        features[0][5] =  1.0;

        double[][] qvalues = null;

        long startTime = System.currentTimeMillis();
        for (int i = 0; i <1000000; i++) {
            qvalues = dqnmlp.run(features);
        }
        System.out.println(System.currentTimeMillis() - startTime);
        for (int i = 0; i < qvalues.length; i++) {
            for (int j = 0; j < qvalues[i].length; j++) {
                System.out.print(qvalues[i][j]);
                System.out.print("  ");
            }
        }

    }
}
