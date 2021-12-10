class Config:
    Py_Path = "/home/mlsserver_py/src/main/python/"
    Py_LSH_Path = "lsh/"
    Local_Data_Path = "/home/mlsserver_py/data"
    Hdfs_Path = ""

    Python_Home = "/opt/anaconda3/envs/ml/bin/python"

    Sentences_File_1 = "tableA.csv"
    Sentences_File_2 = "tableB.csv"

    Input_Path = 'input/test.txt'
    Output_Path = 'output/test_out.txt'

    Begin_Sign = "lsh result is:"
    End_Sign = "lsh result end"

    Model_path = "sentencebert"
    Bucket_size = 1024
    LSH_SEED = 88888888
    LSH_hash_size = 25
    LSH_input_dim = 768
    LSH_num_hashtables = 1500