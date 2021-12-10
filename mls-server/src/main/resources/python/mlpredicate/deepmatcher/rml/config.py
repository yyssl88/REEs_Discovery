class Config:
    Py_Path = "/home/mlsserver_py/src/main/python/"
    Py_DeepMatcher_Path = "mlpredicate/deepmatcher/"

    Local_Data_Path = "/home/mlsserver_py/data"
    Hdfs_Path = ""
    Model_File = "ditto.pt"

    # Py_Path = "/work/javaproject/mls-server_feature-er-condig/src/main/resources/python"
    # Local_Data_Path = "/data/"
    # Hdfs_Path = ""
    # Model_File = "ditto.pt"


    Sentences_File_1 = "tableA.csv"
    Sentences_File_2 = "tableB.csv"


    Input_Path = 'input/test.txt'
    Output_Path = 'output/test_out.txt'

    Begin_Sign = "DeepMatcherTask result is:"
    End_Sign = "DeepMatcherTask result end"

    Batch_Size = 1024