class Config:
    Py_Path = "/home/mlsserver_py/src/main/python/"
    Py_Ditto_Path = "mlpredicate/ditto/"
    Py_Snippext_Path = "mlpredicate/ditto/Snippext_public/"

    Local_Data_Path = "/home/mlsserver_py/data/"
    Hdfs_Path = ""
    Model_File = "ditto.pt"

    Sentences_File_1 = "tableA.csv"
    Sentences_File_2 = "tableB.csv"


    Input_Path = 'input/test.txt'
    Output_Path = 'output/test_out.txt'

    Begin_Sign = "DittoTask result is:"
    End_Sign = "DittoTask result end"

    Batch_Size = 1024