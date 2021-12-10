class Config:
    Py_Path = "/home/mlsserver_py/src/main/python/"
    Py_SentenceBert_Path = "mlpredicate/sentencebert/"

    Local_Data_Path = "/home/mlsserver_py/data/"
    Hdfs_Path = ""
    Model_File = "sentencebert"

    Sentences_File_1 = "tableA.csv"
    Sentences_File_2 = "tableB.csv"


    Input_Path = 'input/test.txt'
    Output_Path = 'output/test_out.txt'

    Begin_Sign = "SentenceBertTask result is:"
    End_Sign = "SentenceBertTask result end"

    Batch_Size = 1024