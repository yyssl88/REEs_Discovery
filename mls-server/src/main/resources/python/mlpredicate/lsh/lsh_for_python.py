import os

from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, SentencesDataset, InputExample, losses, evaluation
from torch.utils.data import DataLoader

model_path = "savedmodel"

def get_lsh(spark=None, sentences=[], path="", fit_model=True):
    # 第1.1步，加载模型并进行微调训练
    model = None
    if fit_model or (not os.path.exists(model_path)):
        model = SentenceTransformer('data/xlm-r-100langs-bert-base-nli-mean-tokens')

        train_examples = []
        for line in open("data/train.csv", 'r', encoding='utf-8'):
            sentence_list = line.split(",")
            if (len(sentence_list) == 3):
                train_examples.append(
                    InputExample(texts=[sentence_list[0], sentence_list[1]], label=int(sentence_list[2]) + 0.0))

        train_dataset = SentencesDataset(train_examples, model)
        train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=320)
        train_loss = losses.CosineSimilarityLoss(model)

        dev_samples = train_examples

        dev_evaluator = evaluation.EmbeddingSimilarityEvaluator.from_input_examples(dev_samples, batch_size=16,
                                                                                    name='sts-dev')

        # Tune the model
        model.fit(train_objectives=[(train_dataloader, train_loss)], epochs=1, warmup_steps=100, evaluator=dev_evaluator,
                  evaluation_steps=500)
        # 保存和加载模型
        model.save("savedmodel")
    else:
        model = SentenceTransformer("savedmodel")

    # 第1.2步，把测试句子转成向量
    test_path = "data/test.txt"
    if len(path) > 0:
        test_path = path

    test_sentences = set()
    if len(sentences) > 0:
        test_sentences = sentences
    else:
        for line in open(test_path, 'r', encoding='utf-8'):
            test_sentences.add(line)

    corpus_embeddings = model.encode(list(test_sentences), show_progress_bar=True, convert_to_numpy=True, num_workers=8)
    # print(corpus_embeddings)

    # 第2步，用sentencebert把句子转成向量
    if spark == None:
        spark = SparkSession \
            .builder \
            .appName("lsh") \
            .master("local[3]") \
            .getOrCreate()

    dataA = []
    n = 0
    for corpus_embedding in corpus_embeddings:
        dataA.append((n, Vectors.dense(corpus_embedding),))
        n = n + 1

    dfA = spark.createDataFrame(dataA, ["id", "features"])

    brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=0.01,
                                      numHashTables=30)
    model = brp.fit(dfA)

    # Feature Transformation
    hashes = model.transform(dfA).select("hashes")
    print("The hashed dataset where hashed values are stored in the column 'hashes':")
    hashes.show(truncate=False)

    # hashes = model.transform(dfA).show(truncate=False)

    return hashes

if __name__ == "__main__":
    # get_lsh()
    get_lsh(sentences=["hello world"], fit_model=True)