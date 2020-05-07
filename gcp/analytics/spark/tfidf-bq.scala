import com.google.cloud.spark.bigquery._
val df = spark.read.bigquery("bigquery-public-data.stackoverflow.comments").where("creation_date between '2019-01-01' and '2019-02-01'")

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.StopWordsRemover

val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
val wordsData = tokenizer.transform(df)

val remover = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("filtered")

val wordsNoStop = remover.transform(wordsData)

// val hashingTF = new HashingTF()
//   .setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(100)

// val featurizedData = hashingTF.transform(wordsNoStop)
// alternatively, CountVectorizer can also be used to get term frequency vectors

val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("filtered").setOutputCol("rawFeatures")
    .setVocabSize(3).setMinDF(2).fit(wordsNoStop)


val featurizedData = cvModel.transform(wordsNoStop)
// alternatively, CountVectorizer can also be used to get term frequency vectors

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)

val rescaledData = idfModel.transform(featurizedData)
rescaledData.show()