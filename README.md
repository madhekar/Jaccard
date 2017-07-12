Jaccard Similarity scoring between single and bigram words with-in large set of documents in corpus.

Program uses distributed spark processing of files.

steps to execute:

1. add plain text documents in ./data folder
2. build project with gradle:
     gradle clean build
3. execute spark-submit as:
     park-submit --class com.similarity.jaccard.SparkJaccard  --master local[2] build/libs/jaccard.jar "./data/" "./out/"
4. Jaccard_scores.txt file should be created with scores between single words and bigram words
