package com.similarity.jaccard

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap
import java.io.File
import java.io.PrintWriter

/** Computes Jaccard Similarity between two words within document **/ 

object SparkJaccard{

  def main(args: Array[String]){
    if(args.length < 2){
      println("Usage: SparkJaccard input_folder output_folder")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Jaccard")
    val sc = new SparkContext(conf)
    var arr_lines = Array()
    var m_biagram = new HashMap[String, Int].withDefaultValue(0)
    var m_single = new HashMap[String, Int].withDefaultValue(0)
    println(args(0))
    val files = new File(args(0)).listFiles.map(_.getName).toList
    files.foreach { f =>
        val lines = sc.textFile(args(0)+f)
  
       // find specific biagram
       lines.map{ 

           // Split each line into substrings by periods
           _.split('.').map{ substrings =>

           // Trim substrings and then tokenize on spaces
           substrings.trim.split(' ').

           // clean replacement technique, and convert to lowercase
           map{_.replaceAll("""([\p{Punct}&&[^.$]]|\b\p{IsLetter}{1,2}\b)\s*""", "").toLowerCase()}.
        
           filter(_.nonEmpty).

           // Find bigrams
           sliding(2)
           }.
               // Flatten, and map the bigrams to concatenated strings
               flatMap{identity}.map{_.mkString(" ")}.//filter{word=>word.equals("red_dog")}.
    
              // Group the bigrams and count their frequency
               groupBy{identity}.mapValues{_.size}

        }.
         // Reduce to get a global count, then collect
         flatMap{identity}.reduceByKey(_+_).collect.
	 
         foreach{x=> m_biagram(x._1) += x._2}	
         //val bigram_cnt = filtered_bigram(0)._2

         //single unique words with in each line
         lines.flatMap(line=>line.split(" "))
                   .map(_.replaceAll("""([\p{Punct}&&[^.$]]|\b\p{IsLetter}{1,2}\b)\s*""", ""))
                   //.filter(word=>word.equals("dog"))
                   .map(word => (word, 1))
                   .reduceByKey{case (x,y)=> x+y}
                   .collect
                   .foreach{y=>
                      m_single(y._1) += y._2
                    } 
      }

      //create Jaccard similarity output file by matching partial keys
      var s_cnt=0
      var b_cnt=0
      var b_1=""
      var b_2=""
      val pw = new PrintWriter(new File(args(1)+"Jaccard_score.txt"))
      pw.write("Jaccard similarity scores between single and bigram words in Documents:\n")
      println("Jaccard similarity scores between single and bigram words in Documents:")
      m_biagram.keys.foreach((word) =>{
        b_1 = word.split("\\W+")(0)
        b_2 = word.split("\\W+")(1)
        //println(b_1 + ":" + b_2 + ":" + m_biagram(word))
        if(m_single.contains(b_1) || m_single.contains(b_2)){
           b_cnt = m_biagram(word)
           if(m_single(b_1)>0){
             s_cnt = m_single(b_1)
             println("'"+ b_1 +"' and '"+ word + "' is: "+ b_cnt.toFloat / s_cnt)  
             pw.write("'"+ b_1 +"' and '"+ word + "' is: "+ b_cnt.toFloat / s_cnt+"\n")
           }
           if(m_single(b_2)>0){
             s_cnt = m_single(b_2)
             println("'"+ b_2 +"' and '"+ word + "' is: "+ b_cnt.toFloat / s_cnt)
             pw.write("'"+ b_2 +"' and '"+ word + "' is: "+ b_cnt.toFloat / s_cnt+ "\n") 
          }
         }
        }
	)     
       //close print writer
       pw.close      
      //stop spark session
      sc.stop()
  }
}
