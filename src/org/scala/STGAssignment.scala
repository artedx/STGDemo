package org.scala


import java.sql.Date
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object STGAssignment extends App {
  
//  case class enrSchema(student_code:Double ,enrollment_year: String)
//  case class examSchema(student_name:String,student_code: Int,exam_date:Date,exam_grade:Int)
  
  val spark=SparkSession.builder
  .master("local")
  .appName("stg assignment")
  .getOrCreate
  
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  
  
  val enrDF=spark.read
  .option("header",true)
  .csv("/Users/ATikle/Downloads/Assignment_SGT/input-data - 202102230947/output/enrollments.csv")
  
//  val enrSchemaDF=enrDF.as[enrSchema].toDF()
 
//  enrDF.show(false)
//  enrDF.printSchema()
  
  val examDF=spark.read
  .option("header",true)
  .csv("/Users/ATikle/Downloads/Assignment_SGT/input-data - 202102230947/output/exams.csv")
  
//  val examSchemaDF=examDF.as[examSchema].toDF()

//  examDF.show(false)
//  examDF.printSchema
  
  val examJoinEnrDF=examDF.join(enrDF,examDF("student_code")===enrDF("student_code"),"inner").drop(examDF("student_code"))
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  
  
  val df1=examJoinEnrDF.withColumn("examdate",to_date($"exam_date","dd-MM-yyyy"))
  .withColumn("enrdate",to_date($"enrollment_year","yyyy"))
  
  
  
//  val df2=df1.select("student_code","student_name","exam_date","enrdate")
//  
//  val df3=df1.select(date_format($"exam_date","dd-MM-yyyy").as("examdate")).show()
  
  val df2=df1.withColumn("yearDiff", abs(year($"enrdate")-year($"examdate")))
  
  val resultDF=df2.select("student_code","student_name","yearDiff").filter("yearDiff >= 3").drop("yearDiff")
  
  resultDF.show(false)
  
  
  
//  examJoinEnrDF.createOrReplaceTempView("ExamEnr")
// 
// val examenrDF= spark.sql("select  from ExamEnr")
 
 

  
  
  
  
  
  
  
}