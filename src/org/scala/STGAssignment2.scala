package org.scala


import java.sql.Date
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object STGAssignment2 extends App {
  
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
  
  examDF.show
  
//  val DF1=examDF.filter("exam_grade >= 18").sort(asc("exam_grade"))
//  DF1.show
  
 examDF.createOrReplaceTempView("Exam")
 val finalDF=spark.sql("select student_code, avg(exam_grade) as grade_avg, count(1) as cnt from Exam group by student_code having cnt > 3 order by grade_avg desc limit 10").drop("cnt")
 
 finalDF.show(false)
 
  
//  val joinDF=DF2.join(DF1,DF2("student_code")===DF1("student_code"),"inner").show
  
  
  
  
  
  
  
  
  
  
  
  
//  val examSchemaDF=examDF.as[examSchema].toDF()

//  examDF.show(false)
//  examDF.printSchema
  
//  val examJoinEnrDF=examDF.join(enrDF,examDF("student_code")===enrDF("student_code"),"inner").drop(examDF("student_code"))
//  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//  
//  
//  val df1=examJoinEnrDF.withColumn("examdate",to_date($"exam_date","dd-MM-yyyy"))
//  .withColumn("enrdate",to_date($"enrollment_year","yyyy"))
//  
//  println(df1.count)
  
//  val df2=df1.select("student_code","student_name","exam_date","enrdate")
//  
//  val df3=df1.select(date_format($"exam_date","dd-MM-yyyy").as("examdate")).show()
  
//  val df2=df1.withColumn("yearDiff", abs(year($"enrdate")-year($"examdate")))
//  
//  val resultDF=df2.select("student_code","student_name","yearDiff").filter("yearDiff >= 3")
//  
//  println(resultDF.count)
  
  
//  examJoinEnrDF.createOrReplaceTempView("ExamEnr")
// 
// val examenrDF= spark.sql("select  from ExamEnr")
 
 

  
  
  
  
  
  
  
}