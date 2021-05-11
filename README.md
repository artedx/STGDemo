# STGDemo
STGDemo

Given two files in input in CSV format (RFC 1480) with the following structures (example 
attached):
• exams.csv
{student_name},{student_code},{exam_date},{exam_grade}
exam_date is a date formatted using ISO 8601 YYYY-MM-DD format
exam_grade is an integer number from 0 to 31
• enrollments.csv
{student_code},{enrollment_year}
enrollment_year is formatted using ISO 8601 YYYY format
Note: assume that each file is well-formed and valid; they don't contain duplicate or 
inconsistent data.
Implement two algorithms producing two files (one per each) meeting the following 
requirements:
1) Provide the code and name of the students enrolled more than three years ago, if the 
name is not available please leave the name field empty.
2) Provide the student code of the 10 best students which passed more than 3 exams 
sorted descending by average of grade. An exam is considered passed with a grade 
greater or equal to 18.
Output file formats are:
• students-enrolled.csv
{student_code},{student_name}
• top-ten-students.csv
{student_code},{grade_avg}
You are free to choose your favorite programming language (scala or java is a plus) and use 
whatever frameworks and/or libraries that you think are useful keeping in mind the KISS 
principle https://en.wikipedia.org/wiki/KISS_principle.
Your project must work and be covered by some unit test and should be compiled and 
packaged using a build tool if the chosen language support it (sbt, gradle or maven is a plus)
