Map Reduce Execution:


hadoop jar final.jar hadoop.algorithms.joins.yelpData.Q1_Top10_Review_And_Business.Q1 rxn152730/yelpdatafall/business/business.csv rxn152730/yelpdatafall/review/review.csv
hadoop jar final.jar hadoop.algorithms.joins.yelpData_Q2_User_Ave_Rating.User_Ave_Rating_Driver rxn152730/yelpdatafall/review/review.csv UJG/yelpdatafall/user/user.csv "Pgm J."
hadoop jar final.jar hadoop.algorithms.joins.yelpData.Q3_User_Rating_Stanford.User_Rating_Stanford_Driver rxn152730/yelpdatafall/review/review.csv UJG/yelpdatafall/business/business.csv
hadoop jar final.jar hadoop.algorithms.joins.yelpData.Q4_Top_10_User_Review.Top_Most_Reviewers rxn152730/yelpdatafall/review/review.csv rxn152730/yelpdatafall/user/user.csv
hadoop jar final.jar hadoop.algorithms.joins.yelpData.Q5_Business_Count_TX.Top_Rated_Texas_Resurants rxn152730/yelpdatafall/business/business.csv rxn152730/yelpdatafall/review/review.csv

Scala Execution:

spark-submit --master local --class org.apache.spark.examples.streaming.Q1 simple-project_2.10-1.0.jar /home/hduser/rxn152730/Assignment_5/yelpData/review.csv /home/hduser/rxn152730/Assignment_5/yelpData/business.csv
spark-submit --master local --class org.apache.spark.examples.streaming.Q2 simple-project_2.10-1.0.jar "Lisa W." /home/hduser/rxn152730/Assignment_5/yelpData/user.csv /home/hduser/rxn152730/Assignment_5/yelpData/review.csv
spark-submit --master local --class org.apache.spark.examples.streaming.Q3 simple-project_2.10-1.0.jar /home/hduser/rxn152730/Assignment_5/yelpData/business.csv /home/hduser/rxn152730/Assignment_5/yelpData/review.csv
spark-submit --master local --class org.apache.spark.examples.streaming.Q4 simple-project_2.10-1.0.jar /home/hduser/rxn152730/Assignment_5/yelpData/user.csv /home/hduser/rxn152730/Assignment_5/yelpData/review.csv
spark-submit --master local --class org.apache.spark.examples.streaming.Q5 simple-project_2.10-1.0.jar /home/hduser/rxn152730/Assignment_5/yelpData/business.csv /home/hduser/rxn152730/Assignment_5/yelpData/review.csv