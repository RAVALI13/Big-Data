HDFSWordC is the maven project developed in order to download tweets of a particular topic(say nyse-newyork stock exchange in my project)and count the number of trending hashtags for the topic.

The data is downloaded for 5 different dates on the same topic "nyse"

GetUserStatus.java is the file used to download the tweets onto the HDFS server.

TrendingTopicCount is the program that includes the modified code of MapReduce. It maps the number of words and counts the frequency of occurences.





To run:

hadoop jar jarfilenamecreated.jar HDFSWordc.HDFSWordC.GetUserStatus hdfs://cshadoop1/user/rxn152730/newsrcfolder/ hdfs://cshadoop1/user/rxn152730/newdstfolder/

