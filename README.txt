//Shashank Gupta sgupta27@uncc.edu

--The src folder consists of all the Java files.
--The output folder consists of the output files generated after running the MapReduce.
--The strings for processing are considered as not case sensitive. Hence "IBM", "ibm", and "Ibm" all are equal.
--I have run all the programs on the sample input files "Hadoop is yellow Hadoop" and "yellow Hadoop is an elephant"
The file gave the required results.
--For taking the input for Search.java I am using the third parameter as the input string.
--I have also implemented the optional Rank.java

-- To run TFIDF
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint 
jar -cvf tfidf.jar -C build/ .
hadoop jar tfidf.jar org.myorg.TFIDF /user/cloudera/assignment2/input /user/cloudera/assignment2/outputTFIDF

-- To run Search.java
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint 
jar -cvf search.jar -C build/ .
hadoop jar search.jar org.myorg.Search /user/cloudera/assignment2/outputTFIDF /user/cloudera/assignment2/outputSearch "computer science"