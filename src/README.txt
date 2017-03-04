
1.Create the job jar file from shell:
  $ cd to your Main class src dir
  $ javac *.java
  #attention next cmd's end has an dot, which means current dir
  $ jar -cvf WordSegmentation.jar *.class -C /usr/local/java/hbase-1.2.4/lib .
  $ hadoop jar WordSegmentation.jar WordSegmentationMain
