============================
==WantJob program tutorial==
============================

Example:(https://github.com/ailias/JobSpider-Auto-Recruitment-Information-Collection-on-Hadoop/blob/master/1.jpg)

  Firstly you should make sure your hadoop+hbase environment can work successfully, 
then we can do the following configuration for running it.

1: Starting hadoop and hbase environment.

###################
2:create hbase tables
###################

##After starting the hbase, shell to hbase:
   $ hbase shell

##table for inverseIndexing
   > create 'WordFrequency','wordfrequency'
   > create 'DocFrequency','docfrequency'
   > create 'DocLength','doclength'
   > create 'TermFrequency','termfrequency'

################
3:Running crontab.sh in crontab env
################

##First edit crontab file(make sure you have install crontab):
   $ crontab -e
##input following text and save(means running crontab.sh everyday at 1:00 am):
   0 1 * * * /path/to/WordSegmentation/NewsmthScrapy/crontab.sh

###################
4:create jar package for running the program
###################

##change the email account to your email for sending emails
  $ vim src/WordSegmentationMain.java

##and change the username and password string to your eimail
  String username = "your-qq@qq.com";
  String password = "your password";

##Create the job jar file from shell:
  $ cd to your Main class src dir
  $ javac *.java

##attention next cmd's end has an dot, which means current dir
  $ jar -cvf WordSegmentation.jar *.class
  $ hadoop jar WordSegmentation.jar WordSegmentationMain
