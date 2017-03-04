/**
 * Created by ailias on 2/23/17.
 */


import com.sun.jersey.api.MessageException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

public class ResultEmail {
    private ArrayList<String> toEmailsList;//the to mail list
    private HashMap<String, String> emailNameMap;//store the <email, name > key-value pair
    private HashMap<String, String[]> emailQueriesMap;//<email,queries[]> key-value pair
    private static final String contenfFileName = "files/mailContent.txt";
    private BufferedReader reader = null;


    public ResultEmail(FileSystem fileSystem, String queryFileName) {
        toEmailsList = new ArrayList<>();
        emailNameMap = new HashMap<>();
        emailQueriesMap = new HashMap<>();
        this.readQueryFile(fileSystem, queryFileName);
    }

    /**
     * read file and parse the data to hashmap variable
     *
     * @param queryFileName
     */
    public void readQueryFile(FileSystem fileSystem, String queryFileName) {
        String lineStr = null;
        String tmpEmail;
        try {
            Path filePath = new Path("hdfs://namenode:9000/" + queryFileName);
            //Path filePath = new Path(queryFileName);
            if (fileSystem.exists(filePath)) {
                FSDataInputStream inputStream = fileSystem.open(filePath);
                reader = new BufferedReader(new InputStreamReader(inputStream));
                while ((lineStr = reader.readLine()) != null) {//read file line by line
                    String[] lineArr = lineStr.trim().split("\\s+");
                    tmpEmail = lineArr[1];
                    toEmailsList.add(tmpEmail);//add the email to arraylist
                    this.emailNameMap.put(tmpEmail, lineArr[0]);
                    lineArr = (String[]) ArrayUtils.remove(lineArr, 0);//delete name
                    lineArr = (String[]) ArrayUtils.remove(lineArr, 0);//delete email
                    this.emailQueriesMap.put(tmpEmail, lineArr);
                }
                reader.close();
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public String getMailContentFile(FileSystem fileSystem, String toEmail, String[] results) throws IOException {
        String contentStr = "<center><h3>WantJob: Latest Daily Recruitment Information</h3></center><p>";
        contentStr += "Dear " + this.emailNameMap.get(toEmail) + ":<p>&nbsp;&nbsp;&nbsp;&nbsp; Your input query words are: <p>&nbsp;&nbsp;&nbsp;&nbsp;";
        String[] queries = this.emailQueriesMap.get(toEmail);
        for (int qindex = 0; qindex < queries.length; qindex++) { //read all queries string to the content
            contentStr = contentStr + " " + queries[qindex];
        }
        contentStr += "<p>The following are your results:<p>";
        for (int rindex = 0; rindex < results.length; rindex++) {//read all results to the content
            String[] url_title = results[rindex].split("#");
            contentStr = contentStr + "<li>" + Integer.toString(rindex + 1) + " : <a href='" + url_title[0] + "'>" + url_title[1] + "</a></li><br>";
        }
        Path filePath = new Path("hdfs://namenode:9000/" + contenfFileName);
        //Path filePath = new Path(contenfFileName);
        if (fileSystem.exists(filePath)) {
            FSDataInputStream inputStream = fileSystem.open(filePath);
            String tmpStr = null;
            try {
                reader = new BufferedReader(new InputStreamReader(inputStream));
                while ((tmpStr = reader.readLine()) != null) {
                    contentStr += tmpStr;
                }
            } catch (IOException e) {
                System.out.println(e);
            } finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return contentStr;

    }

    /**
     * return toEmail List
     *
     * @return
     */
    public ArrayList<String> getToEmailList() {
        return this.toEmailsList;
    }

    /**
     * return <toEmail,Queries[]> pairs
     *
     * @return
     */
    public HashMap<String, String[]> getEmailQueriesMap() {
        return this.emailQueriesMap;
    }

    public void sendEmail(FileSystem fileSystem, String date, String toEmail, String[] results) throws IOException {
        // Get system properties
        Properties properties = System.getProperties();
        //properties.put("mail.debug","true"); //used for debug the send mail process.
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.host", "smtp.ict.ac.cn");
        properties.put("mail.smtp.port", "25");
        //properties.put("mail.smtp.ssl.enable", "true");
        properties.put("mail.transport.protocol", "smtp");
        Session session = Session.getDefaultInstance(properties);
//        Session session = Session.getInstance(properties,
//                new javax.mail.Authenticator() {
//                    protected PasswordAuthentication getPasswordAuthentication() {
//                        return new PasswordAuthentication(username, password);
//                    }
//                }
//        );
        Transport transport = null;
        try {
            //parse the email[] string to Address[]
            Address[] toEmailsAddrArr = new Address[1];
            toEmailsAddrArr[0] = new InternetAddress(toEmail);
            //create a message
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("yangcheng01@ict.ac.cn"));
            //message.setRecipients(Message.RecipientType.TO,toEmailsAddrArr);
            message.setSubject("[" + date + "] WantJob: The Latest Daily Recruitment Information");
            String content = getMailContentFile(fileSystem, toEmail, results);
            message.setContent(content, "text/html;charset=UTF-8");
            String username = "yangcheng01@ict.ac.cn";
            String password = "yjh1990111808";
            //create a transport
            transport = session.getTransport();
            transport.connect(username, password);
            transport.sendMessage(message, toEmailsAddrArr);
            transport.close();
        } catch (MessagingException e) {
            e.printStackTrace();
        }
        System.out.println("Send Email to " + toEmail + " Success\n");
    }


}
