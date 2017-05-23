/**
 * Created by ailias on 2/23/17.
 */


import com.sun.jersey.api.MessageException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static int infoCount = 25;


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

    /**
     * parsing the pku url from the ori_url
     *
     * @param ori_url
     * @return
     */
    public String parsePkuUrl(String ori_url) {
        String str_pattern = "\\d+$";
        Pattern pattern = Pattern.compile(str_pattern);
        Matcher matcher = pattern.matcher(ori_url);
        if (matcher.find()) {
            return "https://bbs.pku.edu.cn/v2/post-read.php?bid=845&threadid=" + matcher.group(0);
        } else {
            System.out.println("No pattern for this url:" + ori_url);
            return ori_url;
        }
    }

    /**
     * parsing the newsmth url from the ori_url
     *
     * @param ori_url
     * @return
     */
    public String parseNewsmthUrl(String ori_url) {
        String str_pattern = "\\d+$";
        Pattern pattern = Pattern.compile(str_pattern);
        Matcher matcher = pattern.matcher(ori_url);
        if (matcher.find()) {
            //return "http://www.newsmth.net/nForum/#!article/Career_Campus/" +matcher.group(0);
            return "http://www.newsmth.net/nForum/article/Career_Campus/" + matcher.group(0);
        } else {
            System.out.println("No pattern for this url:" + ori_url);
            return ori_url;
        }
    }

    /**
     * parsing the yssy url from the ori_url
     *
     * @param ori_url
     * @return
     */
    public String parseYssyUrl(String ori_url) {
        String str_pattern = "\\d+";
        Pattern pattern = Pattern.compile(str_pattern);
        Matcher matcher = pattern.matcher(ori_url);
        if (matcher.find()) {
            return "http://bbs.sjtu.edu.cn/bbscon,board,JobInfo,file,M." + matcher.group(0) + ".A.html";
        } else {
            System.out.println("No pattern for this url:" + ori_url);
            return ori_url;
        }
    }

    /**
     * main entry for parsing url.
     *
     * @param ori_url
     * @return
     */
    public String parseUlr(String ori_url) {
        String url = ori_url;
        if (ori_url.matches("(.*)bbs.pku.edu.cn(.*)")) {
            url = parsePkuUrl(ori_url);
        } else if (ori_url.matches("(.*)newsmth.net(.*)")) {
            url = parseNewsmthUrl(ori_url);
        } else if (ori_url.matches("(.*)bbs.sjtu.edu.cn(.*)")) {
            url = parseYssyUrl(ori_url);
        } else {
            System.out.println("Can not parse the url:" + ori_url);
        }
        return url;
    }

    public String getMailContentFile(FileSystem fileSystem, String toEmail, String[] results) throws IOException {
        String contentStr = "<center><h3>WantJob: Latest Daily Recruitment Information</h3></center><p>";
        contentStr += "<p><h4>Dear " + this.emailNameMap.get(toEmail) + ":</h4></p><p style='padding-left:20px'>Your input query words are: </p><p style='padding-left:20px'>";
        String[] queries = this.emailQueriesMap.get(toEmail);
        for (int qindex = 0; qindex < queries.length; qindex++) { //read all queries string to the content
            contentStr = contentStr + " " + queries[qindex];
        }
        contentStr += "</p><p style='padding-left:20px'>The followings are your results:</p><p>";
        String url;
        for (int rindex = 0; rindex < results.length && rindex <= infoCount; rindex++) {//read all results to the content
            String[] url_title = results[rindex].split("#");
            url = parseUlr(url_title[0]);
            contentStr = contentStr + "<li>" + Integer.toString(rindex + 1) + " : <a href='" + url + "'>" + url_title[1] + "</a></li><br>";
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

    public void sendEmail(String username, String password, FileSystem fileSystem, String date, String toEmail, String[] results) throws IOException {
        // Get system properties
        Properties properties = System.getProperties();
        //properties.put("mail.debug","true"); //used for debug the send mail process.
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.host", "smtp.qq.com");
        properties.put("mail.smtp.port", "25");
        //properties.put("mail.smtp.ssl.enable", "true");
        properties.put("mail.transport.protocol", "smtp");
        Session session = Session.getDefaultInstance(properties);
        Transport transport = null;
        try {
            //parse the email[] string to Address[]
            Address[] toEmailsAddrArr = new Address[1];
            toEmailsAddrArr[0] = new InternetAddress(toEmail);
            //create a message
            Message message = new MimeMessage(session);
	    //input your sending email
            message.setFrom(new InternetAddress(username));
            //message.setRecipients(Message.RecipientType.TO,toEmailsAddrArr);
            message.setSubject("[" + date + "] WantJob: The Latest Daily Recruitment Information");
            String content = getMailContentFile(fileSystem, toEmail, results);
            message.setContent(content, "text/html;charset=UTF-8");
            //create a transport
            transport = session.getTransport();
            transport.connect(username, password);
            transport.sendMessage(message, toEmailsAddrArr);
            transport.close();
        } catch (MessagingException e) {
            e.printStackTrace();
            System.out.println("Send Email to " + toEmail + " Failed:\n"+e);
        }
        System.out.println("Send Email to " + toEmail + " Success\n");
    }


}
