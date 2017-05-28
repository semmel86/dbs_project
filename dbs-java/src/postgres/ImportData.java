package postgres;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;

public class ImportData {

	private static PostgresConnector pc = new PostgresConnector();
	private static Connection connection;

	public static void main(String[] argv) throws SQLException, IOException {

		File csvBaseFile = new File("C:\\Users\\semmel\\Documents\\DBS\\Projekt\\american-election-tweets.csv");

		// clean the dirty file
		cleanUpCSV("C:\\Users\\semmel\\Documents\\DBS\\Projekt\\american-election-tweets.csv");
		connection = pc.getConnection();

		Scanner scanner = new Scanner(csvBaseFile);
		while (scanner.hasNext()) {
			String currentLine = scanner.nextLine();
			try {
				// System.out.println(currentLine);
				if (currentLine.contains(";")) {

					String handle = currentLine.substring(0, currentLine.indexOf(";"));
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());
//					System.out.print("Handle: " + handle + "\t");

					// Insert Handle

					String text = currentLine.substring(0, currentLine.indexOf(";")).replaceAll("\"", "");
//					System.out.print("\tText: " + text);
					List<String> hashtags = getHashtags(text);
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());

					//2016-01-06T02:33:40
					String dateStr = currentLine.substring(0, currentLine.indexOf("T"));
//					System.out.print("\tDate: " + dateStr);
					Date date = Date.valueOf(dateStr);
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());

					String retweet_count = currentLine.substring(0, currentLine.indexOf(";"));
//					System.out.print("\tRetweets: " + retweet_count);
					currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());

					String favorite_count = currentLine.substring(0, currentLine.indexOf(";"));
//					System.out.print("\tFavorites: " + favorite_count + "\n");
					// favorite_count
					// retweet_count

					// hashtags

					// persist cuurentLine
					persistIntoDb(handle, text, date, Integer.parseInt(retweet_count), Integer.parseInt(favorite_count),
							hashtags);
				} else {
					System.out.println("There is no comma-separated line");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		scanner.close();
	}

	private static void persistIntoDb(String handle, String text, Date date, int retweet_count, int favorite_count,
			List<String> hashtags) {
		try {
			if (connection != null) {
				
				// handle
				PreparedStatement accountId = connection
						.prepareStatement("SELECT * FROM twitteraccount WHERE handle = ?  ;");
				accountId.setString(1, handle);
				
				ResultSet rs = accountId.executeQuery();
				
				if (!rs.next()) {// new handle // insert handle
					System.out.println("New Handle");
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO twitteraccount (handle) VALUES  (?);");
					insert.setString(1, handle);
					insert.execute();
						rs = accountId.executeQuery();
					
				}
				int account_id = rs.getInt(1);
//				System.out.println("AccountId: " + account_id + " for handle: " + handle);
				
				// tweet
				PreparedStatement tweetId = connection
						.prepareStatement("SELECT * FROM tweet WHERE contenttext = ?  ;");
				tweetId.setString(1, text);
				rs = tweetId.executeQuery();
				
				if (!rs.next()) {// new text 
					System.out.println("New Text");
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO tweet (tweetdate,contenttext,favorite_count,retweet_count) VALUES  (?,?,?,?);");
					insert.setDate(1, date);
					insert.setString(2, text);
					insert.setInt(3, favorite_count);
					insert.setInt(4, retweet_count);

					insert.execute();
					rs = tweetId.executeQuery();
					rs.next();//set position of rs
				}
				int tweet_Id = rs.getInt(1);
				System.out.println("TweetId: "+tweet_Id);
				// select tweetId
				// insert hashtags
				
				for(String tag:hashtags){
				PreparedStatement hashTagId = connection
						.prepareStatement("SELECT * FROM hashtag WHERE hashtag = ?  ;");
				hashTagId.setString(1, tag);
				rs = hashTagId.executeQuery();
				
				if (!rs.next()) {// new tag
					System.out.println("New Tag");
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO hashtag (hashtag) VALUES  (?);");
					insert.setString(1, tag);
					insert.execute();
					rs = hashTagId.executeQuery();
					rs.next();
				}

				int hashTag_Id=rs.getInt(1);
				System.out.println("HASHtagId: "+rs.getInt(1));
				
				PreparedStatement tweetToTextId = connection
						.prepareStatement("SELECT * FROM tweet_belongs_to_hashtags WHERE hashtagid = ? AND tweetid = ?  ;");
				tweetToTextId.setInt(1,hashTag_Id);
				tweetToTextId.setInt(2, tweet_Id);
				rs = tweetToTextId.executeQuery();
				
				if (!rs.next()) {// new tagToText Relation
					System.out.println("New Tag to Text Relation");
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO tweet_belongs_to_hashtags (hashtagid,tweetid) VALUES  (?,?);");
					insert.setInt(1,hashTag_Id);
					insert.setInt(2, tweet_Id);
					insert.execute();
					
				}
				
				}
				
				PreparedStatement accountToTweetId = connection
						.prepareStatement("SELECT * FROM account_tweets WHERE accountid = ? AND tweetid = ?  ;");
				accountToTweetId.setInt(1,account_id);
				accountToTweetId.setInt(2, tweet_Id);
				rs = accountToTweetId.executeQuery();
				
				if (!rs.next()) {// new AccountToTweet Relation
					System.out.println("New Account to Tweet Relation");
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO account_tweets (accountid,tweetid) VALUES  (?,?);");
					insert.setInt(1,account_id);
					insert.setInt(2, tweet_Id);
					insert.execute();
					
				}
				// select Id's
				// insert account_tweet(accountId,tweetId)
				
				// insert hashtag_text(hashtagId,tweetId
				// add to 
				
			} else {
				System.out.println("No connection!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}

	private static List<String> getHashtags(String text) {
		List<String> hashtags = new ArrayList<String>();

		while (text.contains("#")) {
			// get the String after # until " "		
			int startHashtag=text.indexOf("#");
			int endHashtag=text.indexOf(" ", text.indexOf("#")) > 0 ? text.indexOf(" ", text.indexOf("#")):text.length();
			System.out.println("Text:"+text);
			System.out.println("start:"+startHashtag);
			System.out.println("end:"+endHashtag);
			String tag = text.substring(startHashtag,endHashtag);
			System.out.println("Tag: " + tag);
			if (tag.length() > 1)
				hashtags.add(tag); // avoid empty hashtags
			text = text.substring(endHashtag, text.length());
		}
		return hashtags;
	}

	private static void cleanUpCSV(String path) throws IOException {
		File file = new File(path);
		String str = FileUtils.readFileToString(file);// new
														// String(Files.readAllBytes(file.toPath()));
		// remove hard spaces
		str = str.replaceAll(".co/ZHMKIqnUwL ", "");
		// remove single <LF>
		str = str.replaceAll("\r\n", "CARIA" + "GERETURN_AND_LINE");
		str = str.replaceAll("(\\n)", "");
		str = str.replaceAll("CARIAGERETURN_AND_LINE", "\r\n");
		// System.out.println(str);
		FileUtils.writeStringToFile(file, str);

	}

	private String[][] readCSV(String path) throws IOException {
		// read in csv Line by Line
		File csvBaseFile = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(csvBaseFile));

		while (br.ready()) {
			String currentLine = br.readLine();
			System.out.println(currentLine);
			if (currentLine.contains(";")) {
				String handle = currentLine.substring(0, currentLine.indexOf(";"));
				currentLine = currentLine.substring(currentLine.indexOf(";") + 1, currentLine.length());

				System.out.println(currentLine);
				System.out.println(handle);
			} else {
				System.out.println("There is no comma-separated line");
			}
		}
		return null;
	}
}
