package postgres;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.commons.io.FileUtils;

/**
 * @author semmel
 *
 */
/**
 * @author semmel
 *
 */
/**
 * @author semmel
 *
 */
public class ImportData {

	private static PostgresConnector pc = new PostgresConnector("election", "elect2016", "178.254.35.26", "Election");
	private static Connection connection;
	private static String separator;

	public static void main(String[] argv) throws SQLException, IOException {

		File csvBaseFile = new File("C:\\Users\\semmel\\Documents\\DBS\\Projekt\\american-election-tweets.csv");
		// set the specific separator
		separator = ";";

		// clean the dirty file with the cleanUpCSV() method
		cleanUpCSV("C:\\Users\\semmel\\Documents\\DBS\\Projekt\\american-election-tweets.csv");

		// get the connection for import
		connection = pc.getConnection();

		// read all lines and import all valid lines to the database
		readAndImportCsv(csvBaseFile);

	}

	/**
	 * Count all occurrences of the separator in the line
	 * 
	 * @param String
	 *            line
	 * @return int count
	 */
	private static int columnCount(String line) {
		int count = 0;
		while (line.contains(separator)) {
			if (line.indexOf(separator) == -1)
				break;

			line = line.substring(line.indexOf(separator) + 1);
			// System.out.println(line.indexOf(seperator)+","+line);
			count++;
			// System.out.println(count);
		}
		return count;
	}

	/**
	 * Returns the first field as String
	 * 
	 * @param line
	 * @return String field
	 */
	private static String getNextField(String line) {
		String field = line.substring(0, line.indexOf(separator));
		line = line.substring(line.indexOf(";") + 1, line.length());
		return field;
	}

	/**
	 * Returns the Line without the first field
	 * 
	 * @param line
	 * @return String line
	 */
	private static String getNext(String line) {
		return line.substring(line.indexOf(separator) + 1, line.length());
	}

	/**
	 * Iterates over all lines, validate them and imports every valid row into
	 * the connected Database
	 * 
	 * @param file
	 * @throws FileNotFoundException
	 */
	private static void readAndImportCsv(File file) throws FileNotFoundException {

		// use a simple scanner to read the lines, alternatively a
		// BufferedReader(FileReader) could be used
		Scanner scanner = new Scanner(file);
		// read while there are lines in the file
		int linecount=0;
		while (scanner.hasNext()) {
			String currentLine = scanner.nextLine();
			linecount++;
			try {
				// check if its a valid csv line, separated by ";" with the
				// specific row count
				if (columnCount(currentLine) == 10) {

					// get the handle from first field
					String handle = getNextField(currentLine);
					currentLine = getNext(currentLine);
					// get text from second field
					String text = getNextField(currentLine);
					// trim "
					text = text.replaceAll("\"", "");

					// extract all hashtags
					List<String> hashtags = getHashtags(text);

					// jump over the next three columns
					currentLine = getNext(currentLine);
					currentLine = getNext(currentLine);
					currentLine = getNext(currentLine);

					// get the datestring from filed five
					String dateStr = getNextField(currentLine);

					Date date = Date.valueOf(dateStr.substring(0, currentLine.indexOf("T")));

					// jump over to field eight
					currentLine = getNext(currentLine);
					currentLine = getNext(currentLine);
					currentLine = getNext(currentLine);

					// read the retweetcount from field eight
					String retweet_count = getNextField(currentLine);

					// read the favorite count from field nine
					currentLine = getNext(currentLine);
					String favorite_count = getNextField(currentLine);
					
					// import to the database
					persistIntoDb(handle, text, date, Integer.parseInt(retweet_count), Integer.parseInt(favorite_count),
							hashtags);
					System.out.println(linecount);
				} else {
					// Oh, line not valid, but we throw no exception, to avoid
					// breaking the import
					System.out.println("Skipped unvalid line "+linecount);
				}
			} catch (IllegalArgumentException e) {
				// Not good, the date parsing fails, which means that the line
				// was not imported, but the import continues
				e.printStackTrace();
			}

		}
		scanner.close();
	}

	/**
	 * Import the contents to the database.
	 * 
	 * @param handle
	 * @param text
	 * @param date
	 * @param retweet_count
	 * @param favorite_count
	 * @param hashtags
	 */
	private static void persistIntoDb(String handle, String text, Date date, int retweet_count, int favorite_count,
			List<String> hashtags) {
		try {
			if (connection != null) {

				// Check if the handle exists
				PreparedStatement accountId = connection
						.prepareStatement("SELECT * FROM twitteraccount WHERE handle = ?  ;");
				accountId.setString(1, handle);

				ResultSet rs = accountId.executeQuery();

				if (!rs.next()) {// it's a new handle -> insert handle
					
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO twitteraccount (handle) VALUES  (?);");
					insert.setString(1, handle);
					insert.execute();
					System.out.println("New Handle " + handle +" inserted");
					rs = accountId.executeQuery();

				}
				// get the ID!
				int account_id = rs.getInt(1);

				// Check if the tweet exists
				PreparedStatement tweetId = connection.prepareStatement("SELECT * FROM tweet WHERE contenttext = ?  ;");
				tweetId.setString(1, text);
				rs = tweetId.executeQuery();

				if (!rs.next()) {// it's a new tweet -> insert tweet
					
					PreparedStatement insert = connection.prepareStatement(
							"INSERT INTO tweet (tweetdate,contenttext,favorite_count,retweet_count) VALUES  (?,?,?,?);");
					insert.setDate(1, date);
					insert.setString(2, text);
					insert.setInt(3, favorite_count);
					insert.setInt(4, retweet_count);
					insert.execute();
					System.out.println("New Tweet " + text + " inserted");
					rs = tweetId.executeQuery();
					rs.next();// set position of rs
				}
				// get the ID
				int tweet_Id = rs.getInt(1);
//				System.out.println("TweetId: " + tweet_Id);

				for (String tag : hashtags) {

					// check if the tag exists
					PreparedStatement hashTagId = connection
							.prepareStatement("SELECT * FROM hashtag WHERE hashtag = ?  ;");
					hashTagId.setString(1, tag);
					rs = hashTagId.executeQuery();

					if (!rs.next()) {// it's a new tag -> insert tag

						PreparedStatement insert = connection
								.prepareStatement("INSERT INTO hashtag (hashtag) VALUES  (?);");
						insert.setString(1, tag);
						insert.execute();
						rs = hashTagId.executeQuery();
						System.out.println("New Hashtag " + tag + " inserted");
						rs.next();
					}
					// get the ID
					int hashTag_Id = rs.getInt(1);

					// Check if the relation Hashtag to Tweet exists
					PreparedStatement tweetToTextId = connection.prepareStatement(
							"SELECT * FROM tweet_belongs_to_hashtags WHERE hashtagid = ? AND tweetid = ?  ;");
					tweetToTextId.setInt(1, hashTag_Id);
					tweetToTextId.setInt(2, tweet_Id);
					rs = tweetToTextId.executeQuery();

					if (!rs.next()) {// new tagToTweett Relation
						System.out.println("New Tag to Text Relation");
						PreparedStatement insert = connection.prepareStatement(
								"INSERT INTO tweet_belongs_to_hashtags (hashtagid,tweetid) VALUES  (?,?);");
						insert.setInt(1, hashTag_Id);
						insert.setInt(2, tweet_Id);
						insert.execute();
						System.out.println("New Tag to Text Relations inserted");

					}

				}
				// Check if the relation Account to Tweet exists
				PreparedStatement accountToTweetId = connection
						.prepareStatement("SELECT * FROM account_tweets WHERE accountid = ? AND tweetid = ?  ;");
				accountToTweetId.setInt(1, account_id);
				accountToTweetId.setInt(2, tweet_Id);
				rs = accountToTweetId.executeQuery();

				if (!rs.next()) {// new AccountToTweet Relation-> insert
					
					PreparedStatement insert = connection
							.prepareStatement("INSERT INTO account_tweets (accountid,tweetid) VALUES  (?,?);");
					insert.setInt(1, account_id);
					insert.setInt(2, tweet_Id);
					insert.execute();
					System.out.println("New Account to Tweet Relation inserted");

				}

			} else {
				System.out.println("No connection!");
			}
		} catch (SQLException e) {
			// ups, ...
			e.printStackTrace();
		}

	}

	/**
	 * Extracts all Hashtags from the given text
	 * 
	 * @param text
	 * @return List<String> of Hashtags
	 */
	private static List<String> getHashtags(String text) {
		List<String> hashtags = new ArrayList<String>();

		while (text.contains("#")) {
			// get the String after # until " "
			int startHashtag = text.indexOf("#");
			int endHashtag = text.indexOf(" ", text.indexOf("#")) > 0 ? text.indexOf(" ", text.indexOf("#"))
					: text.length();
			String tag = text.substring(startHashtag, endHashtag);
			if (tag.length() > 1)
				hashtags.add(tag); // avoid empty hashtags
			text = text.substring(endHashtag, text.length());
		}
		return hashtags;
	}

	/**
	 * cleans the text file under given path to enable reading by line (1)
	 * Remove all hard spaces (2) Remove all single <LF>
	 * 
	 * @param path
	 * @throws IOException
	 */
	private static void cleanUpCSV(String path) throws IOException {
		File file = new File(path);
		// read in File to String
		String str = FileUtils.readFileToString(file);

		// remove hard spaces
		str = str.replaceAll(".co/ZHMKIqnUwL ", "");
		// remove single <LF>
		str = str.replaceAll("\r\n", "CARIA" + "GERETURN_AND_LINE");
		str = str.replaceAll("(\\n)", "");
		str = str.replaceAll("CARIAGERETURN_AND_LINE", "\r\n");
		// write String to File
		FileUtils.writeStringToFile(file, str);

	}

}
