CREATE TABLE TwitterAccount (accountId SERIAL PRIMARY KEY,handle VARCHAR(128) NOT NULL);

CREATE TABLE Tweet (tweetId SERIAL PRIMARY KEY, tweetDate DATE, contentText TEXT, favorite_count INTEGER, retweet_count INTEGER) ;

CREATE TABLE Hashtag (hashtagId SERIAL PRIMARY KEY, hashtag VARCHAR(128)) ;
CREATE UNIQUE INDEX CONCURRENTLY hashtag_IDX ON Hashtag (hashtag);

CREATE TABLE Tweet_Belongs_To_Hashtags (hashtagId INTEGER, tweetId INTEGER,CONSTRAINT prim_key PRIMARY KEY(hashtagId,tweetId)); 

ALTER TABLE tweet_belongs_to_hashtags ADD CONSTRAINT tweet_FK FOREIGN KEY (tweetId) REFERENCES tweet (tweetId) MATCH FULL;
ALTER TABLE tweet_belongs_to_hashtags ADD CONSTRAINT hashtag_FK FOREIGN KEY (hashtagId) REFERENCES hashtag (hashtagId) MATCH FULL;


CREATE TABLE Account_tweets (accountId INTEGER, tweetId INTEGER,CONSTRAINT account_tweets_key PRIMARY KEY(accountId,tweetId)); 

ALTER TABLE account_tweets ADD CONSTRAINT account_FK FOREIGN KEY (accountId) REFERENCES twitteraccount (accountId) MATCH FULL;
ALTER TABLE account_tweets ADD CONSTRAINT tweet_FK FOREIGN KEY (tweetId) REFERENCES tweet (tweetId) MATCH FULL;