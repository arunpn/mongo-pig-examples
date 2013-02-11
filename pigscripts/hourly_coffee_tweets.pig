/*
    Copyright 2013 Mortar Data Inc.
        
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
        
        http://www.apache.org/licenses/LICENSE-2.0
            
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
*/

/*
    This pig script will go through a small sampling of a single day's worth of Tweets and 
    count the number of times coffee was tweeted bucketed into two hour time blocks of the
    tweeter's local time.
 */

REGISTER '../udfs/python/coffeetweets.py' USING streaming_python AS coffeetweets;

tweets =  LOAD 'mongodb://readonly:readonly@ds035147.mongolab.com:35147/twitter.tweets' 
         USING com.mongodb.hadoop.pig.MongoLoader('created_at:chararray, text:chararray, user:tuple(utc_offset:int)');

-- Find the tweets that mention coffee and have valid time information
coffee_tweets = FILTER tweets BY text matches '.*[Cc]offee.*' AND user.utc_offset IS NOT NULL;

-- Calculate local time for each tweet
tweets_with_local_time = 
     FOREACH coffee_tweets 
    GENERATE coffeetweets.local_time(created_at, user.utc_offset) AS created_at_local_tz_iso;

-- Calculate time bucket for each tweet
tweets_with_time_buckets = 
     FOREACH tweets_with_local_time 
    GENERATE coffeetweets.hour_block(created_at_local_tz_iso) AS hour_block;

-- Count the number of tweets by time bucket
grouped = GROUP tweets_with_time_buckets BY hour_block;
counted =  FOREACH grouped 
          GENERATE group,
                   COUNT(tweets_with_time_buckets.hour_block) as num_tweets;

ordered = ORDER counted BY group ASC;

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/coffee_tweets_out;
STORE ordered  INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/coffee_tweets_out' 
              USING PigStorage('\t');
