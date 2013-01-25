/*
    This pig script will return a single text field which is the Pig schema of the collection loaded.  

    This schema can be copied directly into the MongoLoader constructor to load the collection.
*/

REGISTER '../udfs/python/mongo_util.py' USING streaming_python AS mongo_util;

data =  LOAD 'mongodb://readonly:readonly@ds035147.mongolab.com:35147/twitter.tweets' 
       USING com.mongodb.hadoop.pig.MongoLoader();

-- Create one row for every field in the document
raw_fields =  FOREACH data 
             GENERATE flatten(mongo_util.mongo_map(document));

-- For each field in the document get a count of the types of the values for that field.
key_type_groups = GROUP raw_fields BY (keyname, type);
key_type_counts =  FOREACH key_type_groups 
                  GENERATE flatten(group), 
                           COUNT(raw_fields.keyname) as count:long;

-- Prepare the collection data for the Python UDF call that will go through the field/type information
-- and construct a single schema string.
name_groups = GROUP key_type_counts BY keyname;
all_keys = GROUP name_groups all;
out = FOREACH all_keys {
    results = ORDER name_groups BY group;
    GENERATE mongo_util.create_mongo_schema(results);
}

rmf s3n://jkarn-dev/$MORTAR_EMAIL_S3_ESCAPED/schema_out;
STORE out  INTO 's3n://jkarn-dev/$MORTAR_EMAIL_S3_ESCAPED/schema_out' 
          USING PigStorage('\t');
