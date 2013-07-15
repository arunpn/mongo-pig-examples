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
    This pig script will return a single text field which is the Pig schema of the collection loaded.  

    This schema can be copied directly into the MongoLoader constructor to load the collection.
*/

REGISTER '../udfs/python/mongo_util.py' USING streaming_python AS mongo_util;

/*
 To calculate input splits Hadoop makes a call that requires admin privileges in MongoDB 2.4+.

 If you are connecting as a user with admin privileges you should remove this line for much better
 performance.
*/
SET mongo.input.split.create_input_splits false

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

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/schema_out;
STORE out  INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/schema_out' 
          USING PigStorage('\t');
