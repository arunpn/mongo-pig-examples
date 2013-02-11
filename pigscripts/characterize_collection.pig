/*
    This pig script will return some basic information about a MongoDB collection.  Output is:

    1. Field Name.  Embedded fields have their parent's field name prepended to their name.
            Every field that appears in any document in the collection is listed.
    2. Unique value count.  The number of unique values associated with the field.
    3. Example value.  An example value for the field.
    4. Example value type.  The data type of the example value.
    5. Value count.  The number of times the example value appeared for this field in the collection

    Each field is listed up to five times with their five most common example values.
 */

REGISTER '../udfs/python/mongo_util.py' USING streaming_python AS mongo_util;

data = LOAD 'mongodb://readonly:readonly@ds035147.mongolab.com:35147/twitter.tweets' 
       USING com.mongodb.hadoop.pig.MongoLoader();

-- Create one row for every field in the document
raw_fields =  FOREACH data 
             GENERATE flatten(mongo_util.mongo_map(document));

-- Group the rows by field name and find the number of unique values for each field in the collection
key_groups = GROUP raw_fields BY (keyname);
unique_vals = FOREACH key_groups {
    v = raw_fields.val;
    unique_v = distinct v;
    GENERATE flatten(group)  as keyname:chararray, 
             COUNT(unique_v) as num_vals_count:long;
}

-- Find the number of times each value occurs for each field
key_val_groups = GROUP raw_fields BY (keyname, type, val);
key_val_groups_with_counts =  FOREACH key_val_groups 
                             GENERATE flatten(group), 
                                      COUNT($1) as val_count:long;

-- Find the top 5 most common values for each field
key_vals = GROUP key_val_groups_with_counts BY (keyname);
top_5_vals = FOREACH key_vals {
    ordered_vals = ORDER key_val_groups_with_counts BY val_count DESC;
    limited_vals = LIMIT ordered_vals 5;
    GENERATE flatten(limited_vals);
}

-- Join unique vals with top 5 values
join_result = JOIN unique_vals BY keyname, 
                   top_5_vals  BY keyname;

-- Clean up columns (remove duplicate keyname field)
result =  FOREACH join_result 
         GENERATE unique_vals::keyname, 
                  num_vals_count, 
                  val, 
                  type, 
                  val_count;

-- Sort by field name and number of values
out = ORDER result BY unique_vals::keyname, val_count DESC;

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out;
STORE out INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out' 
         USING PigStorage('\t');
