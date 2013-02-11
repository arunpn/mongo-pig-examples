## Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  

## Getting Started

Here we've included some example scripts that explore using MongoDB with Hadoop. These scripts use a sample of tweets from a single day loaded into a readonly publicly available MongoDB instance.  To start using them:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1.  Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:mortardata/mongo-pig-examples.git
        cd mongo-pig-examples
        mortar register mongo-pig-examples

Once you've setup the project, use the `mortar illustrate` command to show data flowing through a given script.  Use `mortar run` to run the script on a Hadoop cluster.

For lots more help and tutorials on running Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.

## Examples

### characterize_collection:

This pig script will return some basic information about a MongoDB collection.  Output is:

1. Field Name.  Embedded fields have their parent's field name prepended to their name.  Every field that appears in any document in the collection is listed.
1. Unique value count.  The number of unique values associated with the field.
1. Example value.  An example value for the field.
1. Example value type.  The data type of the example value.
1. Value count.  The number of times the example value appeared for this field in the collection

Each field is listed up to five times with their five most common example values.

### mongo_schema_generator:

This pig script will return a single text field which is the Pig schema of the collection loaded.  This schema can be copied directly into the MongoLoader constructor to load the collection.  See [Using MongoDB with Mortar](http://help.mortardata.com/#!/mongodb) for an explanation of why you might like to load your collection using a schema.

### hourly_coffee_tweets:

This pig script will go through a small sampling of a single day's worth of Tweets and count the number of times coffee was tweeted bucketed into two hour time blocks of the tweeter's local time.
