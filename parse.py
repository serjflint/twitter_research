import html
import json
import logging
import sys
import time

import psycopg2
from progressbar import Bar, FormatLabel, ProgressBar, AdaptiveETA

import config


# Count lines in a file
def file_len(fname):
    with open(fname) as f:
        i = 0
        for i, l in enumerate(f):
            pass
    return i + 1


# Check arguments
if len(sys.argv) > 1:
    table_name = sys.argv[1]
    file_name = 'D:/workspace/stream_' + table_name + '.json'  # File with records
    log_name = 'D:/workspace/parse_' + table_name + '.log'  # Log file
else:
    print("Too few arguments")
    sys.exit()

# Initialize logging format and file
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=log_name,
                    level=logging.DEBUG)

# Progressbar format
widgets = [
    FormatLabel('%(value)d of %(max_value)d'),
    ' ', Bar(marker='>', left='[', right=']'),
    ' ', AdaptiveETA(),
]

# Start program
print("Start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
logging.info("Started")

# Connect to database
try:
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}".format(
            config.DB_HOST, config.DB_NAME, config.DB_USER, config.DB_PASSWORD))
    cur = conn.cursor()
except psycopg2.OperationalError as err:
    logging.error("Connection to database failed")
    print("Connection error:", err)
    logging.info("Finished")
    sys.exit(-1)

# Table creation query
create_query = """
CREATE TABLE public.\"{0}\"
(
  tweet_id bigint PRIMARY KEY,
  user_id bigint,
  created_at timestamp with time zone,
  tweet_text character varying(1000),
  hashtags character varying(400)[],
  urls character varying(4000)[],
  user_mentions bigint[]
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.\"{0}\"
  OWNER TO admin;
""".format(table_name)

# Insert record query
insert_query = "INSERT INTO \"{0}\" \
(tweet_id, user_id, created_at, tweet_text, hashtags, urls, user_mentions) \
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;".format(table_name)

resume_query = "SELECT count(*) FROM \"{}\";".format(table_name)

resume_count = 0
resume_flag = False

# Create table for storing records
try:
    cur.execute(create_query)
except psycopg2.DatabaseError as err:
    logging.warning("Table already exists")
    resume_flag = True
finally:
    conn.commit()

if resume_flag:
    cur.execute(resume_query)
    resume_count = cur.fetchone()[0]
    # If table exists but empty
    if not resume_count:
        resume_count = 0
        resume_flag = False
    else:
        print('Resume from', resume_count)
        logging.warning("Resume from %d", resume_count)

# Open data file with records
with open(file_name) as data_file:
    progress = 0
    skipped = 0
    amount = file_len(file_name) // 2
    pbar = ProgressBar(widgets=widgets, max_value=amount).start()

    # Process all line in data file as records
    for line in data_file:
        # If line is not empty
        if line != "\n":
            # Try to parse record
            tweet = ""
            try:
                # Find resume position
                if resume_flag:
                    if progress == resume_count:
                        resume_flag = False
                    continue
                # Parse line as JSON
                tweet = json.loads(line)

                # Read all necessary fields
                tweet_id = tweet['id']
                user_id = tweet['user']['id']
                tweet_text = html.unescape(tweet['text'])

                # Convert to postgresql timestamp format
                created_at = time.strftime('%Y-%m-%d %H:%M:%S',
                                           time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))

                urls = []
                hashtags = []
                user_mentions = []

                for url in tweet['entities']['urls']:
                    if url['expanded_url']:
                        urls.append(url['expanded_url'])
                for hashtag in tweet['entities']['hashtags']:
                    hashtags.append(hashtag['text'])
                for user_mention in tweet['entities']['user_mentions']:
                    user_mentions.append(user_mention['id'])

                # Execute insert query
                cur.execute(insert_query, [tweet_id, user_id, created_at, tweet_text,
                                           hashtags, urls, user_mentions])

            except json.JSONDecodeError as err:
                logging.warning(err)
                skipped += 1
            except KeyError as err:
                logging.warning("Wrong structure of record: %s", tweet)
                skipped += 1
            except (psycopg2.DatabaseError, psycopg2.DataError, psycopg2.InternalError) as err:
                logging.error("Failed to store the record to database. %s %s", err, line)
                print('Progress', progress)
                print('Error', err)
                print('Tweet', line)
                skipped += 1
                conn.commit()
                sys.exit(-2)
            finally:
                progress += 1
                if progress % 1000 == 0:
                    pbar.update(progress)
                    if not resume_flag:
                        conn.commit()

    pbar.finish()

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()

time.sleep(1)
print('Finished', progress, 'of', amount, 'Skipped', skipped)
logging.info("Processed %s of %s", progress, amount)
logging.warning("Skipped %s", skipped)
print("Finish time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
logging.info("Finished")
