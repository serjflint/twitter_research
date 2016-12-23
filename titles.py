import html
import logging
import math
import re
import sys
import time

import eventlet
import psycopg2
import requests
from progressbar import Bar, FormatLabel, ProgressBar, ETA

import config

# Portion of records to process
chunk = 10

# Check arguments
if len(sys.argv) > 1:
    table = sys.argv[1]
    table_name = 'titles_' + table
    log_name = 'D:/workspace/' + table_name + '.log'  # Log file
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
    ' ', ETA(),
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

create_query = """
CREATE TABLE public.\"{0}\"
(
  tweet_id bigint PRIMARY KEY,
  titles character varying(1000)[]
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.\"{0}\"
OWNER TO admin;
""".format(table_name)

resume_query = "SELECT max(tweet_id) FROM \"{}\";".format(table_name)

count_query = "SELECT count(*) FROM \"{}\";".format(table)

select_query = "SELECT tweet_id, urls FROM \"{}\" ORDER BY tweet_id ASC".format(table)

insert_query = "INSERT INTO \"{}\" (tweet_id, titles) VALUES (%s, %s) ON CONFLICT DO NOTHING".format(table_name)

resume_id = 0
resume_flag = False

# Create table for storing records
try:
    cur.execute(create_query)
except psycopg2.DatabaseError as err:
    logging.warning("Table already exists")
    resume_flag = True
finally:
    conn.commit()

# If table exists resume from max id
if resume_flag:
    cur.execute(resume_query)
    resume_id = cur.fetchone()[0]
    # If table exists but empty
    if not resume_id:
        resume_id = 0
        resume_flag = False
    else:
        print('Resume from', resume_id)
        logging.warning("Resume from %d", resume_id)

# Count the number of records to process
cur.execute(count_query)
amount = cur.fetchone()[0]

# Select all records to process
cur.execute(select_query)

# Get title from .html file
regex = re.compile('<title>(.*?)</title>', re.IGNORECASE | re.DOTALL)

# Separate cursor for insert_query
cur_insert = conn.cursor()

# Create progress bar
progress = 0
skipped = 0
pbar = ProgressBar(widgets=widgets, max_value=amount).start()

for index in range(math.ceil(amount / chunk)):
    rows = cur.fetchmany(size=chunk)
    for row in rows:
        url = ""
        try:
            # Find resume position
            if resume_flag:
                if row[0] == resume_id:
                    resume_flag = False
                continue

            # Skip empty urls
            if len(row[1]) == 0:
                raise IndexError

            titles = []
            for url in row[1]:
                with eventlet.Timeout(15, TimeoutError):
                    r = requests.get(url, timeout=(3, 15))
                title = regex.search(r.text).group(1)
                if len(title) >= 1000:
                    raise IndexError
                title = html.unescape(title)
                title = title.replace("\n", " ")
                title = title.strip(' \t\r')
                titles.append(title)

            if len(titles) > 0:
                cur_insert.execute(insert_query, [row[0], titles])
            else:
                skipped += 1

        except (AttributeError, IndexError):
            logging.warning("Title extraction error")
            skipped += 1
        except (TimeoutError, requests.exceptions.ReadTimeout):
            logging.warning("Timeout error")
            skipped += 1
        except (UnicodeError, requests.exceptions.ContentDecodingError):
            logging.warning("Decoding error")
            skipped += 1
        except (requests.exceptions.ConnectionError,
                requests.exceptions.TooManyRedirects,
                requests.exceptions.ChunkedEncodingError) as err:
            logging.warning("Connection error, %s, %s", err, url)
            skipped += 1
        finally:
            progress += 1
            pbar.update(progress)
            if not resume_flag and progress % chunk == 0:
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
