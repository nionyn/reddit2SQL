import file_functions
import sqlite3

from pathlib import Path
from collections import OrderedDict

conn = sqlite3.connect('database.db')

domain_list = []
subreddit_list = []
retrieve = ['url', 'domain', 'subreddit', 'ups', 'id', 'author', 'gilded', 'title', 'upvote_ratio', 'subreddit_id', 'author_fullname', 'created_utc']
retrieved = {}

c = conn.cursor()

def processFiles():
        for file in file_functions.getdir():
            try:
                data = file_functions.getfile(file)
                for n, t in enumerate(data["data"]["children"]):

                    #get multiple values from json
                    for r in retrieve:
                        try:
                            retrieved[r] = t["data"][r]
                        except:
                            retrieved[r] = ''

                    #special values added
                    retrieved['position'] = n+1
                    retrieved['timestamp'] = Path(file).stem
                    columns = ','.join((x for x in retrieved.keys()))

                    #submissions
                    sql = '''INSERT OR IGNORE INTO subreddits (subreddit_id, subreddit) VALUES (?,?)'''
                    values = [retrieved["subreddit_id"], retrieved["subreddit"]]
                    c.execute(sql, (values))

                    #id, author_fullname, subreddit_id, created_utc
                    sql = '''INSERT OR IGNORE INTO submissions (id, author_fullname, subreddit_id, created_utc) VALUES (?,?,?,?)'''
                    values = [retrieved["id"], retrieved["author_fullname"], retrieved["subreddit_id"], retrieved["created_utc"]]
                    c.execute(sql, (values))

                    #authors
                    sql = '''INSERT OR IGNORE INTO authors (author_fullname, author) VALUES (?,?)'''
                    values = [retrieved["author_fullname"], retrieved["author"]]
                    c.execute(sql, (values))

                    #snapshots
                    #values =  ','.join(("'"+ str(x) +"'" for x in retrieved.values()))
                    values = [retrieved["id"], retrieved["title"], retrieved["position"], retrieved["ups"], retrieved["author_fullname"], 
                              retrieved["gilded"], retrieved["upvote_ratio"], retrieved["subreddit_id"], retrieved["timestamp"]]
                    sql = '''INSERT OR IGNORE INTO snapshots (id, title, position, ups, 
                    author_fullname, gilded, upvote_ratio, subreddit_id, timestamp) VALUES (?,?,?,?,?,?,?,?,?)'''
                    c.execute(sql, values)
                

                file_functions.movefile(file)
                print('processed {} sucessfully'.format(file))

            except:
                file_functions.errorfile(file)
                print('{}, file error'.format(file))
    
def closedb():
    conn.commit()
    conn.close()



