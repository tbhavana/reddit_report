#### Run this using  - 
# python -m unittest test_trending.TrendingTestCase
####
import unittest
from luigi.parameter import DateSecondParameter, IntParameter
import praw
import datetime
from os import environ as env
from dotenv import load_dotenv
import luigi

import luigi
import luigi.interface
from luigi.mock import MockTarget

load_dotenv()

class GetTrendingSubreddits(luigi.Task):

    N = IntParameter()
    date = DateSecondParameter(default=datetime.datetime.now())
    
    #Initializing reddit instance using the praw package
    reddit = praw.Reddit(
            client_id=env["CLIENT_ID"],
            client_secret=env["SECRET_KEY"],
            user_agent="Report App/1.0 by tbhavana25",
            username=env["User"],
            password=env["password"],
        )

    def output(self):  
        return MockTarget('/tmp/subreddits_%d' % self.N)

    def run(self):
        
        subredd = self.reddit.subreddits.popular(limit=self.N) # using the 'popular' option from praw package
        subs = []
        with self.output().open('w') as out:
            for i in subredd:
                subs.append(i)
                out.write(i.display_name)
                out.write("\n")



class TrendingTestBase(unittest.TestCase):
    def setUp(self) -> None:
        MockTarget.fs.clear()

class TrendingTestCase(TrendingTestBase):
    
    def test_cmdline(self):
        luigi.run(['--local-scheduler', '--no-lock', 'GetTrendingSubreddits', '--N', '5'])
        luigi.run(['--local-scheduler', '--no-lock', 'GetTrendingSubreddits', '--N', '10'])

        self.assertEqual(MockTarget.fs.get_data('/tmp/subreddits_5'), b'Home\r\nAskReddit\r\nffxiv\r\nGenshin_Impact\r\nmemes\r\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/subreddits_10'), b'Home\r\nAskReddit\r\nffxiv\r\nGenshin_Impact\r\nmemes\r\nPublicFreakout\r\ngtaonline\r\nMinecraft\r\nmovies\r\nAmItheAsshole\r\n')
