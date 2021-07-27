import datetime
import json
from os import environ as env

import praw
from praw.models import MoreComments

from luigi import Task, LocalTarget
from luigi.parameter import IntParameter, DateSecondParameter

from dotenv import load_dotenv

load_dotenv() # loads the secret information from .env file

class GetTrendingSubreddits(Task):

    '''Gets the top N subreddits and stores them in a text file as output for the next task in pipeline'''

    N = IntParameter() #number of top subreddits to fetch default is 50, can be changed in config file
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
        return LocalTarget(self.date.strftime('TopKSubreddits_%Y-%m-%d_%H-%M-%S.txt'))

    def run(self):
        
        subredd = self.reddit.subreddits.popular(limit=self.N) # using the 'popular' option from praw package
        subs = []
        with self.output().open('w') as out:
            for i in subredd:
                subs.append(i)
                out.write(i.display_name)
                out.write("\n")


class GetTopPosts(Task):

    '''This step in the pipeline gets the top N posts for each subreddit
        Outputs a json file with dictionary of format 
        {
            subreddit_name : [...List of post ids...]
        }
    '''

    N = IntParameter() #number of top posts to fetch default is 10, can be changed in config file
    date = DateSecondParameter(default=datetime.datetime.now())
    reddit = GetTrendingSubreddits.reddit

    def requires(self):
        return GetTrendingSubreddits()
    
    def output(self):
        return LocalTarget(self.date.strftime('topPosts_%Y-%m-%d_%H-%M-%S.json'))
    
    def run(self):
        top_submissions = {}
        with self.input().open('r') as inp:
            for line in inp:
                sub = line.rstrip()
                top_submissions[sub] = []
                for submission in self.reddit.subreddit(sub).top(limit=self.N):
                    top_submissions[sub].append(submission.id)
        
        with self.output().open('w') as out:
            json.dump(top_submissions, out)


class ComputePostScores(Task):

    '''gets the comments for each post and calculates the Post scores as (sum of all comment scores) / N
        Outputs a PostScores.json file which is used by next task to compute the final SubredditScore
    '''

    N = IntParameter() # Number of comments to be fetched for each post
    date = DateSecondParameter(default=datetime.datetime.now())
    reddit = GetTrendingSubreddits.reddit # reddit instance

    def requires(self):
        return GetTopPosts()

    def output(self):
        return LocalTarget(self.date.strftime('PostScores_%Y-%m-%d_%H-%M-%S.json'))

    def run(self):
        with self.input().open('r') as inp:
            top_submissions = json.load(inp)
        
        post_scores = {}
        for key, val in top_submissions.items():
            for id in val:
                submission = self.reddit.submission(id=id)
                submission.comment_sort = 'top'
                submission.comment_limit = self.N

                comment_score = 0
                for top_level_comment in submission.comments:
                    if isinstance(top_level_comment, MoreComments):
                        continue
                    
                    else:
                        comment_score += top_level_comment.score
                
                if key in post_scores:
                    post_scores[key].append(comment_score//self.N)
                else:
                    post_scores[key] = []
                    post_scores[key].append(comment_score//self.N)
        
        with self.output().open('w') as out:
            json.dump(post_scores, out)

            
class ComputeSubredditScores(Task):

    '''Computes the subreddit score from post scores using the formula 
        sub_score = (sum of post_scores) / N , N = # of posts
    '''
    date = DateSecondParameter(default=datetime.datetime.now())

    def requires(self):
        return ComputePostScores()
    
    def output(self):
        return LocalTarget(self.date.strftime('SubredditScores_%Y-%m-%d_%H-%M-%S.txt'))
    
    def run(self):
        with self.input().open('r') as inp:
            post_scores = json.load(inp)
        
        
        with self.output().open('w') as out:
            for key, val in post_scores.items():
                num_posts = len(val)
                sub_score = 0
                for post_score in val:
                    sub_score += post_score
                
                out.write('{} {}'.format(key, sub_score/num_posts))
                out.write("\n")
        
