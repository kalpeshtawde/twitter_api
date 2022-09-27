import os
import pandas as pd
from time import sleep
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from app.management.commands.tweet import QuoteTweet, LikingUsers, \
    RetweetedBy, TweetReply, UserTweets, UserMentions, UserLookup


def get_likes(tweet_id, next_token=None, prev_likes=[]):
    print(f"Getting data for user likes {next_token}")

    likes = prev_likes

    lk = LikingUsers(tweet_id=tweet_id, next_token=next_token)
    user_likes = lk.main()

    if 'data' in user_likes:
        likes.extend(user_likes['data'])

    if 'next_token' in user_likes['meta']:
        get_likes(tweet_id, user_likes['meta']['next_token'], likes)

    if likes:
        df = pd.DataFrame.from_records(
            likes,
            columns=['username', 'likes', 'created_at'],
        )
        df['likes'] = 1
        df = df.set_index('username').sort_index(axis=0)
        print(f"************ {tweet_id}")
        print(df)
        return df
    return None


def get_retweet(tweet_id, next_token=None, prev_retweet=[]):
    print(f"Getting data for user retwets {next_token}")

    retweets = prev_retweet

    rb = RetweetedBy(tweet_id=tweet_id, next_token=next_token)
    retweet = rb.main()

    if 'data' in retweet:
        retweets.extend(retweet['data'])

    if 'next_token' in retweet['meta']:
        get_retweet(tweet_id, retweet['meta']['next_token'], retweets)

    if retweets:
        df = pd.DataFrame.from_records(
            retweets,
            columns=['username', 'retweets', 'created_at'],
        )
        df['retweets'] = 1
        df = df.set_index('username')
        return df
    return None


def get_quote_retweet(tweet_id, next_token=None, prev_quotes=[], prev_users=[]):
    quotes = prev_quotes
    users = prev_users

    qt = QuoteTweet(tweet_id=tweet_id, next_token=next_token)
    quote_retweet = qt.main()

    if 'data' in quote_retweet:
        quotes.extend(quote_retweet['data'])
        users.extend(quote_retweet['includes']['users'])

    if 'next_token' in quote_retweet['meta']:
        get_quote_retweet(tweet_id, quote_retweet['meta']['next_token'],
                          quotes, users)

    if quotes:
        df1 = pd.DataFrame.from_records(
            quotes,
            columns=['author_id', 'quote_retweets', 'created_at']
        )
        df1['quote_retweets'] = 1
        df2 = pd.DataFrame.from_records(
            users,
            columns=['id', 'username']
        )
        df2 = df2.drop_duplicates()
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username')
        return df

    return None


def get_tweet_reply(tweet_id, next_token=None, prev_reply=[], prev_users=[]):
    reply = prev_reply
    users = prev_users

    tr = TweetReply(tweet_id=tweet_id, next_token=next_token)
    tweet_reply = tr.main()

    if 'data' in tweet_reply:
        reply.extend(tweet_reply['data'])
        users.extend(tweet_reply['includes']['users'])

    if 'next_token' in tweet_reply['meta']:
        get_tweet_reply(tweet_id, tweet_reply['meta']['next_token'],
                          reply, users)

    if reply:
        df1 = pd.DataFrame.from_records(
            reply,
            columns=['author_id', 'tweet_reply', 'created_at']
        )
        df1['tweet_reply'] = 1
        df2 = pd.DataFrame.from_records(
            users,
            columns=['id', 'username']
        )
        df2 = df2.drop_duplicates()
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username')

        return df
    return None


def get_tweet_stat(tweet_id):
    likes_df = get_likes(tweet_id, prev_likes=[])
    retweet_df = get_retweet(tweet_id, prev_retweet=[])
    quote_retweet_df = get_quote_retweet(tweet_id, prev_quotes=[])
    tweet_reply_df = get_tweet_reply(tweet_id, prev_reply=[])

    status = all(item is None for item in [likes_df, retweet_df,
                                       quote_retweet_df, tweet_reply_df])
    if not status:
        result = pd.concat([likes_df, retweet_df, quote_retweet_df, tweet_reply_df])
        result = result.sort_index(axis=0)
        for col in ['likes', 'retweets', 'quote_retweets',
                         'tweet_reply', 'created_at']:
            if col not in result.columns:
                result[col] = 0
        result = result[['likes', 'retweets', 'quote_retweets',
                         'tweet_reply', 'created_at']]
        return result
    return None


def get_user_tweets(user_id, start_date, end_date, next_token=None, prev_tweets=[]):
    print(f"Getting user tweets with token {next_token}")
    tweets = prev_tweets
    ut = UserTweets(user_id=user_id, start_date=start_date,
                    end_date=end_date, next_token=next_token)
    user_tweets = ut.main()
    tweets.extend([tweet['id'] for tweet in user_tweets['data']])

    if 'next_token' in user_tweets['meta']:
        get_user_tweets(user_id, start_date, end_date, user_tweets['meta']['next_token'], tweets)

    if tweets:
        results = []
        for tweet_id in tweets:
            result = get_tweet_stat(tweet_id)
            if result is not None:
                result = result.drop('created_at', axis=1)
                results.append(result)
            sleep(1)

        if results:
            df = pd.concat(results)
            df = df.groupby('username').sum()
            return df
    return None


def get_user_mentions(user_id, start_date, end_date, next_token=None,
                      prev_mentions=[], prev_users=[]):
    print(f"Getting user mentions with token {next_token}")
    mentions = prev_mentions
    users = prev_users
    um = UserMentions(user_id=user_id, start_date=start_date,
                      end_date=end_date, next_token=next_token)
    user_mentions = um.main()

    if 'data' in user_mentions:
        mentions.extend(user_mentions['data'])
        users.extend(user_mentions['includes']['users'])

    if 'next_token' in user_mentions['meta']:
        get_user_mentions(user_id, start_date, end_date, user_mentions[
            'meta']['next_token'], mentions, users)

    if mentions:
        df1 = pd.DataFrame.from_records(
            mentions,
            columns=['author_id', 'user_mentions']
        )
        df1['user_mentions'] = 1
        df2 = pd.DataFrame.from_records(
            users,
            columns=['id', 'username']
        )
        df2 = df2.drop_duplicates()
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username').groupby('username').sum()
        return df
    return None


def write_excel(result):
    writer = pd.ExcelWriter('/code/output.xlsx')
    result.to_excel(writer)
    writer.save()
    writer.close()


def run_tweet_data(tweet_id):
    print("Writing data to output.xlsx")
    result = get_tweet_stat(tweet_id)
    print(result)
    write_excel(result)


def run_user_data(user_id, start_date, end_date):
    tweets = get_user_tweets(user_id, start_date, end_date, prev_tweets=[])
    mentions = get_user_mentions(user_id, start_date, end_date, prev_mentions=[], prev_users=[])

    status = all(item is None for item in [tweets, mentions])

    if not status:
        result = pd.concat([tweets, mentions])
        result = result.groupby('username').sum()
        print("#########################")
        print(result)
        write_excel(result)


def get_userid(username):
    ul = UserLookup(username=username)
    user_data = ul.main()
    if 'data' in user_data:
        return user_data['data'][0]['id']
    return None


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--tweet_id', nargs='+', type=int)
        parser.add_argument('--user_id', nargs='+', type=str)
        parser.add_argument('--start_date', nargs='+', type=str)
        parser.add_argument('--end_date', nargs='+', type=str)

    def handle(self, *args, **options):
        if os.path.exists('/code/output.xlsx'):
            os.unlink('/code/output.xlsx')

        if options['tweet_id'] is not None:
            print(f"Tweet id processing: {options['tweet_id'][0]}")
            run_tweet_data(options['tweet_id'][0])
        elif options['user_id'] is not None:
            print(f"User id processing: {options['user_id'][0]}")

            start_date = end_date = None
            if options['start_date'][0] != '':
                start_date = f"{options['start_date'][0]}T00:00:00Z"
                end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            if options['end_date'][0] != '':
                utcnow = datetime.utcnow().strftime('%Y-%m-%d')
                if utcnow == options['end_date'][0]:
                    end_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    end_date = f"{options['end_date'][0]}T23:59:59Z"

            user_id = get_userid(options['user_id'][0])

            if user_id:
                run_user_data(user_id, start_date, end_date)
            else:
                raise f"User not found: {options['user_id'][0]}"
