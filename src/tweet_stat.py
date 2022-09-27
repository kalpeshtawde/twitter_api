import pandas as pd
from time import sleep

from tweet import Tweet, QuoteTweet, LikingUsers, RetweetedBy, TweetReply, \
                UserTweets, UserMentions


def get_likes(tweet_id):
    lk = LikingUsers(tweet_id=tweet_id)
    likes = lk.main()
    if 'data' in likes:
        df = pd.DataFrame.from_records(
            likes['data'],
            columns=['username', 'likes', 'created_at'],
        )
        df['likes'] = 1
        df = df.set_index('username')
        return df
    return None


def get_retweet(tweet_id):
    rb = RetweetedBy(tweet_id=tweet_id)
    retweet = rb.main()
    if 'data' in retweet:
        df = pd.DataFrame.from_records(
            retweet['data'],
            columns=['username', 'retweets', 'created_at'],
        )
        df['retweets'] = 1
        df = df.set_index('username')
        return df
    return None


def get_quote_retweet(tweet_id):
    qt = QuoteTweet(tweet_id=tweet_id)
    quote_retweet = qt.main()
    if 'data' in quote_retweet:
        df1 = pd.DataFrame.from_records(
            quote_retweet['data'],
            columns=['author_id', 'quote_retweets', 'created_at']
        )
        df1['quote_retweets'] = 1
        df2 = pd.DataFrame.from_records(
            quote_retweet['includes']['users'],
            columns=['id', 'username']
        )
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username')
        return df

    return None


def get_tweet_reply(tweet_id):
    tr = TweetReply(tweet_id=tweet_id)
    tweet_reply = tr.main()
    if 'data' in tweet_reply:
        df1 = pd.DataFrame.from_records(
            tweet_reply['data'],
            columns=['author_id', 'tweet_reply', 'created_at']
        )
        df1['tweet_reply'] = 1
        df2 = pd.DataFrame.from_records(
            tweet_reply['includes']['users'],
            columns=['id', 'username']
        )
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username')

        return df
    return None


def get_tweet_stat(tweet_id):
    likes_df = get_likes(tweet_id)
    retweet_df = get_retweet(tweet_id)
    quote_retweet_df = get_quote_retweet(tweet_id)
    tweet_reply_df = get_tweet_reply(tweet_id)

    status = all(item is None for item in [likes_df, retweet_df,
                                       quote_retweet_df, tweet_reply_df])
    if not status:
        result = pd.concat([likes_df, retweet_df, quote_retweet_df, tweet_reply_df])
        #result = result.fillna(value='NA')
        result = result.sort_index(axis=0)
        return result
    return None


def get_user_tweets():
    print("Getting user tweets")
    ut = UserTweets(user_id=1251147194180096000)
    user_tweets = ut.main()
    tweets = [tweet['id'] for tweet in user_tweets['data']]
    if tweets:
        results = []
        for tweet_id in tweets:
            result = get_tweet_stat(tweet_id)
            if result is not None:
                result = result.drop('created_at', axis=1)
                results.append(result)
            sleep(1)
        df = pd.concat(results)
        df = df.groupby('username').sum()
        return df
    return None


def get_user_mentions():
    print("Getting user mentions")
    um = UserMentions(user_id=1251147194180096000)
    user_mentions = um.main()
    if 'data' in user_mentions:
        df1 = pd.DataFrame.from_records(
            user_mentions['data'],
            columns=['author_id', 'user_mentions']
        )
        df1['user_mentions'] = 1
        df2 = pd.DataFrame.from_records(
            user_mentions['includes']['users'],
            columns=['id', 'username']
        )
        df = pd.merge(df1, df2, left_on=['author_id'], right_on=['id'])
        df = df.drop('author_id', axis=1)
        df = df.drop('id', axis=1)
        df = df.set_index('username').groupby('username').sum()
        return df
    return None


def run():
    # writer = pd.ExcelWriter('output.xlsx')
    # result.to_excel(writer)
    # writer.save()
    # print(result)

    tweets = get_user_tweets()
    mentions = get_user_mentions()

    status = all(item is None for item in [tweets, mentions])
    if not status:
        result = pd.concat([tweets, mentions])
        result = result.groupby('username').sum()
        print(result)


if __name__=="__main__":
    run()