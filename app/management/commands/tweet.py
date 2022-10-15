import requests
import os


class Tweet:
    def __init__(self, tweet_id=1572075632636628994, user_id=1251147194180096000):
        self.bearer_token = os.environ.get("BEARER_TOKEN")
        self.tweet_id = tweet_id
        self.user_id = user_id


class UserMentions(Tweet):
    def __init__(self, start_date=None, end_date=None, user_id=None,
                 next_token=None):
        super().__init__(user_id=user_id)
        self.start_date = start_date
        self.end_date = end_date
        self.next_token = next_token

    def create_url(self):
        return f"https://api.twitter.com/2/users/{self.user_id}/mentions"

    def get_params(self):
        return {
            'tweet.fields': 'author_id',
            'expansions': 'author_id',
            'user.fields': 'name,username',
            "start_time": self.start_date,
            "end_time": self.end_date,
            "pagination_token": self.next_token,
        }

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserTweetsPython"
        return r

    def connect_to_endpoint(self, url, params):
        response = requests.request("GET", url, auth=self.bearer_oauth,
                                    params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        params = self.get_params()
        json_response = self.connect_to_endpoint(url, params)
        return json_response


class UserTweets(Tweet):
    def __init__(self, start_date=None, end_date=None, user_id=None,
                 next_token=None):
        super().__init__(user_id=user_id)
        self.start_date = start_date
        self.end_date = end_date
        self.next_token = next_token

    def create_url(self):
        return f"https://api.twitter.com/2/users/{self.user_id}/tweets"

    def get_params(self):
        return {
            'tweet.fields': 'author_id',
            'expansions': 'author_id',
            'user.fields': 'name,username',
            "start_time": self.start_date,
            "end_time": self.end_date,
            "pagination_token": self.next_token,
        }

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserTweetsPython"
        return r

    def connect_to_endpoint(self, url, params):
        response = requests.request("GET", url, auth=self.bearer_oauth,
                                    params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        params = self.get_params()
        json_response = self.connect_to_endpoint(url, params)
        return json_response


class TweetReply(Tweet):
    def __init__(self, tweet_id=None, next_token=None):
        super().__init__(tweet_id=tweet_id)
        self.next_token = next_token

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    def get_params(self):
        return {
            'query': f'conversation_id:{self.tweet_id}',
            'tweet.fields': 'created_at,author_id',
            'expansions': 'author_id',
            'user.fields': 'name,username',
            'pagination_token': self.next_token,
        }

    def connect_to_endpoint(self, url, params):
        response = requests.get(url, auth=self.bearer_oauth, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def main(self):
        params = self.get_params()
        json_response = self.connect_to_endpoint(
            'https://api.twitter.com/2/tweets/search/recent', params)
        return json_response


class RetweetedBy(Tweet):
    def __init__(self, tweet_id=None, next_token=None):
        super().__init__(tweet_id=tweet_id)
        self.next_token = next_token

    def get_params(self):
        return {
            'user.fields': 'description',
            "pagination_token": self.next_token,
        }

    def create_url(self):
        url = "https://api.twitter.com/2/tweets/{}/retweeted_by".format(
            self.tweet_id)
        return url

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2LikingUsersPython"
        return r

    def connect_to_endpoint(self, url, user_fields):
        response = requests.request("GET", url, auth=self.bearer_oauth,
                                    params=user_fields)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        params = self.get_params()
        json_response = self.connect_to_endpoint(url, params)
        return json_response


class LikingUsers(Tweet):
    def __init__(self, tweet_id=None, next_token=None):
        super().__init__(tweet_id=tweet_id)
        self.next_token = next_token

    def get_params(self):
        return {
            'user.fields': 'description',
            "pagination_token": self.next_token,
        }

    def create_url(self):
        url = "https://api.twitter.com/2/tweets/{}/liking_users".format(
            self.tweet_id)
        return url

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2LikingUsersPython"
        return r

    def connect_to_endpoint(self, url, user_fields):
        response = requests.request("GET", url, auth=self.bearer_oauth,
                                    params=user_fields)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        params = self.get_params()
        json_response = self.connect_to_endpoint(url, params)
        return json_response


class QuoteTweet(Tweet):
    def __init__(self, tweet_id=None, next_token=None):
        super().__init__(tweet_id=tweet_id)
        self.next_token = next_token

    def create_url(self):
        return "https://api.twitter.com/2/tweets/{}/quote_tweets".format(
            self.tweet_id)

    def get_params(self):
        return {
            "tweet.fields": "created_at,author_id",
            "expansions": "author_id",
            "user.fields": "name,username",
            "pagination_token": self.next_token,
        }

    def create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def connect_to_endpoint(self, url, headers, params):
        response = requests.request("GET", url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        headers = self.create_headers(self.bearer_token)
        params = self.get_params()
        json_response = self.connect_to_endpoint(url, headers, params)
        return json_response


class UserLookup(Tweet):
    def __init__(self, username=None, tweet_id=None):
        super().__init__(tweet_id=tweet_id)
        self.username = username

    def create_url(self):
        usernames = f"usernames={self.username}"
        user_fields = "user.fields=id"
        url = f"https://api.twitter.com/2/users/by?{usernames}&{user_fields}"
        return url

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r

    def connect_to_endpoint(self, url):
        response = requests.request("GET", url, auth=self.bearer_oauth,)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def main(self):
        url = self.create_url()
        json_response = self.connect_to_endpoint(url)
        return json_response
