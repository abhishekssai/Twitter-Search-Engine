import tweepy
import json
import config

query = "(job -is:retweet) OR (layoff -is:retweet) OR (tech layoff -is:retweet) OR (recession -is:retweet) " \
        "OR (hiring -is:retweet) " "OR (startups -is:retweet)"

tweet_collector = []


def append_to_file(tweets):
    with open('tweet_data_raw.jsonl', 'a') as outfile:
        for tweet in tweets:
            json.dump(tweet, outfile)
            outfile.write("\n")


class TwitterPrinterV2(tweepy.StreamingClient):
    __new_tweet__ = {}

    def on_connect(self):
        print("Connected to twitter stream")

    def on_tweet(self, tweet):
        self.__new_tweet__ = tweet.data

    def on_includes(self, includes):
        user_objects = includes['users']
        id_search_dict = {user.id: user.data for user in user_objects}
        self.__new_tweet__['user_data'] = id_search_dict[int(self.__new_tweet__['author_id'])]
        tweet_collector.append(self.__new_tweet__)
        if len(tweet_collector) >= 500:
            print("Pulled 500 more tweets ")
            append_to_file(tweet_collector)
            tweet_collector.clear()

        def on_disconnect(self):
            append_to_file(tweet_collector)
            print("Closing connection..")

        def on_errors(self, errors):
            print("Error encountered : ", errors)


stream = TwitterPrinterV2(config.BEARER_TOKEN)

stream.add_rules(tweepy.StreamRule(query))
stream.filter(expansions=['author_id', 'geo.place_id'],
              tweet_fields=['author_id', 'created_at', 'lang', 'geo'],
              user_fields=['profile_image_url', 'verified'],
              place_fields=['country', 'country_code'])
