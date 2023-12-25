import jsonlines
import json


def get_hashtags(tweet):
    ret = []
    words = tweet.split()
    for word in words:
        if word[0] == '#':
            if len(word) >= 2:
                ret.append(word[1:])
    return ret


def extract_and_append():
    with open('data.jsonl', 'w') as out:
        with jsonlines.open('tweet_data_raw.jsonl') as inp:
            json_data = inp.iter()
            for line in json_data:
                if line['lang'] == 'en':
                    line['hashtags'] = get_hashtags(line['text'])
                    json.dump(line, out)
                    out.write("\n")

extract_and_append()
