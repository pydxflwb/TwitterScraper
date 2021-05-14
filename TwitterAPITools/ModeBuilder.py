# Author: Pengyu Xiao
# Version: v1.2 Mar 25 2021
# Description: Low-Level Abstract -- ModeBuilder

import os

# Mode Builder
#   * passes mode to Scraper
#   * automatically structures relevant string

# Mode
# following/followers/profile/timeline/tweet_search/conversation


class ModeBuilder(object):
    def __init__(self):
        self.basic_url = "https://api.twitter.com/2/"
        self.mode_map = {"FE": 1, "FI": 2,
                         "PR": 3, "TL": 4,
                         "TW": -1, "CO": -2,
                         "": 0}
        self.val = 0    # mode "" is default

    def getMode(self):
        # Just For Test
        return self.val

    def changeMode(self, mode):
        if mode in self.mode_map.keys():
            self.val = self.mode_map[mode]
        else:
            self.val = 0

    def buildUrl(self, arg):
        # Warning: Modes not in the map
        head = tail = ""
        if self.val > 0:
            head = "users/"
            if self.val == 1:       # FE
                tail = "/followers"
            if self.val == 2:       # FI
                tail = "/following"
            if self.val == 4:       # TL
                tail = "/tweets"
            if self.val == 3:       # PR
                head = head + "by?usernames="
        elif self.val < 0:
            head = "tweets"
            if self.val == -1:      # TW
                head = head + "?ids="
            if self.val == -2:      # CO
                head = head + "/search/recent?query=conversation_id:"
        return self.basic_url + head + str(arg) + tail

    def buildParams(self):
        params = {}
        if 1 <= self.val <= 3:      # FE FI PR
            params["user.fields"] = "id,username,created_at,description,public_metrics,protected,location," \
                                    "profile_image_url,verified"
            if self.val < 3:        # FE FI
                params["max_results"] = 1000

        elif self.val == 4 :         # TL
            params["tweet.fields"] = "context_annotations,author_id,conversation_id,created_at,entities,geo,id," \
                                     "in_reply_to_user_id,referenced_tweets,lang,public_metrics,text"
            params["max_results"] = 100
        elif self.val == -1:        # TW
            params["tweet.fields"] = "context_annotations,author_id,conversation_id,created_at,entities,geo,id," \
                                     "in_reply_to_user_id,referenced_tweets,lang,public_metrics,text"
        #     params["tweet.fields"] = "author_id,created_at,entities,public_metrics"
        #     params["expansions"] = "referenced_tweets.id"
        elif self.val == -2:        # CO
            params["tweet.fields"] = "author_id,created_at,entities,public_metrics,in_reply_to_user_id"
        return params

    def buildPath(self, arg, fdir, root):
        # Directory
        if self.val > 0 and self.val != 4:
            path = fdir + "/Users/{}".format(arg)
        elif self.val < 0 or self.val == 4:
            path = fdir + "/Tweets"
        else:
            return ""
        # Root User? Only For User
        if root and root == "root":
            path = path + "_root"
            if self.val == 3:       # PR
                path = path + "pr"
        if not os.path.exists(path):
            os.mkdir(path)

        # Mode
        if self.val == 1:      # FE
            path = path + "/id_{}_followers.csv".format(arg)
        elif self.val == 2:    # FI
            path = path + "/id_{}_following.csv".format(arg)
        elif self.val == 3:    # PR
            path = path + "/name_{}_profile.csv".format(arg)
        elif self.val == 4:    # TL
            path = path + "/uid_{}_timeline.csv".format(arg)
        elif self.val == -1:   # TW
            if not os.path.exists(path + '/ext'):
                os.mkdir(path + '/ext')
            path = path + "/ext/uid_{}_retweet.csv".format(arg)
        elif self.val == -2:   # CO
            if not os.path.exists(path + '/ext'):
                os.mkdir(path + '/ext')
            path = path + "/ext/uid_{}_conversation.csv".format(arg)
        else:
            return ""
        return path

    def setAllReady(self, arg, fdir, root):
        url = self.buildUrl(arg)
        params = self.buildParams()
        path = self.buildPath(arg, fdir, root)
        return url, params, path

    def setExtensionReady(self, uid, tid, fdir):
        url = self.buildUrl(tid)
        params = self.buildParams()
        path = self.buildPath(uid, fdir, "")
        return url, params, path
