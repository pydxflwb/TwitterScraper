# Author: Pengyu Xiao
# Version: v1.2 Mar 30 2021
# Description: Low-Level Abstract -- DataController
import os
import json
import copy
import pandas as pd
from abc import abstractmethod


class DataController(object):
    def __init__(self):
        self.col = self.getCol()
        self.data = pd.DataFrame(columns=self.col)
        self.start_line = 0
        self.split_count = 0
        self.storage_flag = False  # header去重
        self.split_thresh = 10000

    @abstractmethod
    def getCol(self):
        # To Override
        pass

    @abstractmethod
    def replaceText(self, fjson):
        # To Override
        pass

    @abstractmethod
    def fillNull(self, fjson):
        # To Override
        pass

    def inputData(self, fdata, path):
        # 1. 想办法去header重
        # storage_flag
        new_path = path.split(".")[0] + "_{}.csv".format(self.split_count)
        for fjson in fdata:
            fjson1 = self.fillNull(fjson)
            fjson1 = self.replaceText(fjson1)
            self.data.loc[self.start_line] = fjson1
            self.start_line += 1
            if self.start_line == self.split_thresh:
                if not self.storage_flag:
                    self.data.to_csv(new_path, mode="a+", index=True, header=True)
                else:
                    self.data.to_csv(new_path, mode="a+", index=True, header=False)
                self.split_count += 1
                new_path = path.split(".")[0] + "_{}.csv".format(self.split_count)
                self.start_line = 0
                self.storage_flag = False
                self.data.drop(index=self.data.index)
        if not self.storage_flag:
            self.data.to_csv(new_path, mode="a+", index=True, header=True)
            self.storage_flag = True
        else:
            self.data.to_csv(new_path, mode="a+", index=True, header=False)


class UserDataController(DataController):
    def getCol(self):
        col = ['id', 'username', 'created_at', 'description', 'protected', 'location',
               'profile_image_url', 'verified']
        return col

    def replaceText(self, fjson):
        fjson["description"] = fjson["description"].replace("\n", "\t").replace("\r", "\t")
        fjson["followers_count"] = fjson["public_metrics"]["followers_count"]
        fjson["following_count"] = fjson["public_metrics"]["following_count"]
        fjson["tweet_count"] = fjson["public_metrics"]["tweet_count"]
        fjson["listed_count"] = fjson["public_metrics"]["listed_count"]
        return fjson.pop("public_metrics")

    def fillNull(self, fjson):
        for akey in self.col:
            if akey not in fjson.keys():
                if akey == 'id':
                    fjson[akey] = 0
                elif akey == 'description':
                    fjson[akey] = ""
        return fjson


class TwitterDataController(DataController):
    def getCol(self):
        col = ['context_annotations', 'conversation_id', 'author_id', 'created_at', 'entities', 'geo', 'id',
               'in_reply_to_user_id', 'referenced_tweets', 'lang', 'text']
        return col

    def replaceText(self, fjson):
        fjson["text"] = fjson["text"].replace("\n", "\t").replace("\r", "\t")
        fjson["retweet_count"] = fjson["public_metrics"]["retweet_count"]
        fjson["reply_count"] = fjson["public_metrics"]["reply_count"]
        fjson["like_count"] = fjson["public_metrics"]["like_count"]
        fjson["quote_count"] = fjson["public_metrics"]["quote_count"]
        fjson.pop("public_metrics")
        return fjson

    def fillNull(self, fjson):
        for akey in self.col:
            if akey not in fjson.keys():
                if akey in ['id', 'conversation_id', 'in_reply_to_user_id', 'author_id']:
                    fjson[akey] = 0
                elif akey in ['context_annotations', 'entities', 'geo', 'referenced_tweets', 'lang', 'text']:
                    fjson[akey] = ""
        return fjson


class ConversationController(TwitterDataController):
    def getCol(self):
        col = ['id', 'text', 'author_id', 'created_at', 'entities', 'in_reply_to_user_id'
               ]
        return col

    def fillNull(self, fjson):
        for akey in self.col:
            if akey not in fjson.keys():
                if akey in ['id', 'conversation_id', 'in_reply_to_user_id']:
                    fjson[akey] = 0
                elif akey in ['context_annotations', 'entities', 'geo', 'referenced_tweets', 'lang', 'text']:
                    fjson[akey] = ""
        return fjson


class ExtTwitterDataController(TwitterDataController):
    # 该类功能稍显混乱，需要重构
    def __init__(self, ext_path):
        super().__init__()
        self.ext_path = ext_path

    def getCol(self, mode="TW"):
        if mode == "TW":
            col = ['context_annotations', 'conversation_id', 'author_id', 'created_at', 'entities', 'geo', 'id',
                   'in_reply_to_user_id', 'referenced_tweets', 'lang', 'text']
        elif mode == "CO":
            col = ['id', 'text', 'author_id', 'created_at', 'entities', 'in_reply_to_user_id']
        else:
            col = []
        return col

    def fillNill(self, fjson):
        for akey in self.col:
            if akey not in fjson.keys():
                print(akey)
                if akey in ['id', 'conversation_id', 'in_reply_to_user_id', 'author_id']:
                    fjson[akey] = 0
                elif akey in ['context_annotations', 'entities', 'geo', 'referenced_tweets', 'lang', 'text']:
                    fjson[akey] = "null"
        # print(fjson)
        return fjson

    def inputExtData(self, fdata, path):
        extend_req_list = []
        new_path = path.split(".")[0] + "_{}.csv".format(self.split_count)
        print(new_path)
        for fjson in fdata:
            fjson1 = self.fillNill(fjson)
            fjson1 = self.replaceText(fjson1)
            print(fjson1)
            if fjson["referenced_tweets"] and fjson["referenced_tweets"] != 'null':
                ref = fjson1["referenced_tweets"][0]
                if 'type' in ref.keys():
                    new_ref = copy.deepcopy(ref)  # Deep Copy
                    new_ref["source_id"] = fjson1['id']
                    extend_req_list.append(new_ref)
            self.data.loc[self.start_line] = fjson1
            self.start_line += 1
            if self.start_line == self.split_thresh:
                if not self.storage_flag:
                    self.data.to_csv(new_path, mode="a+", index=True, header=True)
                else:
                    self.data.to_csv(new_path, mode="a+", index=True, header=False)
                self.split_count += 1
                new_path = path.split(".")[0] + "_{}.csv".format(self.split_count)
                self.start_line = 0
                self.storage_flag = False
                self.data.drop(index=self.data.index)
        if not self.storage_flag:
            self.data.to_csv(new_path, mode="a+", index=True, header=True)
            self.storage_flag = True
        else:
            self.data.to_csv(new_path, mode="a+", index=True, header=False)

        if not os.path.exists(self.ext_path):
            f = open(self.ext_path, "w+")
            f.close()
        
        with open(self.ext_path, "r+") as ef:
            if os.path.getsize(self.ext_path) == 0:
                ext_data = {"data": extend_req_list}
            else:
                ext_data = json.load(ef)
                print(ext_data)
                ext_data["data"].extend(extend_req_list)
            ef.seek(0)
            json.dump(ext_data, ef)
        ef.close()

    def changeStorage(self):
        # self.storage_flag = False
        self.data.drop(index=self.data.index, inplace=True)
        self.data.reset_index()

    def inputData(self, fdata, path):
        for fjson in fdata:
            fjson1 = self.fillNull(fjson)
            fjson1 = self.replaceText(fjson1)
            self.data.loc[self.start_line] = fjson1
            self.start_line += 1
        self.data.to_csv(path, mode="w", index=True, header=True)

    def dataExtend(self, mode):
        self.col = self.getCol(mode)
        self.data = pd.DataFrame(columns=self.col)


