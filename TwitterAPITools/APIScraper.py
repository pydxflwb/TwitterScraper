# Author: Pengyu Xiao
# Version: v1.2 Mar 25 2021
# Description: Low-Level Abstract -- APIScraper
import requests
import time
import os
import json
import pandas as pd
from TwitterAPITools.ModeBuilder import ModeBuilder
from TwitterAPITools.DataController import DataController, TwitterDataController, UserDataController, \
    ExtTwitterDataController


class APIScraper(object):
    # Use OAuth2 Bearer Token
    def __init__(self, btoken: str):
        self.headers = {"Authorization": "Bearer {}".format(btoken)}
        self.builder = ModeBuilder()  # Val = 0

    ################################################################
    # Basic Methods
    def getMode(self):
        print(self.builder.getMode())

    def changeMode(self, mode: str):
        self.builder.changeMode(mode)

    def setAllReady(self, arg, fdir: str, root: str):
        return self.builder.setAllReady(arg, fdir, root)

    ################################################################
    # Base Work Methods
    def connectToEndpoint(self, url: str, params: dict):
        response = requests.request("GET", url, headers=self.headers, params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def singleReq(self, url, params, path, token: str, controller: DataController) -> (int, str):
        # Connect and Request Once
        if token:
            params["pagination_token"] = token
        # ToDo(PengyuXiao): Restore Token Into File?

        print("SingleReq : Connecting...")
        res = self.connectToEndpoint(url, params)
        # print(res)

        # Check Response
        if 'errors' in res.keys():
            return 0, ""
        if 'meta' in res.keys():
            fmeta = res['meta']
        else:
            fmeta = {}
        if 'data' in res.keys():
            fdata = res['data']
            print("Input")
            controller.inputData(fdata, path)
            print("Input End")
        if 'next_token' in fmeta:
            return 1, fmeta["next_token"]
        else:
            return 0, ""

    def workFlow(self, arg, fdir, ofile, token=""):
        flag = 1
        url, params, path = self.setAllReady(arg, fdir, root="")
        controller = DataController()
        cnt = 0

        while flag:
            outfile = open(ofile, "a", encoding='utf-8')
            flag, token = self.singleReq(url, params, path, token, controller)
            cnt += 1
            print("* Finish * ID: {} * Count: {} *".format(arg, cnt), file=outfile)
            outfile.close()
            if flag:
                time.sleep(60)
        outfile = open(ofile, "a", encoding='utf-8')
        print("### Finish ### id: {}".format(arg), file=outfile)
        outfile.close()


################################################################


class TimelineScraper(APIScraper):
    def __init__(self, btoken):
        super().__init__(btoken)
        self.changeMode("TL")
        self.getMode()

    def workFlow(self, arg, fdir, ofile, token=""):
        flag = 1
        url, params, path = self.setAllReady(arg, fdir, root="")
        if os.path.exists(path):
            flag = 0
        # controller = TwitterDataController()
        controller = TwitterDataController()
        cnt = 0
        print("Start")
        while flag:
            outfile = open(ofile, "a", encoding='utf-8')
            flag, token = self.singleReq(url, params, path, token, controller)
            cnt += 1
            # Twitter Threshold is 100
            print("* Finish * ID: {} * Count: {} *".format(arg, cnt), file=outfile)
            outfile.close()
            if flag:
                time.sleep(60)
            if cnt > 0:
                flag = 0
        outfile = open(ofile, "a", encoding='utf-8')
        print("### Finish ### id: {}".format(arg), file=outfile)
        outfile.close()

    def workFromUserDir(self, fdir, ofile):
        id_list = os.listdir(fdir + "/Users")
        for aid in id_list:
            if len(aid.split("_")) == 1:
                uid = int(aid)
            elif aid.split("_")[1] != "rootpro":
                uid = int(aid.split("_")[0])
            else:
                continue
            print("UID:{}".format(uid))
            self.workFlow(uid, fdir, ofile)

    def workWithUIDFile(self, uidfile, fdir, ofile):
        with open(uidfile, "r") as f:
            uid_list = f.readlines()
        f.close()
        checkdir = fdir + '/Tweets'
        checklist = os.listdir(checkdir)
        oldlist = []
        for cfile in checklist:
            if len(cfile.split("_")) == 4:
                oldlist.append(int(cfile.split("_")[1]))
        print(oldlist)

        count = 0
        for uuid in uid_list:
            uid = int(uuid)
            if uid not in oldlist:
                print("UID:{}".format(uid))
                self.workFlow(uid, fdir, ofile)
                count += 1
                outfile = open(ofile, "a", encoding='utf-8')
                print("### Total: {}".format(count), file=outfile)
                outfile.close()
                time.sleep(60)
        print("### End ###")

    # def workInADir(self, dirname, checkTweetList):
    #     follist = os.listdir(dirname)
    #     for fol in follist:
    #
    #
    # def workFromTwoStage(self, fdir, ofile):



################################################################


class UserFolScraper(APIScraper):
    def __init__(self, btoken, mode):
        super().__init__(btoken)
        self.changeMode(mode)
        self.getMode()

    def workFlow(self, arg, fdir, ofile, token=""):
        flag = 1
        url, params, path = self.setAllReady(arg, fdir, root="")
        if os.path.exists(path):
            flag = 0
        controller = UserDataController()
        cnt = 0
        print("Start")
        while flag:
            outfile = open(ofile, "a", encoding="utf-8")
            flag, token = self.singleReq(url, params, path, token, controller)
            cnt += 1
            print("* Finish * ID: {} * Count: {} *".format(arg, cnt), file=outfile)
            outfile.close()
            if flag:
                time.sleep(60)
        outfile = open(ofile, "a", encoding='utf-8')
        print("### Finish ### id: {}".format(arg), file=outfile)
        outfile.close()

    def workFrom1StageUser(self, fdir, ofile):
        id_list = os.listdir(fdir + "/Users")


class ExtendTweetScraper(APIScraper):
    def __init__(self, btoken):
        super().__init__(btoken)
        self.changeMode("TW")
        self.getMode()

    def setExtensionReady(self, uid, tid, fdir):
        return self.builder.setExtensionReady(uid, tid, fdir)

    def workWithTimeLineFile(self, tl_file, uid, fdir, retw_controller, conv_controller):
        # Deal With Retweet and Conversation
        ext_fdata = pd.read_csv(tl_file, delimiter=",", index_col=0)
        retw_controller.changeStorage()
        conv_controller.changeStorage()
        for index, row in ext_fdata.iterrows():
            # Retweet, Quote and Reply
            if row["referenced_tweet"] and pd.notna(row["referenced_tweets"]):
                ref_detail = json.loads(row["referenced_tweets"].replace("\'", "\""))[0]
                if ref_detail['type'] == 'retweeted' or ref_detail['type'] == 'quoted':
                    # Retweet
                    self.changeMode("TW")
                    self.getMode()
                    tid = int(ref_detail['id'])
                    url, params, path = self.setExtensionReady(uid, tid, fdir)
                    self.singleReq(url, params, path, token="", controller=retw_controller)
                    time.sleep(3)
                elif ref_detail['type'] == 'replied':
                    # Conversation
                    self.changeMode("CO")
                    self.getMode()
                    tid = row["conversation_id"]
                    url, params, path = self.setExtensionReady(uid, tid, fdir)
                    self.singleReq(url, params, path, token="", controller=conv_controller)
                    time.sleep(3)
                else:
                    pass

    def workInDir(self, tldir, fdir):
        retw_ctl = ExtTwitterDataController("")
        retw_ctl.dataExtend("TW")
        conv_ctl = ExtTwitterDataController("")
        conv_ctl.dataExtend("CO")
        tllist = os.listdir(tldir)
        for tfile in tllist:
            if len(tfile.split(".")) > 1:
                tpath = tldir + '/' + tfile
                print("{} Begin".format(tpath))
                uid = int(tfile.split("_")[1])
                self.workWithTimeLineFile(tpath, uid, fdir, retw_ctl, conv_ctl)


