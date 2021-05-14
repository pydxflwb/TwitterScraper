import requests
import time
import os
import pandas as pd
from TwitterAPITools.ModeBuilder import ModeBuilder
from APIScraper import APIScraper
from TwitterAPITools.DataController import DataController, TwitterDataController, UserDataController


class ExtTimelineScraper(APIScraper):
    def workFlow(self, arg, fdir, ofile, token=""):
        pass