import os


class MetaDataFile:
    isFile = None
    path = None

    def __init__(self, isFile=False, path=""):
        self.isFile = isFile
        self.path = path

    def getExtension(self):
        array = self.path.split(".")
        result = ""
        if len(array) > 1:
            result = array[-1].strip()
        return result

    def getName(self):
        return os.path.basename(self.path).split(".")[0]

    def __str__(self):
        return f'Path: {self.path} isFile: {self.isFile}'

    def __lt__(self, other):
        return self.path < other.path
