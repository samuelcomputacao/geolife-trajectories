import os
from metaDataFile import MetaDataFile


class Files:
    fileLenght = None
    folderLenght = None
    metaData = None

    def __init__(self, parameter):
        self.fileLenght = 0
        self.folderLenght = 0
        self.metaData = {}
        self.mapFiles(parameter.sourcePath)

    def mapFiles(self, pathParent):
        dirs = os.listdir(pathParent);
        self.metaData[pathParent] = []
        for dir in dirs:
            path = f'{pathParent}/{dir}'
            metaData = MetaDataFile(os.path.isfile(path), path)
            if not metaData.isFile:
                self.mapFiles(metaData.path)
            else:
                self.fileLenght += 1
            self.metaData[pathParent].append(metaData)
