import datetime
import sys
import threading
import multiprocessing
import time
import os
from shutil import copyfile


def processDate(dateStr, dateFutureStr, baseDateStr):
    firstDate = datetime.datetime.strptime(dateStr, '%Y%m%d%H%M%S')
    dateFuture = datetime.datetime.strptime(dateFutureStr, '%Y%m%d%H%M%S')
    baseDate = datetime.datetime.strptime(baseDateStr[0:8] + dateStr[-6:], '%Y%m%d%H%M%S')
    diferenceDate = dateFuture - firstDate
    baseDate = baseDate + datetime.timedelta(days=diferenceDate.days)
    baseDate = baseDate + datetime.timedelta(seconds=diferenceDate.seconds)
    return baseDate


def getExtensionList(file):
    return file.getExtension()


def getDateFutureLabels(filePath):
    dates = []
    offset = 1
    cont = 0
    file = open(filePath, 'r')
    for line in file:
        if cont < offset:
            cont += 1
        else:
            line = line.split('\t')[0].strip()
            line = line.replace("/", "").replace(":", "").replace(" ", "")
            dates.append(line)
    dates.sort()
    file.close()
    return dates[0]


def getDateFuturePLT(path):
    file = open(path, 'r')
    offset = 6
    cont = 0
    dates = []
    for line in file:
        if cont < offset:
            cont += 1
        else:
            line = line.split(",")[5:]
            line = line[0].strip() + line[1].strip()
            line = line.replace("-", "").replace(":", "")
            dates.append(line)
    dates.sort()
    file.close()
    return dates[0]


class Shift:
    total = None
    processed = None
    files = None

    finishAnimated = None
    semaphore = None

    parameters = None
    semaphoreFolder = None

    PATH_PIPE = 'pipe'

    def __init__(self, files, parameters):
        self.files = files
        self.total = files.fileLenght
        self.processed = 0
        self.indexAnimated = 0
        self.semaphore = multiprocessing.Semaphore()
        self.parameters = parameters
        self.threads = 0
        self.semaphoreFolder = multiprocessing.Semaphore()
        self.removePipe()

    def shiftDate(self):
        self.finishAnimated = False
        th = multiprocessing.Process(target=self.printProcessAnimated)
        th.start()
        for key in self.files.metaData.keys():
            listFiles = self.files.metaData[key]
            listFiles.sort()
            firstName = listFiles[0].getName()

            numCpu = multiprocessing.cpu_count()
            listPartitioned = self.getPartition(listFiles, numCpu)
            threads = []
            for i in range(0, numCpu):
                thread = multiprocessing.Process(target=self.processFiles, args=(listPartitioned[i], firstName))
                thread.start()
                threads.append(thread)

            for i in range(0, numCpu):
                threads[i].join()

        self.finishAnimated = True
        self.removePipe()

    def removePipe(self):
        if os.path.exists(self.PATH_PIPE):
            os.remove(self.PATH_PIPE)

    def getPartition(self, list, parts):
        result = [[] for i in range(0, parts)]
        idx = 0
        lenght = len(list)
        count = 0
        while idx < lenght:
            if count >= parts:
                count = 0
            result[count].append(list[idx])
            count += 1
            idx += 1
        return result

    def processFiles(self, files, firstName):
        for file in files:
            self.processFile(file, firstName)

    def processFile(self, file, firstName):
        if file.isFile:
            if "labels" == file.getName() and "txt" == file.getExtension():
                self.processFileTXT(file)
            elif "plt" == file.getExtension():
                self.processFilePLT(file, firstName)
            else:
                self.processFileDefault(file)

    def processFileDefault(self, file):
        targetPath = file.path.replace(self.parameters.sourcePath, self.parameters.targetPath)
        self.createFolder(targetPath)
        copyfile(file.path, targetPath)
        self.incrementProcessed()

    def processFileTXT(self, file):
        sourceFile = open(file.path, 'r')
        targetPath = file.path.replace(self.parameters.sourcePath, self.parameters.targetPath)
        self.createFolder(targetPath)
        targetFile = open(targetPath, 'w')
        date = getDateFutureLabels(file.path)
        offset = 1
        cont = 0
        for line in sourceFile:
            lineStr = line.strip()
            if cont < offset:
                cont += 1
            else:
                line = line.split("\t")
                dateStartStr = line[0].strip().replace("/", "").replace(" ", "").replace(":", "")
                dateFinishStr = line[1].strip().replace("/", "").replace(" ", "").replace(":", "")
                mode = line[2].strip()
                dateStart = processDate(date, dateStartStr, self.parameters.dtIni)
                dateFinish = processDate(date, dateFinishStr, self.parameters.dtIni)
                lineStr = f'{dateStart.strftime("%Y/%m/%d %H:%M:%S")}\t{dateFinish.strftime("%Y/%m/%d %H:%M:%S")}\t{mode}'
            targetFile.write(lineStr + '\n')
        targetFile.close()
        sourceFile.close()
        self.incrementProcessed()

    def createFolder(self, targetPath):
        targetFolder = targetPath.replace(targetPath.split("/")[-1], "")
        self.semaphoreFolder.acquire()
        if not os.path.exists(targetFolder):
            os.makedirs(targetFolder)
        self.semaphoreFolder.release()

    def processFilePLT(self, fileSource, firstDateStr):
        dtIni = processDate(firstDateStr, fileSource.getName(), self.parameters.dtIni)
        file = open(fileSource.path, 'r')
        targetPath = fileSource.path.replace(self.parameters.sourcePath, self.parameters.targetPath).replace(
            fileSource.getName(), dtIni.strftime("%Y%m%d%H%M%S"))
        self.createFolder(targetPath)
        targetFile = open(targetPath, 'w')
        offset = 6
        cont = 0
        for line in file:
            lineStr = line.strip()
            if cont < offset:
                cont += 1
            else:
                line = line.strip().split(",")
                lineDateStr = (line[5] + line[6]).replace("-", "").replace(":", "")
                lineDate = processDate(fileSource.getName(), lineDateStr, dtIni.strftime("%Y%m%d%H%M%S"))
                lineStr = f'{line[0]},{line[1]},{line[2]},{line[3]},{line[4]},{lineDate.strftime("%Y-%m-%d,%H:%M:%S")}'
            targetFile.write(lineStr + '\n')
        file.close()
        targetFile.close()
        self.incrementProcessed()

    def printProcessAnimated(self):
        idx = 1
        timeStart = datetime.datetime.now()
        while not self.finishAnimated:
            if idx > 5:
                idx = 1
            percent = self.getProcessed() * 100 / self.total
            timeEnd = datetime.datetime.now() - timeStart
            label = "Processing: %s %.2f%% Time: %s" % (("." * idx + (6 - idx) * " "), percent, str(timeEnd))
            sys.stdout.write("\r" + label)
            sys.stdout.flush()
            idx += 1
            time.sleep(1)

        timeEnd = datetime.datetime.now() - timeStart
        label = "Processing: ..... 100.00%% Time: %f" % timeEnd
        sys.stdout.write("\r" + label)
        sys.stdout.flush()

    def getProcessed(self):
        num = 0
        if os.path.exists(self.PATH_PIPE):
            file = open(self.PATH_PIPE, 'r')
            for line in file:
                num = int(line.strip())
            file.close()
        return num

    def incrementProcessed(self, value=1):
        self.semaphore.acquire()
        num = self.getProcessed()
        num += value
        file = open(self.PATH_PIPE, 'w')
        file.write(str(num))
        file.close()
        self.semaphore.release()