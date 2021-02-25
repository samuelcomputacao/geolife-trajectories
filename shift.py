import datetime
import sys
import multiprocessing
import time
import os
from shutil import copyfile


def convertStringToDate(dateStr, fmt='%Y%m%d%H%M%S'):
    return datetime.datetime.strptime(dateStr, fmt)


def convertDateToString(date, fmt='%Y%m%d%H%M%S'):
    return datetime.datetime.strftime(date, fmt)


def processDate(dateStr, dateFutureStr, baseDateStr):
    try:
        firstDate = convertStringToDate(dateStr)
        dateFuture = convertStringToDate(dateFutureStr)
        baseDate = convertStringToDate(baseDateStr)
        diferenceDate = dateFuture - firstDate
        baseDate = baseDate + datetime.timedelta(days=diferenceDate.days)
        baseDate = baseDate + datetime.timedelta(seconds=diferenceDate.seconds)
        return baseDate
    except Exception:
        pass


def isDate(string, fmt='%Y%m%d%H%M%S'):
    retorno = False
    try:
        convertStringToDate(string, fmt)
        retorno = True
    except Exception:
        pass
    return retorno


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
    semaphoreAnimated = None

    parameters = None
    semaphoreFolder = None

    PATH_PIPE = 'pipe'
    PATH_PIPE_ANIMATED = 'pipe_animated'
    processed_last = 0

    def __init__(self, files, parameters):
        self.files = files
        self.total = files.fileLenght
        self.processed = 0
        self.indexAnimated = 0
        self.semaphore = multiprocessing.Semaphore()
        self.semaphoreAnimated = multiprocessing.Semaphore()
        self.parameters = parameters
        self.threads = 0
        self.semaphoreFolder = multiprocessing.Semaphore()
        self.removePipe()

    def isTimeDelta(self):
        self.parameters.delta is None

    def shiftDate(self):
        self.setFinishAnimated(False)
        th = multiprocessing.Process(target=self.printProcessAnimated)
        th.start()
        for key in self.files.metaData.keys():
            listFiles = self.files.metaData[key]
            listFiles.sort()
            firstName = listFiles[0].getName()

            numCpu = multiprocessing.cpu_count()
            if self.parameters.pipeline < numCpu:
                numCpu = self.parameters.pipeline

            listPartitioned, titles = [], []
            if not self.isTimeDelta() and isDate(firstName, '%Y%m%d%H%M%S'):
                listPartitioned, titles = self.getPartition(listFiles, numCpu)
            else:
                listPartitioned, titles = self.getPartition(listFiles, numCpu)

            threads = []
            for i in range(0, numCpu):
                thread = multiprocessing.Process(target=self.processFiles,
                                                 args=(listPartitioned[i], firstName, titles[i]))
                thread.start()
                threads.append(thread)

            for i in range(0, numCpu):
                threads[i].join()

        self.setFinishAnimated(True)

    def removePipe(self):
        if os.path.exists(self.PATH_PIPE):
            os.remove(self.PATH_PIPE)
        if os.path.exists(self.PATH_PIPE_ANIMATED):
            os.remove(self.PATH_PIPE_ANIMATED)

    def getPartition(self, list, parts):
        list.sort()
        resultFiles = [[] for i in range(0, parts)]
        resultTitles = [[] for i in range(0, parts)]
        idx = 0
        lenght = len(list)
        count = 0
        lastTitle = None
        while idx < lenght:
            file = list[idx]
            if count >= parts:
                count = 0
            resultFiles[count].append(file)
            title = file.getName()
            if not self.isTimeDelta():
                if isDate(file.getName()):
                    if idx > 0:
                        fileReader = open(list[idx - 1].path, 'r')
                        size = sum([1 for line in fileReader]) - 6
                        fileReader.close()

                        lasteDate = convertStringToDate(lastTitle)
                        futureDate = lasteDate + datetime.timedelta(seconds=size * self.parameters.delta)
                        title = convertDateToString(futureDate)
                    else:
                        title = self.parameters.dtIni

            resultTitles[count].append(title)
            lastTitle = title
            count += 1
            idx += 1

        return resultFiles, resultTitles

    def processFiles(self, files, firstName, titles):
        for i in range(len(files)):
            self.processFile(files[i], firstName, titles[i])

    def processFile(self, file, firstName, title):
        if file.isFile:
            if "labels" == file.getName() and "txt" == file.getExtension():
                self.processFileTXT(file)
            elif "plt" == file.getExtension():
                self.processFilePLT(file, firstName, title)
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

    def processFilePLT(self, fileSource, firstDateStr, title):
        dtIni = processDate(firstDateStr, fileSource.getName(),
                            self.parameters.dtIni) if self.isTimeDelta() else convertStringToDate(title)
        file = open(fileSource.path, 'r')
        targetPath = fileSource.path.replace(self.parameters.sourcePath, self.parameters.targetPath).replace(
            fileSource.getName(), dtIni.strftime("%Y%m%d%H%M%S"))
        self.createFolder(targetPath)
        targetFile = open(targetPath, 'w')
        offset = 6
        cont = 0
        lastLineDateStr = None
        for line in file:
            lineStr = line.strip()
            if cont < offset:
                cont += 1
            else:
                line = line.strip().split(",")
                if self.isTimeDelta():
                    lineDateStr = (line[5] + line[6]).replace("-", "").replace(":", "")
                    lineDate = processDate(fileSource.getName(), lineDateStr, dtIni.strftime("%Y%m%d%H%M%S"))
                elif lastLineDateStr is None:
                    lineDate = convertStringToDate(title)
                else:
                    lastLineDate = convertStringToDate(lastLineDateStr)
                    lineDate = lastLineDate + datetime.timedelta(seconds=self.parameters.delta)
                lastLineDateStr = convertDateToString(lineDate)
                lineStr = f'{line[0]},{line[1]},{line[2]},{line[3]},{line[4]},{lineDate.strftime("%Y-%m-%d,%H:%M:%S")}'
            targetFile.write(lineStr + '\n')
        file.close()
        targetFile.close()
        self.incrementProcessed()

    def printProcessAnimated(self):
        idx = 1
        timeStart = datetime.datetime.now()
        while not self.getFinishAnimated():
            if idx > 5:
                idx = 1
            percent = self.getProcessed() * 100 / self.total
            timeEnd = datetime.datetime.now() - timeStart
            label = "Processing: %s %.2f%% Time: %s" % (("." * idx + (6 - idx) * " "), percent, str(timeEnd))
            sys.stdout.write("\r" + label)
            sys.stdout.flush()
            idx += 1
            time.sleep(0.4)

        timeEnd = datetime.datetime.now() - timeStart
        label = "Processing: ..... 100.00%% Time: %s" % str(timeEnd)
        sys.stdout.write("\r" + label)
        sys.stdout.flush()
        self.removePipe()

    def getProcessed(self):
        num = 0
        if os.path.exists(self.PATH_PIPE):
            file = open(self.PATH_PIPE, 'r')
            for line in file:
                num = int(line.strip())
            file.close()
        if num != self.processed_last and num != 0:
            self.processed_last = num
        return self.processed_last

    def incrementProcessed(self, value=1):
        self.semaphore.acquire()
        num = self.getProcessed()
        num += value
        file = open(self.PATH_PIPE, 'w')
        file.write(str(num))
        file.close()
        self.semaphore.release()

    def getFinishAnimated(self):
        self.semaphoreAnimated.acquire()
        num = 0
        if os.path.exists(self.PATH_PIPE_ANIMATED):
            file = open(self.PATH_PIPE_ANIMATED, 'r')
            for line in file:
                num = int(line.strip())
            file.close()
        self.semaphoreAnimated.release()
        return True if num > 0 else False

    def setFinishAnimated(self, value):
        self.semaphoreAnimated.acquire()
        num = 1 if value else 0
        file = open(self.PATH_PIPE_ANIMATED, 'w')
        file.write(str(num))
        file.close()
        self.semaphoreAnimated.release()
