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


def removeFolder(path):
    files = os.listdir(path)
    for file in files:
        removeFile(f'{path}/{file}')


def removeFile(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            removeFolder(path)
            os.rmdir(path)

class Shift:
    total = None
    processed = None
    files = None

    finishAnimated = None
    semaphore = None
    semaphoreAnimated = None

    parameters = None
    semaphoreFolder = None
    semaphoreLabels = None

    PATH_PIPE = 'pipe'
    PATH_PIPE_ANIMATED = 'pipe_animated'
    PATH_PIPE_LABELS = 'tmp/'
    processed_last = 0

    mapLabels = {}

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
        self.semaphoreLabels = multiprocessing.Semaphore()
        self.removePipe()
        removeFile(self.parameters.targetPath)

    def isTimeDelta(self):
        return self.parameters.delta is not None

    def shiftDate(self):
        self.setFinishAnimated(False)
        th = multiprocessing.Process(target=self.printProcessAnimated)
        th.start()
        numCpu = multiprocessing.cpu_count()
        if self.parameters.pipeline < numCpu:
            numCpu = self.parameters.pipeline
        threads = []
        for key in self.files.metaData.keys():
            listFiles = self.files.metaData[key]
            listFiles.sort()
            firstName = listFiles[0].getName()

            listPartitioned, titles = [], []

            if self.isTimeDelta() and isDate(firstName, '%Y%m%d%H%M%S'):
                listPartitioned, titles = self.getPartition(listFiles, numCpu)
            else:
                listPartitioned, titles = self.getPartition(listFiles, numCpu)

            for i in range(0, numCpu):
                thread = multiprocessing.Process(target=self.processFiles,
                                                 args=(listPartitioned[i], firstName, titles[i],))
                thread.start()
                threads.append(thread)

            for th in threads:
                th.join()
            threads.clear()

        # Processando arquivo Labels
        threads.clear()
        labels = []
        for key in self.files.metaData.keys():
            listFiles = self.files.metaData[key]
            for file in listFiles:
                if file.getName() == 'labels':
                    labels.append(file)

        listPartitionedLabels = self.getPartitionLabels(labels, numCpu)
        for i in range(0, numCpu):
            thread = multiprocessing.Process(target=self.processFilesLabels,
                                             args=(listPartitionedLabels[i],))
            thread.start()
            threads.append(thread)

        for thr in threads:
            thr.join()
        self.setFinishAnimated(True)

    def processFilesLabels(self, files):
        for file in files:
            self.processFileLabel(file)

    def getPartitionLabels(self, files, parts):
        cont = 0
        idx = 0
        lenght = len(files)
        result = [[] for i in range(parts)]
        while idx < lenght:
            file = files[idx]
            if cont >= parts:
                cont = 0
            if file.getName() == 'labels':
                result[cont].append(file)
                cont += 1
            idx += 1
        return result

    def removePipe(self):
        removeFile(self.PATH_PIPE)
        removeFile(self.PATH_PIPE_ANIMATED)
        removeFile(self.PATH_PIPE_LABELS)

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
            if self.isTimeDelta():
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
                pass
            elif "plt" == file.getExtension():
                self.processFilePLT(file, firstName, title)
            else:
                self.processFileDefault(file)

    def processFileDefault(self, file):
        targetPath = file.path.replace(self.parameters.sourcePath, self.parameters.targetPath)
        self.createFolder(targetPath)
        copyfile(file.path, targetPath)
        self.incrementProcessed()

    def processFileLabel(self, file):
        id = file.path.split('/')[-2]
        mapLabels = self.getKeyMapLabels(id)
        sourceFile = open(file.path, 'r')
        targetPath = file.path.replace(self.parameters.sourcePath, self.parameters.targetPath)
        self.createFolder(targetPath)
        targetFile = open(targetPath, 'w')
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
                if not dateStartStr in mapLabels.keys():
                    keys = list(mapLabels.keys())
                    keys.append(dateStartStr)
                    keys.sort()
                    index = keys.index(dateStartStr)
                    if index > 0:
                        dateStartStr = keys[index - 1]
                    else:
                        dateStartStr = keys[index + 1]

                if not dateFinishStr in mapLabels.keys():
                    keys = list(mapLabels.keys())
                    keys.append(dateFinishStr)
                    keys.sort()
                    index = keys.index(dateFinishStr)
                    if index > 0:
                        dateFinishStr = keys[index - 1]
                    else:
                        dateFinishStr = keys[index + 1]
                dateStart = convertStringToDate(mapLabels[dateStartStr])
                dateFinish = convertStringToDate(mapLabels[dateFinishStr])
                mode = line[2].strip()
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
                            self.parameters.dtIni) if not self.isTimeDelta() else convertStringToDate(title)
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
                lineStr = lineStr.split(",")
                lastDate = (lineStr[5] + lineStr[6]).replace("-", "").replace(":", "")
                if not self.isTimeDelta():
                    lineDateStr = lastDate
                    lineDate = processDate(fileSource.getName(), lineDateStr, dtIni.strftime("%Y%m%d%H%M%S"))
                elif lastLineDateStr is None:
                    lineDate = convertStringToDate(title)
                else:
                    lastLineDate = convertStringToDate(lastLineDateStr)
                    lineDate = lastLineDate + datetime.timedelta(seconds=self.parameters.delta)
                lastLineDateStr = convertDateToString(lineDate)
                lineStr = f'{lineStr[0]},{lineStr[1]},{lineStr[2]},{lineStr[3]},{lineStr[4]},{lineDate.strftime("%Y-%m-%d,%H:%M:%S")}'
                key = fileSource.path.split('/')[-3]
                self.addKeyMapLabels(key, lastDate, convertDateToString(lineDate, "%Y%m%d%H%M%S"))
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
            if percent > 100:
                percent = 100
            timeEnd = datetime.datetime.now() - timeStart
            label = "Processing: %s %.2f%% Time: %s" % (("." * idx + (6 - idx) * " "), percent, str(timeEnd))
            sys.stdout.write("\r" + label)
            sys.stdout.flush()
            idx += 1
            time.sleep(0.5)

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

    def addKeyMapLabels(self, key, lastDate, newDate):
        self.semaphoreLabels.acquire()
        if not os.path.exists(self.PATH_PIPE_LABELS):
            os.makedirs(self.PATH_PIPE_LABELS)
        path = self.PATH_PIPE_LABELS + key
        file = open(path, "a" if os.path.exists(path) else "w")
        file.write(f'{lastDate}->{newDate}\n')
        file.close()
        self.semaphoreLabels.release()

    def getKeyMapLabels(self, path):
        result = {}
        path = self.PATH_PIPE_LABELS + path
        if os.path.exists(path):
            file = open(path, 'r')
            for line in file:
                lineStr = line.split('->')
                result[lineStr[0].strip()] = lineStr[1].strip()
            file.close()
        return result
