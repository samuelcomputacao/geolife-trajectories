import datetime

class Parametros:
    dtIni = None
    sourcePath = None
    targetPath = None
    pipeline = None

    DT_INI = "-dtIni"
    SOURCE = "-source"
    TARGET = "-target"
    PIPELINE = "-pipeline"

    def __init__(self, args):
        if self.DT_INI in args or self.TARGET in args or self.SOURCE in args or self.PIPELINE in args:
            mapValues = self.createMapArgs(args)
            if self.DT_INI in args:
                self.dtIni = mapValues[self.DT_INI]
            else:
                self.dtIni = (datetime.datetime.now()).strftime("%Y%m%d")

            if self.PIPELINE in args:
                self.pipeline = int(mapValues[self.PIPELINE])
            else:
                self.pipeline = 1

            if self.SOURCE in args:
                self.sourcePath = mapValues[self.SOURCE]
            else:
                self.exception(f'Parâmetro {self.SOURCE} necessario!')

            if self.TARGET in args:
                self.targetPath = mapValues[self.TARGET]
            else:
                self.targetPath = "shift-geolife-tragetories"
        else:
            self.exception(f'Parâmetros necessarios!')

    def createMapArgs(self, args):
        lenghtArgs = len(args)
        result = {}
        if lenghtArgs % 2 == 0:
            self.exception("Erro nos parâmetros")
        else:
            for i in range(1, lenghtArgs, 2):
                key = args[i]
                value = args[i + 1]
                result[key] = value
        return result

    def exception(self, msg):
        raise Exception(msg)
