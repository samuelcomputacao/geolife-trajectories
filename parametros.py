class Parametros:
    dtIni = None
    sourcePath = None
    targetPath = None
    pipeline = None

    DT_INI = "-dtIni"
    SOURCE = "-source"
    TARGET = "-target"

    def __init__(self, args):
        if self.DT_INI in args and self.TARGET in args and self.SOURCE in args:
            mapValues = self.createMapArgs(args)
            self.dtIni = mapValues[self.DT_INI]
            self.sourcePath = mapValues[self.SOURCE]
            self.targetPath = mapValues[self.TARGET]
        elif not (self.DT_INI in args):
            self.exception(f'Parâmetro {self.DT_INI} necessario!')
        elif not (self.TARGET in args):
            self.exception(f'Parâmetro {self.TARGET} necessario!')
        elif (not (self.SOURCE in args)):
            self.exception(f'Parâmetro {self.SOURCE} necessario!')

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
