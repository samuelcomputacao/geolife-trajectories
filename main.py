import sys, datetime
from parametros import Parametros
from files import Files
from shift import Shift


def init():
    args = sys.argv
    parameter = Parametros(args)
    files = Files(parameter)
    shift = Shift(files, parameter)
    shift.shiftDate()


if __name__ == '__main__':
    init()
