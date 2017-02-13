# -*- coding: utf-8 -*-

dict={}
def readFile(fileName):
    file=open(fileName,'r')
    inputStr=file.readlines()
    file.close()
    for line in inputStr:
        dict[line]=line


def outFile():
    outfile=open('Dict.txt','a+')
    for key in sorted(dict.keys()):
        outfile.write(key)
    outfile.close()

readFile('origDict.txt')
readFile('ChinaDaily.txt')
readFile('hanzi.txt')
outFile()