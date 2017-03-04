# -*- coding: utf-8 -*-

dict={}
def readFile(fileName):
    file=open(fileName,'r')
    inputStr=file.readlines()
    file.close()
    for line in inputStr:
        dict[line]=line


def outFile(out_name):
    outfile=open(out_name,'a+')
    for key in sorted(dict.keys()):
        outfile.write(key)
    outfile.close()

if __name__ =="__main__":
	input_name = input("Please input your origin dict text file:")	
	readFile(input_name)
	outFile("out"+input_name)
