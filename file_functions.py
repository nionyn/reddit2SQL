import json
from os import listdir
from os import rename

def getdir():
    return listdir("downloaded/all")

def getfile(file):
    with open("downloaded/all/"+file) as json_file:
        return json.load(json_file)

def output_json(input, filename):
    with open(filename, "w") as output:
        json.dump(input, output, indent=4)
    output.close()

def movefile(file):
    rename("downloaded/all/" + file , "processed/"+file)

def errorfile(file):
    rename("downloaded/all/" + file , "error/"+file)
