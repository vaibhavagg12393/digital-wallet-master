import time
from threading import Thread
import os

fname = os.path.realpath("./digital-wallet-master/paymo_input/batch_payment.csv")
sname = os.path.realpath("./digital-wallet-master/paymo_input/stream_payment.csv")
output1 = os.path.realpath("./digital-wallet-master/paymo_output/output1.txt")
output2 = os.path.realpath("./digital-wallet-master/paymo_output/output2.txt")
output3 = os.path.realpath("./digital-wallet-master/paymo_output/output3.txt")
outputGraph = os.path.realpath("./digital-wallet-master/paymo_output/outputGraph.txt")

graph={}

# Create a graph to store sender with their receivers and vice-versa
# used a Set because Sets are significantly faster when it comes to determining if an object is
# present in the set (as in x in s)

def past_graph():
    with open(fname) as fp:
        for line in fp:
            try:
                id1= int(line.strip().split(",")[1].strip())
                id2 = int(line.strip().split(",")[2].strip())
                key = str(id1)
                value = str(id2)
                graph.setdefault(key, set())
                if value not in graph[key]:
                    graph[key].add(value)
                key = str(id2)
                value = str(id1)
                graph.setdefault(key, set())
                if value not in graph[key]:
                    graph[key].add(value)
            except ValueError:
                    continue

# Implementing the first Feature (Direct transfer)

def feature1(id1,id2):
    if graph.get(id1)==None:
        return "unverified"
    elif id2 in graph[id1]:
        return "trusted"
    else:
        return "unverified"

#Implementing second feature (Upto 1 mututal friend)

def feature2(id1,id2,origin,count,parents):
    if graph.get(id1):
        if id2 in graph[id1]:
            count = count+1
            if count<=2:
                return "trusted"
            elif count>2:
                return "unverified"
        else:
            count = count + 1
            for items in graph[id1]:
                if items not in parents and graph.get(items) and items!=origin:
                    origin = id1
                    parents.add(id1)
                    if count<=2:
                        out = feature2(items,id2,origin,count,parents)
                        if out=="trusted":
                            return "trusted"
                    else:
                        continue
                else:
                    continue
            return "unverified"
    else:
        return "unverified"

#   Implementing third feature (Upto 3 mutual friends)
# Example: If A and E have 3 mutual friends, say B,C, and D, then A can trasfer money to E or vice-versa.

def feature3(id1,id2,origin,count,parents):
    if graph.get(id1):
        if id2 in graph[id1]:
            count = count+1
            if count<=4:
                return "trusted"
            elif count>4:
                return "unverified"
        else:
            count = count + 1
            for items in graph[id1]:
                if items!=origin and items not in parents and graph.get(items):
                    origin = id1
                    parents.add(id1)
                    if count<4:
                        out = feature3(items,id2,origin,count,parents)
                        if out=="trusted":
                            return "trusted"
                    else:
                        continue
                else:
                    continue
            return "unverified"
    else:
        return "unverified"

# I created 3 different functions (result1, result2, result3) for reading inputs from stream_payments.csv file
# The reason was I was trying to implement multi-threading so that I could get output1, output2 and output3 simultaneously.
# But it was still taking a lot of time, hence I removed multi-threading. The functions are called serially now.

def result1():
    f1 = open(output1, 'w')
    with open(sname) as sp:
        for line in sp:
            try:
                id1=int(line.strip().split(",")[1].strip())
                id2=int(line.strip().split(",")[2].strip())
                outcome1 = feature1(str(id1),str(id2))
                print >> f1, outcome1
            except ValueError:
                continue
    f1.close()

def result2():
    f2 = open(output2, 'w')
    with open(sname) as sp:
        for line in sp:
            try:
                id1=int(line.strip().split(",")[1].strip())
                id2=int(line.strip().split(",")[2].strip())
                origin = str(id1)
                count = 0
                parents=set()
                parents.add(str(id1))
                outcome2 = feature2(str(id1),str(id2),origin,count,parents)
                print >> f2, outcome2
            except ValueError:
                continue
    f2.close()

def result3():
    f3 = open(output3, 'w')
    with open(sname) as sp:
        for line in sp:
            try:
                id1=int(line.strip().split(",")[1].strip())
                id2=int(line.strip().split(",")[2].strip())
                origin = str(id1)
                count = 0
                parents=set()
                parents.add(str(id1))
                outcome3 = feature3(str(id1),str(id2),origin,count,parents)
                print >> f3, outcome3
            except ValueError:
                continue
    f3.close()

# The main function first creates a graph using the data from batch_payments
# Once a graph is created, 3 result functions are called to print 3 different types of outputs (output1, output2, output3)

if __name__ == '__main__':
    past_graph()
    #f4 = open(outputGraph, 'w')
    #print >> f4, graph
    result1()
    result2()
    result3()


