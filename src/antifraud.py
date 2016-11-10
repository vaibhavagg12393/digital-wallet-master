import time
from threading import Thread

fname = "/Users/Vaibhav/Documents/digital-wallet-master/batch_payment.csv"
sname = "/Users/Vaibhav/Documents/digital-wallet-master/stream_payment.csv"
output1 = "/Users/Vaibhav/Documents/digital-wallet-master/paymo_output/output1.txt"
output2 = "/Users/Vaibhav/Documents/digital-wallet-master/paymo_output/output2.txt"
output3 = "/Users/Vaibhav/Documents/digital-wallet-master/paymo_output/output3.txt"
outputGraph = "/Users/Vaibhav/Documents/digital-wallet-master/paymo_output/outputGraph.txt"

graph={}

def past_graph():
    with open(fname) as fp:
        for line in fp:
            try:
                id1= int(line.strip().split(",")[1].strip())
                id2 = int(line.strip().split(",")[2].strip())
                key = str(id1)
                value = str(id2)
                graph.setdefault(key, [])
                if value not in graph[key]:
                    graph[key].append(value)
                key = str(id2)
                value = str(id1)
                graph.setdefault(key, [])
                if value not in graph[key]:
                    graph[key].append(value)
            except ValueError:
                    continue


def feature1(id1,id2):
    if graph.get(id1)==None:
        return "unverified"
    elif id2 in graph[id1]:
        return "trusted"
    else:
        return "unverified"

def feature2(id1,id2,origin,count):
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
                if items!=origin and graph.get(items):
                    origin = id1
                    if count<=2:
                        out = feature2(items,id2,origin,count)
                        if out=="trusted":
                            return "trusted"
                    else:
                        continue
                else:
                    continue
            return "unverified"
    else:
        return "unverified"

def feature3(id1,id2,origin,count):
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
                if items!=origin and graph.get(items):
                    origin = id1
                    if count<4:
                        out = feature3(items,id2,origin,count)
                        if out=="trusted":
                            return "trusted"
                    else:
                        continue
                else:
                    continue
            return "unverified"
    else:
        return "unverified"

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
                outcome2 = feature2(str(id1),str(id2),origin,count)
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
                outcome3 = feature3(str(id1),str(id2),origin,count)
                print >> f3, outcome3
            except ValueError:
                continue
    f3.close()

if __name__ == '__main__':
    start_time = time.time()
    past_graph()
    print("Time to create graph --- %s seconds ---" % (time.time() - start_time))
    f4 = open(outputGraph, 'w')
    print >> f4, graph
    print "Result 1 start"
    start_time = time.time()
    #result1()
    print("Time Result 1 --- %s seconds ---" % (time.time() - start_time))
    print "Result 2 start"
    start_time = time.time()
    #result2()
    print("Time Result 2 --- %s seconds ---" % (time.time() - start_time))
    print "Result 3 start"
    start_time = time.time()
    result3()
    print("Time Result 3 --- %s seconds ---" % (time.time() - start_time))


