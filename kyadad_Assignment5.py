import json, os, time
from datetime import datetime

'''
Query 1:
SELECT pitime,temp_5, temp_8 FROM print_reading WHERE pitime > 2018-05-06 16:00:00 AND pitime < 2018-05-06 18:00:00;

Query 2:
SELECT AVG(temp_8) FROM print_reading WHERE pitime > 2018-05-05 16:00:00 AND pitime < 2018-05-07 16:00:00;

Results:
Total time taken for both the Queries before partition: 2.5 (Avg)
Total time taken for both the Queries after partition: 0.2 (Avg)
'''

#calculating average
def averageOfList(numOfList):
    avg = sum(numOfList) / len(numOfList)
    return avg

#Upper and lower bound dates for query 1
lowerdt1 = datetime.strptime("2018-05-06 16:00:00", '%Y-%m-%d %H:%M:%S')
upperdt1 = datetime.strptime("2018-05-06 18:00:00", '%Y-%m-%d %H:%M:%S')

#Upper and lower bound dates for query 2
lowerdt2 = datetime.strptime("2018-05-05 16:00:00", '%Y-%m-%d %H:%M:%S')
upperdt2 = datetime.strptime("2018-05-07 16:00:00", '%Y-%m-%d %H:%M:%S')


#Before partitioning the data

path = 'datalog.txt/'
tt=0
Query_1 = []
Query_2 = [0]

#check for correct directory
if os.path.isdir(path):
  for file in os.listdir(path):
    start = time.time()
    #check for correct .txt datalog file
    if "datalog.txt" in file:
      f = open(path+file, 'r')
      for line in f:
        try:
          datalog = json.loads(line)
        except:
          continue
        #check if type exists in file
        if "type" in datalog.keys():
          #check whether tpye is print_reading, as temp information are in these type lines only
          if datalog["type"] == "print_reading":
            if "pitime" in datalog.keys() and 'temp_8' in datalog.keys():
              #formatting date
              pidate = datetime.strptime(datalog["pitime"].split("T")[0]+ " " + str(datalog["pitime"].split("T")[1].split(".")[0]), '%Y-%m-%d %H:%M:%S')
              #finding results for 1st query
              if 'temp_5' in datalog.keys():
                if pidate >= lowerdt1 and pidate <= upperdt1:
                  result = {}
                  result['pitime'] = datalog["pitime"]
                  result['temp_5'] = datalog['temp_5']
                  result['temp_8'] = datalog['temp_8']
                  Query_1.append(result)
              #finding results for 2nd query
              if pidate >= lowerdt2 and pidate <= upperdt2:
                Query_2.append(float(datalog["temp_8"]))
      f.close()
    tt += time.time() - start

#print("Result for Query #1: ", json.dumps(Query_1, indent=1))             
#print("Result for Query #2: Average Temperature (temp_8): ", round(averageOfList(Query_2), 2))
print("Total time taken for both the Queries before partition:", tt)


#Partitioning the folder
part_name = 'datalog_parts'
filelist = [ f for f in os.listdir(part_name) if f.endswith(".txt") ]
for f in filelist:
    os.remove(os.path.join(part_name, f))
try:
  os.rmdir(part_name)
  os.mkdir(part_name)
except:
  print("Problem deleting or creating the folder")

if os.path.isdir(path):
  for file in os.listdir(path):
    if "datalog.txt" in file:
      f = open(path+file, 'r')
      for line in f:
        try:
          datalog = json.loads(line)
        except:
          continue
        if "pitime" in datalog.keys():
          date = datalog['pitime'].split('T')[0]
          partname = part_name + os.sep + date + '.txt'
          fwrite = open(partname,'a')
          fwrite.write(line)
          fwrite.close()
      f.close()


#After Partitioning the data

tt2=0
Query_1_2 = []
Query_2_2 = [0]

if os.path.isdir(part_name):
  for file in os.listdir(part_name):
    start = time.time()
    #check for correct .txt datalog file
    if str(lowerdt1).split(" ")[0] <= file.split(".")[0] and str(upperdt1).split(" ")[0] >= file.split(".")[0]:
      f = open(part_name+os.sep+file, 'r')
      for line in f:
        try:
          datalog = json.loads(line)
        except:
          continue
        #check if type exists in file
        if "type" in datalog.keys():
          #check whether tpye is print_reading, as temp information are in these type lines only
          if datalog["type"] == "print_reading":
            if "pitime" in datalog.keys():
              #formatting date
              pidate = datetime.strptime(datalog["pitime"].split("T")[0]+ " " + str(datalog["pitime"].split("T")[1].split(".")[0]), '%Y-%m-%d %H:%M:%S')
              #finding results for 1st query
              if 'temp_5' in datalog.keys() and 'temp_8' in datalog.keys():
                if pidate >= lowerdt1 and pidate <= upperdt1:
                  result = {}
                  result['pitime'] = datalog["pitime"]
                  result['temp_5'] = datalog['temp_5']
                  result['temp_8'] = datalog['temp_8']
                  Query_1_2.append(result)
      f.close()
    if str(lowerdt2).split(" ")[0] <= file.split(".")[0] and str(upperdt2).split(" ")[0] >= file.split(".")[0]:
      f = open(part_name+os.sep+file, 'r')
      for line in f:
        try:
          datalog = json.loads(line)
        except:
          continue
        #check if type exists in file
        if "type" in datalog.keys():
          #check whether tpye is print_reading, as temp information are in these type lines only
          if datalog["type"] == "print_reading":
            if "pitime" in datalog.keys():
              #formatting date
              pidate = datetime.strptime(datalog["pitime"].split("T")[0]+ " " + str(datalog["pitime"].split("T")[1].split(".")[0]), '%Y-%m-%d %H:%M:%S')
              if 'temp_8' in datalog.keys():
                if pidate >= lowerdt2 and pidate <= upperdt2:
                  Query_2_2.append(float(datalog["temp_8"]))
      f.close()
    tt2 += time.time() - start

#print("Result for Query #1: ", json.dumps(Query_1_2, indent=1))             
#print("Result for Query #2: Average Temperature (temp_8): ", round(averageOfList(Query_2_2), 2))
print("Total time taken for both the Queries after partition:", tt2)