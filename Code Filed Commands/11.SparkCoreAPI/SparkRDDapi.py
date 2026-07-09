from pyspark import SparkContext, SparkConf

conf=SparkConf().setMaster('spark://SoumyadeepDey.domain.name:7077').setAppName('RDDCoreAPI_SELECT+FILTER')
sc = SparkContext.getOrCreate(conf=conf)

rdd1 = sc.textFile('flight_data_1.csv')

def select_api(record):
    row = record.split(',')
    flight_id = row[2]
    aircraft_id =  row[3]
    itinerary_no = row[4]
    return flight_id, aircraft_id, itinerary_no

def filter_api(record):
    global count
    row = record.split(',')
    passenger_country = row[14]
    if count > 10:
        exit
    if passenger_country == 'USA':
        data = row
        count = count + 1
        return data
    else:
        pass

count = 0
rdd2 = rdd1.map(lambda x: select_api(x))
rdd3 = rdd1.filter(lambda x: filter_api(x))

count1 = 0
count2 = 0
for i in rdd2.collect():
    print(i)
    count1 = count1 + 1
    if count1 > 10:
        break

rdd3.saveAsTextFile('filtered_flight_data')
