import pandas as pd
import itertools
import sys

class WeatherData(object):

	def __init__(self, cleanedName, mainDataTypes=[]):
		self.cleanedName = (cleanedName.split('_')[1]).split('.')[0]
		self.cleanedDF = pd.read_csv((cleanedName))
		self.baseColumns = ['STATION_CODE','YEAR', "MONTH", "DAY"]
		self.mainDataTypes = ['PRCP','SNOW','SNWD','TMAX','TMIN','TAVG']+mainDataTypes
		self.stationStatHeaders = ['LATITUDE','LONGITUDE','ELEVATION','STATION_NAME']
		self.stationStats = None

		self.code = self.cleanedDF.iloc[0]['V1']
		self.years = sorted(((self.cleanedDF.iloc[1]).tolist())[1:])
		self.months = ((self.cleanedDF.iloc[2]).tolist())[1:]
		self.months = [m.strip() for m in self.months]
		self.sensors = ((self.cleanedDF.iloc[3]).tolist())[1:]
		self.ymsTuples = [(y,m,s) for y,m,s in zip(self.years, self.months, self.sensors)]
		self.daysList = [day+1 for day in range(31)]

	# ---------- USER LEVEL FUNCTIONS -----------------

	def getFullCSV(self): 
		self.formatCleanedDF()
		self.getStationData()
		rowData = self.retrieveRowData()
		df = self.generateFullDataframe(rowData)
		df.to_csv(str(self.code)+".csv", encoding='utf-8', index=False)

	# ---------- FORMATTING FUNCTIONS -----------------

	def getStationData(self):
		import csv
		with open("stations.csv") as stations:
		    csvReader = csv.reader(stations)
		    for row in csvReader:
		        if (row[0] == self.cleanedName):
		        	self.stationStats = tuple(row)[1:]
		        	break

	def createColumnHeaders(self, tuplesList):
		return [str(yms[0]+"_"+yms[1]+"_"+yms[2]) for yms in tuplesList]

	def formatCleanedDF(self):
		self.cleanedDF = self.cleanedDF.drop(self.cleanedDF.index[[0,1,2,3]])
		self.cleanedDF.drop('Unnamed: 0', axis=1, inplace=True)
		self.cleanedDF.columns = self.createColumnHeaders(self.ymsTuples)

	# ---------- DATAFRAME GENERATION FUNCTIONS -----------------

	def generateFullDataframe(self, rowData):
		dataframe = pd.DataFrame(rowData, columns=(self.baseColumns+self.stationStatHeaders+self.mainDataTypes))
		dataframe.fillna("NA", inplace=True)
		return dataframe

	def orderByDataType(self, order, rawTuple):
		correctOrder = [self.mainDataTypes.index(sensor) for sensor in order]
		finalTuple = ['NA',]*len(self.mainDataTypes)
		for idx, val in zip(correctOrder, list(rawTuple)):
			try:
				finalTuple[idx] = str(int(val))
			except ValueError:
				continue

		return tuple(finalTuple)

	def retrieveRowData(self): 

		validSensors = list(filter(lambda x: x[2] in self.mainDataTypes, self.ymsTuples))
		cleanedTargetColumns = self.createColumnHeaders(validSensors)
		
		columnsAndYears = [(column, year) for column, year in zip(cleanedTargetColumns, validSensors)]
		formattedYears = [list(filter(lambda x: x[1][0] == year, columnsAndYears)) for year in sorted(set(self.years))]
		monthsAsInt = sorted([int(m) for m in set(self.months)])

		orderedRowData = []
		for year in formattedYears:
			allMonths = [list(filter(lambda x: x[1][1] == str(month), year)) for month in monthsAsInt]
			allMonths = [month for month in allMonths if month != []]
			
			for month in allMonths:
				allSensorData = [self.cleanedDF[sensorType[0]].tolist() for sensorType in month]
				allSensorData = list(zip(*allSensorData))
				sensorOrder = [sensorType[1][2] for sensorType in month]

				stationTimeTuple = (self.code, month[0][1][0], month[0][1][1])
				stationData = [stationTimeTuple+(days,)+self.stationStats for days in self.daysList]
				formattedRowData = [self.orderByDataType(sensorOrder, sensorTup) for sensorTup in allSensorData]
				mergedData = [sData+rData for sData, rData in zip(stationData, formattedRowData)]
				orderedRowData.append(mergedData)

		return list(itertools.chain.from_iterable(orderedRowData))

# -------- SCRIPT EXECUTION BELOW ------------------------

if __name__ == "__main__":
	if (len(sys.argv) > 2):
		data = WeatherData(cleanedName=str(sys.argv[1]), mainDataTypes=sys.argv[2:])
		data.getFullCSV()
	else:
		data = WeatherData(cleanedName=str(sys.argv[1]))
		data.getFullCSV()
