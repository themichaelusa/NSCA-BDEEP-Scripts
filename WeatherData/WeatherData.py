import pandas as pd
import itertools
import sys

class WeatherData(object):

	def __init__(self, cleanedName, mainDataTypes=[]):
		self.cleanedName = cleanedName.split('_')[1]
		self.cleanedDF = pd.read_csv((cleanedName +'.csv'))
		self.baseColumns = ['STATION_CODE','YEAR']
		self.mainDataTypes = list(set(['PRCP','SNOW','SNWD','TMAX','TMIN']+mainDataTypes))
		self.stationStatHeaders = ['LATITUDE','LONGITUDE','ELEVATION','STATION_NAME']
		self.stationStats = None

		self.code = self.cleanedDF.iloc[0]['V1']
		self.years = ((self.cleanedDF.iloc[1]).tolist())[1:]
		self.months = ((self.cleanedDF.iloc[2]).tolist())[1:]
		self.sensors = ((self.cleanedDF.iloc[3]).tolist())[1:]
		self.ymsTuples = [(y,m,s) for y,m,s in zip(self.years, self.months, self.sensors)]	
		self.firstUsage = True

	# ---------- USER LEVEL FUNCTIONS -----------------
	def getFullCSV(self): 
		self.firstTimeUseCheck()
		fullDF, firstLoop = None, True

		for year in set(self.years):
			for month in sorted(set(self.months)):
				if(firstLoop):
					firstLoop = False
					fullDF = self.createFormattedCSV(year, month, True)
				else:
					monthDF = self.createFormattedCSV(year, month, True)
					fullDF = fullDF.append(monthDF, ignore_index=True)

		csvName = self.code + '_' + str(year) + '.csv'
		fullDF.to_csv(csvName, encoding='utf-8', index=False)

	# ---------- FORMATTING FUNCTIONS -----------------
	def firstTimeUseCheck(self):
		if (self.firstUsage):
			self.firstUsage = False
			self.formatCleanedDF()
			self.getStationData()

	def getStationData(self):
		import csv
		with open('stations.csv') as stations:
		    csvReader = csv.reader(stations)
		    for row in csvReader:
		        if (row[0] == self.cleanedName):
		        	self.stationStats = tuple(row)[1:]
		        	break

	def createFormattedCSV(self, year, month, appendMode=False):
		emptyDF = self.createEmptyDataframe()
		populatedDF = self.populateDataframeSensors(emptyDF, year, month)
		if (appendMode == False):
			csvName = self.code + '_' + str(year) + '_M' + str(month)+'.csv'
			populatedDF.to_csv(csvName, encoding='utf-8', index=False)
		else:
			return populatedDF

	def createColumnHeaders(self, tuplesList):
		return [str(yms[0]+"_"+yms[1]+"_"+yms[2]) for yms in tuplesList]

	def formatCleanedDF(self):
		self.cleanedDF = self.cleanedDF.drop(self.cleanedDF.index[[0,1,2,3]])
		self.cleanedDF.drop('Unnamed: 0', axis=1, inplace=True)
		self.cleanedDF.columns = self.createColumnHeaders(self.ymsTuples)

	# ---------- DATAFRAME GENERATION FUNCTIONS -----------------
	def createEmptyDataframe(self):
		
		days, uniqueYears = 31, set(self.years)
		dfColumns = self.baseColumns + self.stationStatHeaders + self.mainDataTypes
		allYearsInDF = [[year]*days for year in uniqueYears]
		allYearsInDF = list(itertools.chain.from_iterable(allYearsInDF))
		baseTuples = [(self.code, year) for year in allYearsInDF]

		naTuples = [('NA',)*(len(self.mainDataTypes)) for i in range(days*len(uniqueYears))]
		stationTuples = [self.stationStats for i in range(days*len(uniqueYears))]
		dfTuples = [tuple(i+j+k) for i,j,k in zip(baseTuples, stationTuples, naTuples)]

		return pd.DataFrame(dfTuples, columns=dfColumns)

	def populateDataframeSensors(self, emptyDF, year, month): 
		
		inputMonthSensors = list(filter(lambda x: x[1] == str(month), self.ymsTuples))
		validSensors = list(filter(lambda x: x[2] in self.mainDataTypes, inputMonthSensors))
		cleanedTargetColumns = self.createColumnHeaders(validSensors)
		populatedColumns = [self.cleanedDF[target].tolist() for target in cleanedTargetColumns]

		for column, sensor in zip(populatedColumns, validSensors):
			emptyDF[sensor[2]] = column

		return emptyDF[emptyDF['YEAR'] == str(year)]

# -------- SCRIPT EXECUTION BELOW ------------------------

if __name__ == "__main__":
	if (len(sys.argv) > 2):
		data = WeatherData(cleanedName=str(sys.argv[1]), mainDataTypes=sys.argv[2:])
		data.getFullCSV()
	else:
		data = WeatherData(cleanedName=str(sys.argv[1]))
		data.getFullCSV()

