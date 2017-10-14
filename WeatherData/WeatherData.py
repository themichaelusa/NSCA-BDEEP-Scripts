import pandas as pd
import itertools

class WeatherData(object):

	def __init__(self, cleanedName):
		self.cleanedDF = pd.read_csv((cleanedName +'.csv'))
		self.baseColumns = ['STATION_CODE','YEAR']
		self.stationInfo = ['LATITUDE','LONGITUDE','ELEVATION','STATE','CITY']
		self.mainDataTypes = ['PREP','SNOW','SNWD','TMAX','TMIN','AWDR','AWND','EVAP','WSFG']

		self.code = self.cleanedDF.iloc[0]['V1']
		self.years = ((self.cleanedDF.iloc[1]).tolist())[1:]
		self.months = ((self.cleanedDF.iloc[2]).tolist())[1:]
		self.sensors = ((self.cleanedDF.iloc[3]).tolist())[1:]
		self.ymsTuples = [(y,m,s) for y,m,s in zip(self.years, self.months, self.sensors)]	
		self.firstUsage = True

	# ---------- USER LEVEL FUNCTIONS -----------------
	def getFullCSV(self, asDF=False): 
		self.firstTimeUseCheck()
		fullDF, firstLoop = None, True

		for year in set(self.years):
			for month in set(self.months):
				if(firstLoop):
					firstLoop = False
					fullDF = self.createFormattedCSV(year, month, True)
				else:
					monthDF = self.createFormattedCSV(year, month, True)
					fullDF = fullDF.append(monthDF, ignore_index=True)

		if (asDF):
			csvName = self.code + '_' + str(year) + '.csv'
			fullDF.to_csv(csvName, encoding='utf-8', index=False)
		else:
			return fullDF

	# ---------- FORMATTING FUNCTIONS -----------------
	def firstTimeUseCheck(self):
		if (self.firstUsage):
			self.firstUsage = False
			self.formatCleanedDF()	

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
		dfColumns = self.baseColumns + self.stationInfo + self.mainDataTypes
		allYearsInDF = [[year]*days for year in uniqueYears]
		allYearsInDF = list(itertools.chain.from_iterable(allYearsInDF))
		baseTuples = [(self.code, year) for year in allYearsInDF]

		naTuples = [('NA',)*(len(self.stationInfo)+len(self.mainDataTypes)) for i in range(days*len(uniqueYears))]
		dfTuples = [tuple(i+j) for i,j in zip(baseTuples, naTuples)]

		return pd.DataFrame(dfTuples, columns=dfColumns)

	def populateDataframeSensors(self, emptyDF, year, month): 
		
		inputMonthSensors = list(filter(lambda x: x[1] == str(month), self.ymsTuples))
		validSensors = list(filter(lambda x: x[2] in self.mainDataTypes, inputMonthSensors))
		cleanedTargetColumns = self.createColumnHeaders(validSensors)
		populatedColumns = [self.cleanedDF[target].tolist() for target in cleanedTargetColumns]

		for column, sensor in zip(populatedColumns, validSensors):
			emptyDF[sensor[2]] = column

		return emptyDF[emptyDF['YEAR'] == str(year)]

# -------- CLASS TESTING BELOW ------------------------

if __name__ == "__main__":
	data = WeatherData(cleanedName='CleanedACW00011604')
	data.getFullCSV()
