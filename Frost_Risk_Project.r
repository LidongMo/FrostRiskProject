# this is the code fot the "FrosRisk" project

# import the packages need for this calculation
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
source("FrostRiskSourceFunctions.r")

# prepare the raster 
rawRaster = raster("CHELSA_bio10_01.tif")
# aggregat this into 0.5 degree resolution
aggregatedRaster = aggregate(rawRaster,fact= 60)
# replace 
aggregatedRaster[!is.na(aggregatedRaster)] = 1
aggregatedRaster[is.na(aggregatedRaster)] = 2
# change the value 1 into NA
aggregatedRaster[aggregatedRaster==1] = NA
# distance calculation
distanceRaster = distance(aggregatedRaster)
# cropExtent = c(-180, 180, 0, 90 )
writeRaster (distanceRaster,"Distance_to_Ocean.tif",overwrite=T)


#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 1 Find the GFBI species wether has a specific value in the reference table
# load the reference table
referenceTable = fread("ReferenceTable/species_reference_list.csv")
# split the referenceTable into deciduos and evergreen
deciduousReferenceTable = referenceTable[referenceTable$leaf_persistence %in% c("d"),]
evergreenReferenceTable = referenceTable[referenceTable$leaf_persistence %in% c("e"),]
# write the tables
write.csv(deciduousReferenceTable,"ReferenceTable/Deciduous_species_reference_list.csv")
write.csv(evergreenReferenceTable,"ReferenceTable/Evergreen_species_reference_list.csv")

#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 2 Find the GFBI species wether has a specific value in the reference table
# load the TNRS corrected and raw GFBI species names
GFBIRawCorrectedNames = fread("GFBISpeciesNames/GFBI_raw_and_tnrs_corrected_binomial_df.csv",head=T)[,-1]
# parallel running based on each row
# subset the GFBIRawCorrectedNames by the genus exist in the deciduousreference table
deciduousReferenceTable = fread("ReferenceTable/Deciduous_species_reference_list.csv")[,-1]
genusFilteredGFBI = GFBIRawCorrectedNames [GFBIRawCorrectedNames$CorrectedGenus %in% deciduousReferenceTable$genus,]
# for (ro in 1:nrow(GFBIRawCorrectedNames))
parallelSearchFunc = function(ro)
{
    # get each row in the GFBIRawCorrectedNames
    perRowGFBI = genusFilteredGFBI[ro,]
    # subet the genus level matched reference data
    subsetReferenceTable = deciduousReferenceTable[deciduousReferenceTable$genus %in% perRowGFBI$CorrectedGenus,]
    # subset the reference table to genus level
    if (perRowGFBI$CorrectedSpecies %in% subsetReferenceTable$species)
    {
        # if the species names matched then return the corresponding leafout information
        speciesSubsetReference = subsetReferenceTable[subsetReferenceTable$species == perRowGFBI$CorrectedSpecies,]
        # append this to the perRowGFBI row data
        perRowGFBI$Leafout = speciesSubsetReference$leafout
        perRowGFBI$Source = "Species"
        perRowGFBI$habit = speciesSubsetReference$habit
    }else
    {
        # if not match, then return the average in that genus
        perRowGFBI$Leafout = mean(subsetReferenceTable$leafout)
        # 
        perRowGFBI$Source = "genus"
        # here we are getting the maximum habit type from that genus
        perRowGFBI$habit = names(which.max(table(subsetReferenceTable$habit)))
    }
    # return the out put for each row
    print(paste("--- The row ",ro," has been cacluted ---",sep=""))
    return(perRowGFBI)  
}

system.time(resultList <- mclapply(1:nrow(genusFilteredGFBI),parallelSearchFunc,mc.cores=10,mc.preschedule=F))
# rbind the result
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_matched_deciduoud_species_with_leafout_20190316.csv")

#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 3 Check those plots in GFBI which only has the species in the resutlDataFrame (100%) which is the extremly condition of the next step calculation
# name is as another reference table
leafoutReferenceTable = fread("GFBI_matched_species_with_leafout_20190311.csv")[,-1]
# loop by plot in new plot allocated GFBI data framegdal
GFBITreeLevelData = fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")
# get the plot names
plotNames = unique(GFBITreeLevelData$NewPLT)
plotMatchingFunc = function(plt)
{
    # subset the data to plot level
    perPlotDF = GFBITreeLevelData[GFBITreeLevelData$NewPLT==plt,]
    # check the SPCD is %in% leafoutReferenceTable 
    if(all(perPlotDF$SPCD %in% leafoutReferenceTable$RawName))
    {
        print(plt)
        return(perPlotDF)
    }
}

# do parallel running
system.time(resultList <- mclapply(plotNames,plotMatchingFunc,mc.cores=20,mc.preschedule=F))
# resultList = resultList[-which(lapply(resultList,is.null) == T)]
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_100_percent_matched_plots_tree_level_data.csv")


#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 4 check the 
library(data.table)
library(parallel)
library(dplyr)
# set the working directory by cmd 
# cd  /nfs/nas22.ethz.ch/fs2201/usys_ibz_cr_lab/Lidong/BiomassEstimation/
# set the R version
# module load new gcc/4.8.2 r/3.5.1
# read the data with TPH
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# load the GFBi data
BiomassBiomeTPH                 <- fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")[,-1]
# load the deciduous leaf out day reference table
deciduosLeafoutReference = fread("GFBI_matched_deciduoud_species_with_leafout_20190316.csv")[,-1]
# get the plot names
plotNames                       <- as.vector(unique(BiomassBiomeTPH$NewPLT))

# species percentage from the reference table
# individual percentage from the reference table
# basal area percentage from the reference table

# # individual number
# # speices number
# mean leaf out day 
# weighted mean by individual number of leaf out day
# weighted mean by basal area of leaf out day
plotLevelStatFunc = function(pn=PlotName,inputPlot = InputPlot)
{
    
    # get the subset data for each plot by plot name
    perPlotDataFrame = inputPlot
    # calculate the basal area for each indiviudal
    perPlotDataFrame$basalArea = pi*((perPlotDataFrame$DBH/2)^2)
    # get the total tree number in per plot
    individualNumber = nrow(perPlotDataFrame)
    # get the species number
    speciesNumber = length(unique(perPlotDataFrame$SPCD))
    # get the individual number in the plot
    individualNumber = nrow(perPlotDataFrame)
    # individual number percentage in the reference table
    logicVectorInd = perPlotDataFrame$SPCD %in% deciduosLeafoutReference$RawName
    individualPercent = sum(logicVectorInd == "TRUE")/individualNumber
    # get the species number percentage in the reference table
    logicVectorSpe = unique(perPlotDataFrame$SPCD) %in% deciduosLeafoutReference$RawName
    speciesPercent = sum(logicVectorSpe == "TRUE")/speciesNumber
    xRow =  which(names(perPlotDataFrame)=="SPCD")

    # get the leafout day for each row
    rowLeafoutDay = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduosLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificLeafout = deciduosLeafoutReference[deciduosLeafoutReference$RawName %in% speciesName,][,c("Leafout")]
            return(specificLeafout)
        }else
        {
            return(NA)
        }
    }
    # apply the function to each row
    perPlotDataFrame$Leafout =unlist(apply(perPlotDataFrame,1,rowLeafoutDay))

    rowHabit = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduosLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificHabit = deciduosLeafoutReference[deciduosLeafoutReference$RawName %in% speciesName,][,c("habit")]
            return(specificHabit)
        }else
        {
            return(NA)
        }
    }
    # allocate names to this data frame
    perPlotDataFrame$Habit = unlist(apply (perPlotDataFrame,1,rowHabit))
    # remove the NA values
    plotLeafoutDay = na.omit(perPlotDataFrame)
    plotLeafoutDay$Freq = 1
    # if the percentage is 0 allocate NA to those plot
    if(individualPercent ==0)
    {
        meanLeafoutDay = NA
        # weighted mean by indiviudal frequencey 
        weightedMeanLeafoutInd = NA
        # weigthe mean by basal area
        weightedMeanLeafoutBas = NA
    }else
    {
        # mean leaf out day by individual
        meanLeafoutDay = mean(unique(plotLeafoutDay$Leafout))
        # weighted mean by indiviudal frequencey 
        weightedMeanLeafoutInd = mean(plotLeafoutDay$Leafout)
        # weigthe mean by basal area
        weightedMeanLeafoutBas = weighted.mean(plotLeafoutDay$Leafout,plotLeafoutDay$basalArea)
    }
    if (nrow(plotLeafoutDay)==0)
    {
        
        individualLeafoutGreater =NA
        individualLeafoutSmaller = NA
        speciesLeafoutGreater = NA
        speciesLeafoutSmaller = NA
    }else
    {
        # remove the rows which do nat have leafout information wich have been allocated NA
        individualLeafoutGreater = mean(plotLeafoutDay$Leafout>=100)
        individualLeafoutSmaller = mean(plotLeafoutDay$Leafout<100)
        # get the unique of species and leafout data frame
        uniqueDataFrame = unique(plotLeafoutDay[,c("SPCD","Leafout")])
        # get the percentage of leaf out smaller or lager than the standard
        speciesLeafoutGreater = mean(uniqueDataFrame$Leafout>=100)
        speciesLeafoutSmaller = mean(uniqueDataFrame$Leafout<100)
    }
    # get the rows only have trees
    treeOnlyPlotDF = plotLeafoutDay[plotLeafoutDay$Habit =="t",]
    if (nrow(treeOnlyPlotDF)==0)
    {
        individualLeafoutGreaterT = NA
        individualLeafoutSmallerT = NA
        speciesLeafoutGreaterT = NA
        speciesLeafoutSmallerT =NA

    }else
    {
        individualLeafoutGreaterT = mean(treeOnlyPlotDF$Leafout>=105)
        individualLeafoutSmallerT = mean(treeOnlyPlotDF$Leafout<105)
        # get the unique of species and leafout data frame
        uniqueDataFrameT = unique(treeOnlyPlotDF[,c("SPCD","Leafout")])
        # get the percentage of leaf out smaller or lager than the standard
        speciesLeafoutGreaterT = mean(uniqueDataFrameT$Leafout>=105)
        speciesLeafoutSmallerT = mean(uniqueDataFrameT$Leafout<105)
    }
    
        # get the rows only have trees
    shrubOnlyPlotDF = plotLeafoutDay[plotLeafoutDay$Habit =="t",]
    if (nrow(shrubOnlyPlotDF)==0)
    {
        individualLeafoutGreaterS = NA
        individualLeafoutSmallerS = NA
        speciesLeafoutGreaterS = NA
        speciesLeafoutSmallerS = NA
    }else
    {
        individualLeafoutGreaterS = mean(shrubOnlyPlotDF$Leafout>=95)
        individualLeafoutSmallerS = mean(shrubOnlyPlotDF$Leafout<95)
        # get the unique of species and leafout data frame
        uniqueDataFrameS = unique(shrubOnlyPlotDF[,c("SPCD","Leafout")])
        # get the percentage of leaf out smaller or lager than the standard
        speciesLeafoutGreaterS = mean(uniqueDataFrameS$Leafout>=95)
        speciesLeafoutSmallerS = mean(uniqueDataFrameS$Leafout<95)
    }
    

    # return the information row out
    outputRow = data.frame(RawPLT = unique(perPlotDataFrame$PLT),
                          PLT=pn,
                          LAT=unique(perPlotDataFrame$LAT),
                          LON=unique(perPlotDataFrame$LON),
                          YEAR=unique(perPlotDataFrame$Year),
                          IndividualNumber = individualNumber,
                          SpeciesNumber = speciesNumber,
                          IndividualPercent = individualPercent,
                          SpeciesPercent = speciesPercent,
                          PlotArea = 1/(perPlotDataFrame$TPH[1]),
                          TotalBiomass = sum(perPlotDataFrame$Biomass),
                          MeanDBH = mean(perPlotDataFrame$DBH),
                          MeanLeafoutDay = meanLeafoutDay,
                          WeightedMeanLeafoutInd = weightedMeanLeafoutInd,
                          WeightedMeanLeafoutBas = weightedMeanLeafoutBas,
                          speciesLeafoutGreater = speciesLeafoutGreater,
                          speciesLeafoutSmaller = speciesLeafoutSmaller,
                          individualLeafoutGreater = individualLeafoutGreater,
                          individualLeafoutSmaller = individualLeafoutSmaller,
                          speciesLeafoutGreaterTree = speciesLeafoutGreaterT,
                          speciesLeafoutSmallerTree = speciesLeafoutSmallerT,
                          individualLeafoutGreaterTree = individualLeafoutGreaterT,
                          individualLeafoutSmallerTree = individualLeafoutSmallerT,
                          speciesLeafoutGreaterShrub = speciesLeafoutGreaterS,
                          speciesLeafoutSmallerShrub = speciesLeafoutSmallerS,
                          individualLeafoutGreaterShrub = individualLeafoutGreaterS,
                          individualLeafoutSmallerShrub = individualLeafoutSmallerS)
    print(paste("--- the metadata for plot ",pn," has been calculated ---"))
    return(outputRow)   
}

# in order to get the plot with time series, we just to check the plot with multiple years records
MultipleYearTPHStatistic = function(pn)
{
    # get the per plot data 
    perPlotDF = fread(paste("PerPLT/PLT_",pn,".csv",sep=""))[,-1]
    perPlotDF = na.omit(perPlotDF)
    # check is the plot has more than one years observations
    startDatFrame = data.frame()
    if (length(unique(perPlotDF$Year)) > 1)
    {
        # get the years in that plot
        yearsVector = unique(perPlotDF$Year)
        # lopp by year
        for (yr in yearsVector)
        {
            # subset the yearly data frame 
            yearlyDataFrame = perPlotDF[perPlotDF$Year == yr,]
            # check how many possible TPHs in there
            if (length(unique(yearlyDataFrame$TPH)) > 1)
            {
                TPHVector = unique(yearlyDataFrame$TPH)
                # loopby TPHVector
                for (tph in TPHVector)
                {
                    # get the tph subset data frame
                    perTPHDataFrame = yearlyDataFrame[yearlyDataFrame$TPH == tph,]
                    startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perTPHDataFrame))
                }
            }else
            {
                startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,yearlyDataFrame))
            }
        }
    }else
    {
        startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perPlotDF))
    } 
    return(startDatFrame) 
}
# parallel running
system.time(OutputList  <- mclapply(plotNames,MultipleYearTPHStatistic,mc.cores = 32, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(OutputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Leafout_with_RawPLT.csv")

library(data.table)
library(parallel)
leafoutTable = fread("GFBI_Plots_Statistics_for_Leafout_with_RawPLT.csv")[,-1]
# load the GFBI table
GFBIRawTable = fread("DSN_Reference.csv")
rowPLTOperationFunc = function(ro)
{
    subTable = leafoutTable[ro,]
    # find the rows which has the same coordinates
    GFBISubTable = GFBIRawTable[GFBIRawTable$LAT == subTable$LAT &GFBIRawTable$LON == subTable$LON,]
    subTable$DSN = unique(GFBISubTable$DSN)[1]
    print(ro)
    return(subTable)
}

system.time(outputList <- mclapply(1:nrow(leafoutTable),rowPLTOperationFunc,mc.cores = 34, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(outputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Leafout_with_RawPLT_Final.csv")

setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# read all the four tables we used
listTables = paste("FinalUsedTables/",list.files("FinalUsedTables",pattern = ".csv"),sep="")
tableList = lapply(listTables,fread)
finalTable = rbindlist(tableList)
# subset the table with the column names
subsetTable = finalTable[,c("y","x","MeanLeafoutDay")]
#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 5 get the firs line of each data frame to get the georeference information
library(raster)
# show the plots retained
worldBorder = shapefile("/Volumes/CrowtherLabRAID/Lidong_Mo/BiomassEstimation/WORLD_BORDERS/TM_WORLD_BORDERS_SIMPL-0.3.shp")
# we have to get the single line for each plot coordinates
plotNamesVector = unique(resutlDataFrame$NewPLT)
# for (pn in plotNamesVector)
firstLineRetainFunc = function(pn)
{
    perPlotSub = resutlDataFrame[resutlDataFrame$NewPLT == pn,]
    print(pn)
    return(perPlotSub[1,])
}
# get the first line of each plot for the following ploting process
system.time(resultList <- mclapply(plotNamesVector,firstLineRetainFunc,mc.cores=10,mc.preschedule=F))
onelineResultDF = rbindlist(resultList)
# 

pdf("Retained_Plots_Distributions.pdf")
plot(worldBorder)
points(x=onelineResultDF$LON,y=onelineResultDF$LAT,pch=10,col=("aquamarine4"),cex=0.4,main="Global Distribution of Retained Plots")
dev.off()

#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 6 get the plot level statistics information for
# name is as another reference table
leafoutReferenceTable = fread("GFBI_matched_species_with_leafout_20190311.csv")[,-1]
# loop by plot in new plot allocated GFBI data frame
GFBITreeLevelData = fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")
# get the plot names
plotNames = unique(GFBITreeLevelData$NewPLT)
plotMatchingFunc = function(plt)
{
    # subset the data to plot level
    perPlotDF = GFBITreeLevelData[GFBITreeLevelData$NewPLT==plt,]
    # check the SPCD is %in% leafoutReferenceTable 
    if(all(perPlotDF$SPCD %in% leafoutReferenceTable$RawName))
    {
        print(plt)
        return(perPlotDF)
    }
}

system.time(resultList <- mclapply(plotNames,plotMatchingFunc,mc.cores=20,mc.preschedule=F))
# resultList = resultList[-which(lapply(resultList,is.null) == T)]
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_100_percent_matched_plots_tree_level_data.csv")


#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 7find out which day has the last minimum temperature (0 or -4 C)
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# generate a year vector
yearVector = 1959:2016 

# write a raster stack calc function to get the last 0 degreen at the first 200 days
lastZeroFrostFinding = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[1:182]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= 0))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= 0))
        return(lastDay)
        }
    }
}

lastFourFrostFinding = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[1:182]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= -4))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= -4))
            return(lastDay)
        }
    }
}
lastTwoFrostFinding = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[1:182]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= -2))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= -2))
            return(lastDay)
        }
    }
}

yearlyFrostCalc = function(yr)
{
    climateStack = brick(paste("CRUClimateData/TminDaily/cruncep_",yr,".tif",sep=""),varname= "tmn")
    # get the last frostRaster based on the 0 degree
    lastFrostRaster = calc(climateStack,lastZeroFrostFinding)
    writeRaster(lastFrostRaster,paste("LastZeroForstData/cruncep_last_frost_",yr,".tif",sep=""),overwrite=T)
    # get the last forst based on -4 degree
    lastFrostRaster = calc(climateStack,lastFourFrostFinding)
    writeRaster(lastFrostRaster,paste("LastFourForstData/cruncep_last_frost_",yr,".tif",sep=""),overwrite=T)
    lastFrostRaster = calc(climateStack,lastTwoFrostFinding)
    writeRaster(lastFrostRaster,paste("LastTwoForstData/cruncep_last_frost_",yr,".tif",sep=""),overwrite=T)
    print(paste("--- the last frost for year ",yr," has been calculated ---"))
}

system.time(mclapply(yearVector,yearlyFrostCalc,mc.cores=5,mc.preschedule=F))


#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 8find out which day has the last minimum temperature for southhemisphere (0 or -4 C)
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# generate a year vector
yearVector = 1959:2016 

# write a raster stack calc function to get the last 0 degreen at the last 184 days in the south hemisphere
lastZeroFrostFindingS = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[(length(x)-183):length(x)]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= 0))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= 0))
        return(lastDay)
        }
    }
}

lastFourFrostFindingS = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[(length(x)-183):length(x)]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= -4))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= -4))
            return(lastDay)
        }
    }
}
lastTwoFrostFindingS = function(x)
{
    if (any(is.na(x)))
    {
        return(NA)
    }else
    {
        # subse the vec to 200 days
        subVec = x[(length(x)-183):length(x)]
        # find the days whichi are lower than 0 degree
        if (length(which(subVec <= -2))==0)
        {
            return(0)
        }else
        {
            lastDay = max(which(subVec <= -4))
            return(lastDay)
        }
    }
}

yearlyFrostCalcS = function(yr)
{
    climateStack = brick(paste("CRUClimateData/TminDaily/cruncep_",yr,".tif",sep=""),varname= "tmn")
    # get the last frostRaster based on the 0 degree
    lastFrostRaster = calc(climateStack,lastZeroFrostFindingS)
    writeRaster(lastFrostRaster,paste("LastZeroForstData/cruncep_South_last_frost_",yr,".tif",sep=""),overwrite=T)
    # get the last forst based on -4 degree
    lastFrostRaster = calc(climateStack,lastFourFrostFindingS)
    writeRaster(lastFrostRaster,paste("LastFourForstData/cruncep_South_last_frost_",yr,".tif",sep=""),overwrite=T)
    lastFrostRaster = calc(climateStack,lastTwoFrostFindingS)
    writeRaster(lastFrostRaster,paste("LastTwoForstData/cruncep_South_last_frost_",yr,".tif",sep=""),overwrite=T)
    print(paste("--- the last frost for year ",yr," has been calculated ---"))
}

system.time(mclapply(yearVector,yearlyFrostCalcS,mc.cores=5,mc.preschedule=F))

#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 9 get the degree day for northhemisphere
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# generate a year vector
yearVector = 1959:2016
degreeDayRasterCalculator = function(yr)
{
    # get the mean daily temperature data
    meanDailyTempStack = stack(paste("CRUClimateData/TmeanDaily/cruncep_",yr,".tif",sep=""))
    # loop by the frost types
    for (frostType in c("Zero","Four","Two"))
    {
        # get the last frost layer
        lastFrostRaster = raster(paste("Last",frostType,"ForstData/cruncep_last_frost_",yr,".tif",sep=""))
        # stack them together
        calcStack = stack(lastFrostRaster,meanDailyTempStack)
        # crop to north hemisphere
        cropExtent = c(-180, 180, 0, 90 )
        calcStack = crop(calcStack,cropExtent)
        # use the calculator to do the calculation
        degreeDayRaster = calc(calcStack,degreeDayZeroCalc)
        # wrte the out put to the local folder
        writeRaster(degreeDayRaster,paste("DegreedayToFrost/DegreeZeroToFrost",frostType,"/Degreeday_for_year_",yr,".tif",sep=""),overwrite=T)
        # calculate the degree day with the standard five
        degreeDayRaster = calc(calcStack,degreeDayFiveCalc)
        # wrte the out put to the local folder
        writeRaster(degreeDayRaster,paste("DegreedayToFrost/DegreeFiveToFrost",frostType,"/Degreeday_for_year_",yr,".tif",sep=""),overwrite=T)
    }
    print(paste("--- the degree day to last frost for year ",yr," has been calculated ---"))
}

system.time(mclapply(yearVector,degreeDayRasterCalculator,mc.cores=30,mc.preschedule=F))


degreeDayZeroCalc = function(x)
{
    if (any(is.na(x))|x[1]==0)
    {
        return(0)
    }else
    {
        # subse the vec to 200 days
        dailyTempVect = x[2:(x[1])]
        # change all the values smaller than 0 to 0
        degreeDayVect = replace(dailyTempVect, dailyTempVect < 0, 0)
        # retrun the degree day accumulation
        return(sum(degreeDayVect))
    }
}

degreeDayFiveCalc = function(x)
{
    if (any(is.na(x))|x[1]==0)
    {
        return(0)
    }else
    {
        # subse the vec to 200 days
        dailyTempVect = x[2:(x[1])]
        dailyTempVect = dailyTempVect-5
        # change all the values smaller than 0 to 0
        degreeDayVect = replace(dailyTempVect, dailyTempVect < 0, 0)
        # retrun the degree day accumulation
        return(sum(degreeDayVect))
    }
}


#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 10 get the degree day for south hemisphere
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# generate a year vector
yearVector = 1959:2016
degreeDayRasterCalculatorS = function(yr)
{
    # get the mean daily temperature data for each year
    meanDailyTempStack = stack(paste("CRUClimateData/TmeanDaily/cruncep_",yr,".tif",sep=""))
    # loop by the frost types
    for (frostType in c("Zero","Four"))
    {
        # get the last frost layer
        lastFrostRaster = raster(paste("Last",frostType,"ForstData/cruncep_South_last_frost_",yr,".tif",sep=""))
        # stack them together
        calcStack = stack(lastFrostRaster,meanDailyTempStack)
        # crop to north hemisphere
        cropExtent = c(-180, 180, -90, 0 )
        calcStack = crop(calcStack,cropExtent)
        # use the calculator to do the calculation
        degreeDayRaster = calc(calcStack,degreeDayZeroCalcS)
        # wrte the out put to the local folder
        writeRaster(degreeDayRaster,paste("DegreedayToFrost/DegreeZeroToFrost",frostType,"/Degreeday_South_for_year_",yr,".tif",sep=""),overwrite=T)
        # calculate the degree day with the standard five
        degreeDayRaster = calc(calcStack,degreeDayFiveCalcS)
        # wrte the out put to the local folder
        writeRaster(degreeDayRaster,paste("DegreedayToFrost/DegreeFiveToFrost",frostType,"/Degreeday_South_for_year_",yr,".tif",sep=""),overwrite=T)
    }
    print(paste("--- the degree day to last frost for year ",yr," has been calculated ---"))
}

system.time(mclapply(yearVector,degreeDayRasterCalculatorS,mc.cores=30,mc.preschedule=F))


degreeDayZeroCalcS = function(x)
{
    if (any(is.na(x))|x[1]==0)
    {
        return(0)
    }else
    {
        # subse the vec to the last 184 days
        dailyTempVect = x[c((length(x)-183):(length(x)-184+x[1]))]
        # change all the values smaller than 0 to 0
        degreeDayVect = replace(dailyTempVect, dailyTempVect < 0, 0)
        # retrun the degree day accumulation
        return(sum(degreeDayVect))
    }
}

degreeDayFiveCalcS = function(x)
{
    if (any(is.na(x))|x[1]==0)
    {
        return(0)
    }else
    {
        # subse the vec to 200 days
        dailyTempVect = x[c((length(x)-183):(length(x)-184+x[1]))]
        dailyTempVect = dailyTempVect-5
        # change all the values smaller than 0 to 0
        degreeDayVect = replace(dailyTempVect, dailyTempVect < 0, 0)
        # retrun the degree day accumulation
        return(sum(degreeDayVect))
    }
}




# #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 11 get the degree day mean and standard deviation for north hemisphere
# get the mean and sd of degree days 
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # get the mean 
    meanRaster = calc(dataStack,mean)
    # write raster into local folder
    writeRaster(meanRaster,paste("DegreedayToFrost/NorthHemisphere/Mean_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/NorthHemisphere/Mean_of_",dtv,"_plot.pdf",sep=""))
    plot(meanRaster)
    dev.off()
    print(dtv)
}
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # get the mean 
    sdRaster = calc(dataStack,sd)
    # write raster into local folder
    writeRaster(sdRaster,paste("DegreedayToFrost/NorthHemisphere/StandardDeviation_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/NorthHemisphere/StandardDeviation_of_",dtv,"_plot.pdf",sep=""))
    plot(sdRaster)
    dev.off()
    print(dtv)
}


# #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 12 get the degree day mean and standard deviation for south hemisphere
# get the mean and sd of degree days 
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # get the mean 
    meanRaster = calc(dataStack,mean)
    # write raster into local folder
    writeRaster(meanRaster,paste("DegreedayToFrost/SouthHemisphere/Mean_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/SouthHemisphere/Mean_South_of_",dtv,"_plot.pdf",sep=""))
    plot(meanRaster)
    dev.off()
    print(dtv)
}
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # get the mean 
    sdRaster = calc(dataStack,sd)
    # write raster into local folder
    writeRaster(sdRaster,paste("DegreedayToFrost/SouthHemisphere/StandardDeviation_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/SouthHemisphere/StandardDeviation_of_South_",dtv,"_plot.pdf",sep=""))
    plot(sdRaster)
    dev.off()
    print(dtv)
}

# #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 13 get the difference to mean 
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    meanRaster = raster(paste("DegreedayToFrost/NorthHemisphere/Mean_of_",dtv,"_raster.tif",sep=""))
    for (yr in c(2010,2007,2009,2011,2013))
    {
        # list the files in that foler
        yearRaster = raster(paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",yr,".tif",sep=""))
        # stack them into a stack
        diffRaster = yearRaster - meanRaster
        # write raster into local folder
        writeRaster(diffRaster,paste("DegreedayToFrost/NorthHemisphere/Mean_Difference_of_",dtv,"_",yr,"_raster.tif",sep=""),overwrite=T)
        pdf(paste("DegreedayToFrost/NorthHemisphere/Mean_Difference_of_",dtv,"_",yr,"_plot.pdf",sep=""))
        plot(diffRaster)
        dev.off()
    }
    print(dtv)
}


dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # generete the time series vector
    time = 1:nlayers(dataStack)
    # define the functon 
    pixelTimeSeiresFunc = function(x) 
    {
        if (any(is.na(x)))
        { NA }
        else
        { 
            m = lm(x ~ time)
            summary(m)$coefficients[2]
        }
    } # slope
    # get the mean 
    trendRaster = calc(dataStack,pixelTimeSeiresFunc)
    # write raster into local folder
    writeRaster(trendRaster,paste("DegreedayToFrost/NorthHemisphere/Trend_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/NorthHemisphere/Trend_of_",dtv,"_plot.pdf",sep=""))
    plot(trendRaster)
    dev.off()
    print(dtv)
}


# #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 14 get the difference to mean of south hemisphere
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    meanRaster = raster(paste("DegreedayToFrost/SouthHemisphere/Mean_of_",dtv,"_raster.tif",sep=""))
    for (yr in c(2010,2007,2009,2011,2013))
    {
        # list the files in that foler
        yearRaster = raster(paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",yr,".tif",sep=""))
        # stack them into a stack
        diffRaster = yearRaster - meanRaster
        # write raster into local folder
        writeRaster(diffRaster,paste("DegreedayToFrost/SouthHemisphere/Mean_Difference_of_",dtv,"_",yr,"_raster.tif",sep=""),overwrite=T)
        pdf(paste("DegreedayToFrost/SouthHemisphere/Mean_Difference_of_",dtv,"_",yr,"_plot.pdf",sep=""))
        plot(diffRaster)
        dev.off()
    }
    print(dtv)
}


dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # generete the time series vector
    time = 1:nlayers(dataStack)
    # define the functon 
    pixelTimeSeiresFunc = function(x) 
    {
        if (any(is.na(x)))
        { NA }
        else
        { 
            m = lm(x ~ time)
            summary(m)$coefficients[2]
        }
    } # slope
    # get the mean 
    trendRaster = calc(dataStack,pixelTimeSeiresFunc)
    # write raster into local folder
    writeRaster(trendRaster,paste("DegreedayToFrost/SouthHemisphere/Trend_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/SouthHemisphere/Trend_of_",dtv,"_plot.pdf",sep=""))
    plot(trendRaster)
    dev.off()
    print(dtv)
}

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 15 mann kedall test 
library(data.table)
library(parallel)
library(raster)
library(Kendall)

dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # define the functon 
    tauTimeSeiresFunc = function(x) 
    {
        # y= na.omit(x)
        # if (length(y)<4)
        if(any(is.na(x))|all(x==0))
        { 
            return(NA) 
        }
        else
        { 
            return(MannKendall(x)$tau)
        }
    } # tau
    # get the mean 
    tauRaster = calc(dataStack,tauTimeSeiresFunc)
    # write raster into local folder
    writeRaster(tauRaster,paste("DegreedayToFrost/NorthHemisphere/Tau_North_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/NorthHemisphere/Tau_North_of_",dtv,"_plot.pdf",sep=""))
    plot(tauRaster)
    dev.off()
    pTimeSeiresFunc = function(x) 
    {
        if (any(is.na(x))|all(x==0))
        { NA }
        else
        { 
            # two tails test p value
            return(MannKendall(x)$sl)
        }
    } # p value
    # get the mean 
    pRaster = calc(dataStack,pTimeSeiresFunc)
    # write raster into local folder
    writeRaster(pRaster,paste("DegreedayToFrost/NorthHemisphere/Pvalue_North_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/NorthHemisphere/Pvalue_North_of_",dtv,"_plot.pdf",sep=""))
    plot(pRaster)
    dev.off()
    print(dtv)
}

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 16 mann kedall test for south hemisphere
library(data.table)
library(parallel)
library(raster)
library(Kendall)

dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{
    # list the files in that foler
    rasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",1959:2016,".tif",sep="")
    # stack them into a stack
    dataStack = stack(rasterList)
    # define the functon 
    tauTimeSeiresFunc = function(x) 
    {
        # y= na.omit(x)
        # if (length(y)<4)
        if(any(is.na(x))|all(x==0))
        { 
            return(NA) 
        }
        else
        { 
            return(MannKendall(x)$tau)
        }
    } # tau
    # get the mean 
    tauRaster = calc(dataStack,tauTimeSeiresFunc)
    # write raster into local folder
    writeRaster(tauRaster,paste("DegreedayToFrost/SouthHemisphere/Tau_North_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/SouthHemisphere/Tau_South_of_",dtv,"_plot.pdf",sep=""))
    plot(tauRaster)
    dev.off()
    pTimeSeiresFunc = function(x) 
    {
        if (any(is.na(x))|all(x==0))
        { NA }
        else
        { 
            # two tails test p value
            return(MannKendall(x)$sl)
        }
    } # p value
    # get the mean 
    pRaster = calc(dataStack,pTimeSeiresFunc)
    # write raster into local folder
    writeRaster(pRaster,paste("DegreedayToFrost/SouthHemisphere/Pvalue_South_of_",dtv,"_raster.tif",sep=""),overwrite=T)
    pdf(paste("DegreedayToFrost/SouthHemisphere/Pvalue_South_of_",dtv,"_plot.pdf",sep=""))
    plot(pRaster)
    dev.off()
    print(dtv)
}

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 17 Merge the result for south and north hemisphere
# list the rasters for north and south hemisphere 
southHemiList = list.files(path="DegreedayToFrost/SouthHemisphere",pattern= ".tif")
northHemiList = list.files(path="DegreedayToFrost/NorthHemisphere",pattern= ".tif")
# stack the files listed in each raster file list
southStack = stack(paste("DegreedayToFrost/SouthHemisphere/",southHemiList,sep=""))
northStack = stack(paste("DegreedayToFrost/NorthHemisphere/",northHemiList,sep=""))
# use the merge function to get the global results
mergedStack = merge(northStack,southStack)
# 
# names(mergedStack) = c("M_Diff_DegreeFiveToFrostFour_2007","Mean_Diff_DegreeFiveToFrostFour_2009","M_Diff_DegreeFiveToFrostFour_2010","M_DiffDegreeFiveToFrostZero_2007","M_Diff_DegreeFiveToFrostZero_2009","M_Diff_DegreeFiveToFrostZero_2010","M_Diff_DegreeZeroToFrostFour_2007","M_Diff_DegreeZeroToFrostFour_2009","M_Diff_DegreeZeroToFrostFour_2010","M_Diff_DegreeZeroToFrostZero_2007","M_Diff_DegreeZeroToFrostZero_2009","M_Diff_DegreeZeroToFrostZero_2010","Mean_DegreeFiveToFrostFour","M_DegreeFiveToFrostZero","M_DegreeZeroToFrostFour","M_DegreeZeroToFrostZero","Pvalue_DegreeFiveToFrostFour","Pvalue_DegreeFiveToFrostZero","Pvalue_DegreeZeroToFrostFour","Pvalue_DegreeZeroToFrostZero","SD_DegreeFiveToFrostFour","SD_DegreeFiveToFrostZero","SD_DegreeZeroToFrostFour","SD_DegreeZeroToFrostZero","Tau_DegreeFiveToFrostFour","Tau_of_DegreeFiveToFrostZero","Tau_DegreeZeroToFrostFour","Tau_DegreeZeroToFrostZero","Trend_DegreeFiveToFrostFour","Trend_DegreeFiveToFrostZero","Trend_DegreeZeroToFrostFour","Trend_DegreeZeroToFrostZero")               
# wrtie the merged result into local folder
writeRaster(mergedStack,"Merged_final_result_stack.tif",overwrite=T)

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# SETP 18 get the time series change of forst risk area
# first we need to merge those layers into one global layer for each year

# dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
# yearVector = 1959:2016
# # loop by degree day and forst combination
# for (dtv in dataTypeVectors)
# {
#     # loop by year to do the merge process for the south and north hemisphere
#     for (yr in yearVector) 
#     {
#         # get the south hemisphere raster
#         southFrostRaster = raster(paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",yr,".tif",sep=""))
#         paste("LastZeroForstData/cruncep_South_last_frost_",yr,".tif",sep=""),overwrite=T)
#         # get the north hemisphere raster
#         northFrostRaster = raster(paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",yr,".tif",sep=""))
#         # merge them into one raster
#         mergedGlobalRaster = merge(northFrostRaster,southFrostRaster)
#         # write the  raster into the local folder
#         writeRaster(mergedGlobalRaster,paste("DegreedayToFrost/MergedGlobalDegreeday/Global_Frost_",dtv,"_for_",yr,".tif",sep=""),overwrite=T)
#     }
# }

# for (dtv in dataTypeVectors)
# {
#     # get the raster stack for that dtv 
#     globalStack = stack(paste("DegreedayToFrost/MergedGlobalDegreeday/Global_Degreeday_",dtv,"_for_",yearVector,".tif",sep=""))
#     # change all the values lager than 0 to 1, 
#     globalStack[globalStack>0] = 1
#     # the pixel area raster should be generated
#     pixelArea = area(globalStack)
#     # multiply the stack with the srea raster
#     gloablFrostAreaStack = pixelArea*globalStack
#     # transfer this into a data frame 
#     globalFrosAreaDF = as.data.frame(gloablFrostAreaStack)
#     # write a function for sum of each column
#     yearAreaVetor = colSums(globalFrosAreaDF,na.rm=T)
  

#     myts <- ts(yearAreaVetor, start=1959, end=2016)  
#     pdf(paste("ts_",dtv,".pdf",sep="")) 
#     plot.ts(myts)
#     dev.off()

#     # # define the functon 
#     # timeSeiresFunc = function(x) 
#     # {
#     #     time = 1959:2016
#     #     if (any(is.na(x)))
#     #     { NA }
#     #     else
#     #     { 
#     #         m = lm(x ~ time)
#     #         summary(m)$coefficients[2]
#     #     }
#     # } # slope
#     # timeSeiresFunc(yearAreaVetor)
# }

dataTypeVectors = c("LastZeroForstData","LastFourForstData")
yearVector = 1959:2016
# loop by degree day and forst combination
for (dtp in dataTypeVectors)
{
    # get the south hemisphere raster
    southFrostStack = stack(paste(dtp,"/cruncep_South_last_frost_",yearVector,".tif",sep=""))
    cropSouthExtent = c(-180, 180, -90, 0 )
    southStack = crop(southFrostStack,cropSouthExtent)
    # get the north hemisphere raster
    northFrostStack = stack(paste(dtp,"/cruncep_last_frost_",yearVector,".tif",sep=""))
    # merge them into one raster
    cropNorthExtent = c(-180, 180, 0, 90 )
    northStack = crop(northFrostStack,cropNorthExtent)
    # merge the two stack
    mergedFrostStack = merge(northStack,southStack)
    # write the  raster into the local folder
    writeRaster(mergedFrostStack,paste("MergedGlobalFrost/Global_Frost_",dtp,"_stack.tif",sep=""),overwrite=T)
    # check the regions where forst has dynamics
    # write a function for each pixel
    # we need to allocate the 0 values to NA
    mergedFrostStack[mergedFrostStack==0] =NA
    pxielFunc = function(x)
    {
        if (any(is.na(x)))
        {
            if(all(is.na(x)))
            {
                return(NA)
            }else
            {
                return(1)
            }
        }else
        {
            return(NA)
        }
    }

    # dynamic region calculation
    dynamicFrostRegionRaster = calc(mergedFrostStack,pxielFunc)
    # plot it 
    pdf(paste(dtp,"_Frost_Dynamic_Region.pdf"))
    plot(dynamicFrostRegionRaster)
    dev.off()
    # write this into the local folder
    writeRaster(dynamicFrostRegionRaster,paste("DynamicFrostRegion/",dtp,"_dynamic_frost_region.tif",sep=""),overwrite=T)
    # mask this out by the dynamic regions
    dynamicRgionFrostStack = mask(mergedFrostStack,dynamicFrostRegionRaster)
    dynamicRgionFrostStack[dynamicRgionFrostStack>0] =1
    dynamicRgionFrostStack[is.na(dynamicRgionFrostStack)] =0
    # work out the trend 
    time = 1:nlayers( dynamicRgionFrostStack)
    # define the functon 
    pixelTimeSeiresFunc = function(x) 
    {

        m = lm(x ~ time)
        return(summary(m)$coefficients[2])
    } # slope

    dynamicRegionTrend = calc(dynamicRgionFrostStack,pixelTimeSeiresFunc)
    dynamicRegionTrend = dynamicRegionTrend * dynamicFrostRegionRaster
    # write it into the local folder
    writeRaster(dynamicRegionTrend,paste("DynamicFrostRegion/",dtp,"_Dynamic_region_frost_day_trend.tif",sep=""),overwrite=T)
    # 
    dynamicFrostRegionRaster = calc(mergedFrostStack,pxielFunc)
    dynamicRgionFrostStack[dynamicRgionFrostStack>=0] =1

    pixelArea = area(dynamicRgionFrostStack)
    dynamicAreaFrostStack = pixelArea*dynamicRgionFrostStack
    # trans to data frame
    dynamicRegionDataFram = as.data.frame(dynamicAreaFrostStack)

    # solumn sum
    yealyAreaSum = colSums(dynamicRegionDataFram,na.rm=T)
    # trans to TS data
    tsData = ts(yealyAreaSum, start=1959, end=2016) 

    pdf(paste("ts_",dtp,".pdf",sep="")) 
    plot.ts(tsData)
    dev.off()

}

# get the area which has forst risk for each year

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 19 merge the degree day data of north and south hemisphere

# as we are doing the moving window for each ten years we have to define the start range of the moving windows
# for (syr in 1959:2007)
parallelMovingWindowFunc = function(syr)
{
    # generate the year vector for the moving window
    yearRange = syr:(syr+9)
    # get the north and south hemisphere degree day 
    # list the files in that foler
    southRasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",yearRange,".tif",sep="")
    northRasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",yearRange,".tif",sep="")
    # stack the rasters into the stack
    # stack them into a stack
    southStack = stack(southRasterList)
    northStack = stack(northRasterList)
    # get the mean raster layer for degree days
    southMeanRaster = calc(southStack,mean)
    northMeanRaster = calc(northStack,mean)
    globalMeanRaster = merge(northMeanRaster,southMeanRaster)
    # write the raster into the local folder
    writeRaster(globalMeanRaster,paste("DegreedayGlobalMovindWindow/Global_Degreeday_Mean_",dtv,"_for_Year_",syr,"_to_",(syr+9),"_raster.tif",sep=""),overwrite=T)
    # get the maximum raster layer for degree days
    southMaxRaster = calc(southStack,max)
    northMaxRaster = calc(northStack,max)
    globalMaxRaster = merge(northMaxRaster,southMaxRaster)
    # write the raster into the local folder
    writeRaster(globalMaxRaster,paste("DegreedayGlobalMovindWindow/Global_Degreeday_Max_",dtv,"_for_Year_",syr,"_to_",(syr+9),"_raster.tif",sep=""),overwrite=T)

    # print the year to show the process
    print(syr)
}

dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{   
    print(dtv)
    system.time(mclapply(1959:2007,parallelMovingWindowFunc,mc.cores=15,mc.preschedule=F))
}

# Mann-Kendall Test for the moving window
library(data.table)
library(parallel)
library(raster)
library(Kendall)

# generate a vector of the moving window year names
yearInitial = 1959:2007
yearEnd = yearInitial+9
yearRangeVector = paste(yearInitial,yearEnd,sep="_to_")
# load the mean and max moving window rasters
statisticType = c("Mean","Max")
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")

for (sty in statisticType)
{
    for (dtv in dataTypeVectors)
    {
        # load the gloabal raster for each window
        globalWindowStack = stack(paste("DegreedayGlobalMovindWindow/Global_Degreeday_",sty,"_",dtv,"_for_Year_",yearRangeVector,"_raster.tif",sep=""))
        # Knedall test function definition
        tauTimeSeiresFunc = function(x) 
        {
            # y= na.omit(x)
            # if (length(y)<4)
            if(any(is.na(x))|all(x==0))
            { 
                return(NA) 
            }else
            { 
                return(MannKendall(x)$tau)
            }
        } # tau
        # get the mean 
        tauRaster = calc(globalWindowStack,tauTimeSeiresFunc)
        # write raster into local folder
        writeRaster(tauRaster,paste("DegreedayMovingWindowKendall/Tau_of_Global_",dtv,"_",sty,"_raster.tif",sep=""),overwrite=T)
    
        pTimeSeiresFunc = function(x) 
        {
            if (any(is.na(x))|all(x==0))
            { NA }
            else
            { 
                # two tails test p value
                return(MannKendall(x)$sl)
            }
        } # p value
        # get the mean 
        pRaster = calc(globalWindowStack,pTimeSeiresFunc)
        # write raster into local folder
        writeRaster(pRaster,paste("DegreedayMovingWindowKendall/Pvalue_of_Global_",dtv,"_",sty,"_raster.tif",sep=""),overwrite=T)
        print(dtv)
    }
    print(sty)  
}

statisticType = c("Mean","Max")
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")

yearInitial = 1959:2007
yearEnd = yearInitial+9
yearRangeVector = paste(yearInitial,yearEnd,sep="_to_")

for (sty in statisticType)
{
    for (dtv in dataTypeVectors)
    {
        # load the gloabal raster for each window
        globalWindowStack = stack(paste("DegreedayGlobalMovindWindow/Global_Degreeday_",sty,"_",dtv,"_for_Year_",yearRangeVector,"_raster.tif",sep=""))
        time = 1:nlayers(globalWindowStack)
        pixelTimeSeiresFunc = function(x) 
        {
            if (any(is.na(x))|all(x==0))
            { NA }
            else
            { 
                m = lm(x ~ time)
                summary(m)$coefficients[2]
            }
        } # slope
        # get the mean 
        slopeRaster = calc(globalWindowStack,pixelTimeSeiresFunc)
        # write raster into local folder
        writeRaster(slopeRaster,paste("DegreedayMovingWindowKendall/Slope_of_Global_",dtv,"_",sty,"_raster.tif",sep=""),overwrite=T)
        print(dtv)
    }
    print(sty)  
}


# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 20 merge the degree day data of north and south hemisphere
dataTypeVectors = c("DegreeZeroToFrostZero","DegreeZeroToFrostFour","DegreeFiveToFrostZero","DegreeFiveToFrostFour")
for (dtv in dataTypeVectors)
{   
    print(dtv)
    # get the north and south hemisphere degree day 
    # list the files in that foler
    southRasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_South_for_year_",1959:2016,".tif",sep="")
    northRasterList = paste("DegreedayToFrost/",dtv,"/Degreeday_for_year_",1959:2016,".tif",sep="")
    # stack the rasters into the stack
    # stack them into a stack
    southStack = stack(southRasterList)
    northStack = stack(northRasterList)
    # get the mean raster layer for degree days
    globalStack = merge(northStack,southStack)
    # define a quantile function suitable for the stack calculation
    quantileFunc = function(x)
    {
        if(all(is.na(x)))
        {
            return (NA)
        }else
        {
            outVal = as.vector(quantile(x,0.95,na.r=T))
            return(outVal)
        }
    }
    quantileRaster = calc(globalStack,quantileFunc)
    # write the raster into the local folder
    writeRaster(quantileRaster,paste("GlobalDegreedayToFrostQuantile/Global_Degreeday_Quantile_raster_for_",dtv,".tif",sep=""),overwrite=T)
}


# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 21 do the new frost risk GFBI calculation 
# TRAIT "Leafout" for evergreen gymnosperm 
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# load the raw reference data frame
rawReferenceTable = fread("ReferenceTable/species_list_GFBI.csv")
# subset the data frame which has leaf_persistence "e" and angio_gymno "g"
evergreenReferenceTable = rawReferenceTable[rawReferenceTable$leaf_persistence %in% c("e") &rawReferenceTable$angio_gymno %in% c("g"),]
# write the reference table into the local folder 
write.csv(evergreenReferenceTable,"ReferenceTable/species_list_GFBI_subset_evergren_angio_gymno.csv")

# load the TNRS corrected and raw GFBI species names
GFBIRawCorrectedNames = fread("GFBISpeciesNames/GFBI_raw_and_tnrs_corrected_binomial_df.csv",head=T)[,-1]
# parallel running based on each row
genusFilteredGFBI = GFBIRawCorrectedNames [GFBIRawCorrectedNames$CorrectedGenus %in% evergreenReferenceTable$genus,]
# for (ro in 1:nrow(GFBIRawCorrectedNames))
parallelSearchFunc = function(ro)
{
    # get each row in the GFBIRawCorrectedNames
    perRowGFBI = genusFilteredGFBI[ro,]
    # subet the genus level matched reference data
    subsetReferenceTable = evergreenReferenceTable[evergreenReferenceTable$genus %in% perRowGFBI$CorrectedGenus,]
    # subset the reference table to genus level
    if (perRowGFBI$CorrectedSpecies %in% subsetReferenceTable$species)
    {
        # if the species names matched then return the corresponding leafout information
        speciesSubsetReference = subsetReferenceTable[subsetReferenceTable$species == perRowGFBI$CorrectedSpecies,]
        # append this to the perRowGFBI row data
        perRowGFBI$Leafout = speciesSubsetReference$leafout
        perRowGFBI$Source = "Species"
        perRowGFBI$habit = speciesSubsetReference$habit
    }else
    {
        # if not match, then return the average in that genus
        perRowGFBI$Leafout = mean(subsetReferenceTable$leafout)
        # 
        perRowGFBI$Source = "genus"
        # here we are getting the maximum habit type from that genus
        perRowGFBI$habit = names(which.max(table(subsetReferenceTable$habit)))
    }
    # return the out put for each row
    print(paste("--- The row ",ro," has been cacluted ---",sep=""))
    return(perRowGFBI)  
}

system.time(resultList <- mclapply(1:nrow(genusFilteredGFBI),parallelSearchFunc,mc.cores=10,mc.preschedule=F))
# rbind the result
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_matched_evergreen_species_with_leafout_20190502.csv")


BiomassBiomeTPH                 <- fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")[,-1]
# load the deciduous leaf out day reference table
evergreenLeafoutReference = fread("GFBI_matched_evergreen_species_with_leafout_20190502.csv")[,-1]
# get the plot names
plotNames                       <- as.vector(unique(BiomassBiomeTPH$NewPLT))

# species percentage from the reference table
# individual percentage from the reference table
# basal area percentage from the reference table

# # individual number
# # speices number
# mean leaf out day 
# weighted mean by individual number of leaf out day
# weighted mean by basal area of leaf out day
plotLevelStatFunc = function(pn=PlotName,inputPlot = InputPlot)
{
    
    # get the subset data for each plot by plot name
    perPlotDataFrame = inputPlot
    # calculate the basal area for each indiviudal
    perPlotDataFrame$basalArea = pi*((perPlotDataFrame$DBH/2)^2)
    # get the total tree number in per plot
    individualNumber = nrow(perPlotDataFrame)
    # get the species number
    speciesNumber = length(unique(perPlotDataFrame$SPCD))
    # get the individual number in the plot
    individualNumber = nrow(perPlotDataFrame)
    # individual number percentage in the reference table
    logicVectorInd = perPlotDataFrame$SPCD %in% evergreenLeafoutReference$RawName
    individualPercent = sum(logicVectorInd == "TRUE")/individualNumber
    # get the species number percentage in the reference table
    logicVectorSpe = unique(perPlotDataFrame$SPCD) %in% evergreenLeafoutReference$RawName
    speciesPercent = sum(logicVectorSpe == "TRUE")/speciesNumber
    xRow =  which(names(perPlotDataFrame)=="SPCD")

    # get the leafout day for each row
    rowLeafoutDay = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% evergreenLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificLeafout = evergreenLeafoutReference[evergreenLeafoutReference$RawName %in% speciesName,][,c("Leafout")]
            return(specificLeafout)
        }else
        {
            return(NA)
        }
    }
    # apply the function to each row
    perPlotDataFrame$Leafout =unlist(apply(perPlotDataFrame,1,rowLeafoutDay))

    rowHabit = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% evergreenLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificHabit = evergreenLeafoutReference[evergreenLeafoutReference$RawName %in% speciesName,][,c("habit")]
            return(specificHabit)
        }else
        {
            return(NA)
        }
    }
    # allocate names to this data frame
    perPlotDataFrame$Habit = unlist(apply (perPlotDataFrame,1,rowHabit))
    # remove the NA values
    plotLeafoutDay = na.omit(perPlotDataFrame)
    plotLeafoutDay$Freq = 1
    # if the percentage is 0 allocate NA to those plot
    if(individualPercent ==0)
    {
        meanLeafoutDay = NA
        # weighted mean by indiviudal frequencey 
        weightedMeanLeafoutInd = NA
        # weigthe mean by basal area
        weightedMeanLeafoutBas = NA
    }else
    {
        # mean leaf out day by individual
        meanLeafoutDay = mean(unique(plotLeafoutDay$Leafout))
        # weighted mean by indiviudal frequencey 
        weightedMeanLeafoutInd = mean(plotLeafoutDay$Leafout)
        # weigthe mean by basal area
        weightedMeanLeafoutBas = weighted.mean(plotLeafoutDay$Leafout,plotLeafoutDay$basalArea)
    }
   
    # return the information row out
    outputRow = data.frame(RawPLT = unique(perPlotDataFrame$PLT),
                          PLT= pn,
                          LAT=unique(perPlotDataFrame$LAT),
                          LON=unique(perPlotDataFrame$LON),
                          YEAR=unique(perPlotDataFrame$Year),
                          IndividualNumber = individualNumber,
                          SpeciesNumber = speciesNumber,
                          IndividualPercent = individualPercent,
                          SpeciesPercent = speciesPercent,
                          PlotArea = 1/(perPlotDataFrame$TPH[1]),
                          TotalBiomass = sum(perPlotDataFrame$Biomass),
                          MeanDBH = mean(perPlotDataFrame$DBH),
                          MeanLeafoutDay = meanLeafoutDay,
                          WeightedMeanLeafoutInd = weightedMeanLeafoutInd,
                          WeightedMeanLeafoutBas = weightedMeanLeafoutBas)
    print(paste("--- the metadata for plot ",pn," has been calculated ---"))
    return(outputRow)   
}

# in order to get the plot with time series, we just to check the plot with multiple years records
MultipleYearTPHStatistic = function(pn)
{
    # get the per plot data 
    perPlotDF = fread(paste("PerPLT/PLT_",pn,".csv",sep=""))[,-1]
    perPlotDF = na.omit(perPlotDF)
    # check is the plot has more than one years observations
    startDatFrame = data.frame()
    if (length(unique(perPlotDF$Year)) > 1)
    {
        # get the years in that plot
        yearsVector = unique(perPlotDF$Year)
        # lopp by year
        for (yr in yearsVector)
        {
            # subset the yearly data frame 
            yearlyDataFrame = perPlotDF[perPlotDF$Year == yr,]
            # check how many possible TPHs in there
            if (length(unique(yearlyDataFrame$TPH)) > 1)
            {
                TPHVector = unique(yearlyDataFrame$TPH)
                # loopby TPHVector
                for (tph in TPHVector)
                {
                    # get the tph subset data frame
                    perTPHDataFrame = yearlyDataFrame[yearlyDataFrame$TPH == tph,]
                    startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perTPHDataFrame))
                }
            }else
            {
                startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,yearlyDataFrame))
            }
        }
    }else
    {
        startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perPlotDF))
    } 
    return(startDatFrame) 
}
# parallel running
system.time(OutputList  <- mclapply(plotNames,MultipleYearTPHStatistic,mc.cores = 32, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(OutputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Leafout_Evergreen_Gymnosperm_with_RawPLT.csv")

library(data.table)
library(parallel)
evergreenGymnospermTable = fread("GFBI_Plots_Statistics_for_Leafout_Evergreen_Gymnosperm_with_RawPLT.csv")[,-1]
# load the GFBI table
GFBIRawTable = fread("DSN_Reference.csv")
rowPLTOperationFunc = function(ro)
{
    subTable = evergreenGymnospermTable[ro,]
    # find the rows which has the same coordinates
    GFBISubTable = GFBIRawTable[GFBIRawTable$LAT == subTable$LAT &GFBIRawTable$LON == subTable$LON,]
    subTable$DSN = unique(GFBISubTable$DSN)[1]
    print(ro)
    return(subTable)
}

system.time(outputList <- mclapply(1:nrow(evergreenGymnospermTable),rowPLTOperationFunc,mc.cores = 34, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(outputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Leafout_Evergreen_Gymnosperm_with_RawPLT_Final.csv")



# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 22 do the new frost risk GFBI calculation 
# TRAIT "Leafout" for deciduous
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# load the raw reference data frame
rawReferenceTable = fread("ReferenceTable/Munich_leafout_sd.csv")
# subset the data frame which has leaf_persistence "e" and angio_gymno "g"
deciduousSDReferenceTable = rawReferenceTable[rawReferenceTable$leaf_persistence %in% c("d"),]
deciduousSDReferenceTable = na.omit(deciduousSDReferenceTable)
# write the reference table into the local folder 
write.csv(deciduousSDReferenceTable,"ReferenceTable/species_list_GFBI_subset_deciduous_leaf_out_SD.csv")

# load the TNRS corrected and raw GFBI species names
GFBIRawCorrectedNames = fread("GFBISpeciesNames/GFBI_raw_and_tnrs_corrected_binomial_df.csv",head=T)[,-1]
# parallel running based on each row
genusFilteredGFBI = GFBIRawCorrectedNames [GFBIRawCorrectedNames$CorrectedGenus %in% deciduousSDReferenceTable$genus,]
# for (ro in 1:nrow(GFBIRawCorrectedNames))
parallelSearchFunc = function(ro)
{
    # get each row in the GFBIRawCorrectedNames
    perRowGFBI = genusFilteredGFBI[ro,]
    # subet the genus level matched reference data
    subsetReferenceTable = deciduousSDReferenceTable[deciduousSDReferenceTable$genus %in% perRowGFBI$CorrectedGenus,]
    # subset the reference table to genus level
    if (perRowGFBI$CorrectedSpecies %in% subsetReferenceTable$species)
    {
        # if the species names matched then return the corresponding leafout information
        speciesSubsetReference = subsetReferenceTable[subsetReferenceTable$species == perRowGFBI$CorrectedSpecies,]
        # append this to the perRowGFBI row data
        perRowGFBI$LeafoutSD = speciesSubsetReference$leafoutSD
        perRowGFBI$Source = "Species"
        perRowGFBI$habit = speciesSubsetReference$habit
    }else
    {
        # if not match, then return the average in that genus
        perRowGFBI$LeafoutSD = mean(subsetReferenceTable$leafoutSD)
        # 
        perRowGFBI$Source = "genus"
        # here we are getting the maximum habit type from that genus
        perRowGFBI$habit = names(which.max(table(subsetReferenceTable$habit)))
    }
    # return the out put for each row
    print(paste("--- The row ",ro," has been cacluted ---",sep=""))
    return(perRowGFBI)  
}

system.time(resultList <- mclapply(1:nrow(genusFilteredGFBI),parallelSearchFunc,mc.cores=10,mc.preschedule=F))
# rbind the result
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_matched_deciduous_species_with_leafout_SD_20190502.csv")


BiomassBiomeTPH                 <- fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")[,-1]
# load the deciduous leaf out day reference table
deciduousSDLeafoutReference = fread("GFBI_matched_deciduous_species_with_leafout_SD_20190502.csv")[,-1]
# get the plot names
plotNames                       <- as.vector(unique(BiomassBiomeTPH$NewPLT))

plotLevelStatFunc = function(pn=PlotName,inputPlot = InputPlot)
{
    
    # get the subset data for each plot by plot name
    perPlotDataFrame = inputPlot
    # calculate the basal area for each indiviudal
    perPlotDataFrame$basalArea = pi*((perPlotDataFrame$DBH/2)^2)
    # get the total tree number in per plot
    individualNumber = nrow(perPlotDataFrame)
    # get the species number
    speciesNumber = length(unique(perPlotDataFrame$SPCD))
    # get the individual number in the plot
    individualNumber = nrow(perPlotDataFrame)
    # individual number percentage in the reference table
    logicVectorInd = perPlotDataFrame$SPCD %in% deciduousSDLeafoutReference $RawName
    individualPercent = sum(logicVectorInd == "TRUE")/individualNumber
    # get the species number percentage in the reference table
    logicVectorSpe = unique(perPlotDataFrame$SPCD) %in% deciduousSDLeafoutReference $RawName
    speciesPercent = sum(logicVectorSpe == "TRUE")/speciesNumber
    xRow =  which(names(perPlotDataFrame)=="SPCD")

    # get the leafout day for each row
    rowLeafoutDay = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduousSDLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificLeafoutSD = deciduousSDLeafoutReference[deciduousSDLeafoutReference$RawName %in% speciesName,][,c("LeafoutSD")]
            return(specificLeafoutSD)
        }else
        {
            return(NA)
        }
    }
    # apply the function to each row
    perPlotDataFrame$LeafoutSD =unlist(apply(perPlotDataFrame,1,rowLeafoutDay))

    rowHabit = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduousSDLeafoutReference$RawName)
        {
            # get the leaf out day for that row
            specificHabit = deciduousSDLeafoutReference[deciduousSDLeafoutReference$RawName %in% speciesName,][,c("habit")]
            return(specificHabit)
        }else
        {
            return(NA)
        }
    }
    # allocate names to this data frame
    perPlotDataFrame$Habit = unlist(apply (perPlotDataFrame,1,rowHabit))
    # remove the NA values
    plotLeafoutSD = na.omit(perPlotDataFrame)
    plotLeafoutSD$Freq = 1
    # if the percentage is 0 allocate NA to those plot
    if(individualPercent ==0)
    {
        meanLeafoutSD = NA
        # weighted mean by indiviudal frequencey 
        weightedLeafoutSDInd = NA
        # weigthe mean by basal area
        weightedLeafoutSDBas = NA
    }else
    {
        # mean leaf out day by individual
        meanLeafoutSD = mean(unique(plotLeafoutSD$LeafoutSD))
        # weighted mean by indiviudal frequencey 
        weightedLeafoutSDInd = mean(plotLeafoutSD$LeafoutSD)
        # weigthe mean by basal area
        weightedLeafoutSDBas = weighted.mean(plotLeafoutSD$LeafoutSD,plotLeafoutSD$basalArea)
    }
   
    # return the information row out
    outputRow = data.frame(PLT=pn,
                          LAT=unique(perPlotDataFrame$LAT),
                          LON=unique(perPlotDataFrame$LON),
                          YEAR=unique(perPlotDataFrame$Year),
                          IndividualNumber = individualNumber,
                          SpeciesNumber = speciesNumber,
                          IndividualPercent = individualPercent,
                          SpeciesPercent = speciesPercent,
                          PlotArea = 1/(perPlotDataFrame$TPH[1]),
                          TotalBiomass = sum(perPlotDataFrame$Biomass),
                          MeanDBH = mean(perPlotDataFrame$DBH),
                          MeanLeafoutSD = meanLeafoutSD,
                          WeightedLeafoutSDInd = weightedLeafoutSDInd,
                          WeightedLeafoutSDBas = weightedLeafoutSDBas)
    print(paste("--- the metadata for plot ",pn," has been calculated ---"))
    return(outputRow)   
}

# in order to get the plot with time series, we just to check the plot with multiple years records
MultipleYearTPHStatistic = function(pn)
{
    # get the per plot data 
    perPlotDF = fread(paste("PerPLT/PLT_",pn,".csv",sep=""))[,-1]
    perPlotDF = na.omit(perPlotDF)
    # check is the plot has more than one years observations
    startDatFrame = data.frame()
    if (length(unique(perPlotDF$Year)) > 1)
    {
        # get the years in that plot
        yearsVector = unique(perPlotDF$Year)
        # lopp by year
        for (yr in yearsVector)
        {
            # subset the yearly data frame 
            yearlyDataFrame = perPlotDF[perPlotDF$Year == yr,]
            # check how many possible TPHs in there
            if (length(unique(yearlyDataFrame$TPH)) > 1)
            {
                TPHVector = unique(yearlyDataFrame$TPH)
                # loopby TPHVector
                for (tph in TPHVector)
                {
                    # get the tph subset data frame
                    perTPHDataFrame = yearlyDataFrame[yearlyDataFrame$TPH == tph,]
                    startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perTPHDataFrame))
                }
            }else
            {
                startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,yearlyDataFrame))
            }
        }
    }else
    {
        startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perPlotDF))
    } 
    return(startDatFrame) 
}
# parallel running
system.time(OutputList  <- mclapply(plotNames,MultipleYearTPHStatistic,mc.cores = 24, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(OutputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Leafout_Deciduous_Leafout_SD.csv")

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 23 do the new frost risk GFBI calculation 
# TRAIT fros for deciduous
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# load the raw reference data frame
rawReferenceTable = fread("ReferenceTable/Munich_frost.csv")
# subset the data frame which has leaf_persistence "e" and angio_gymno "g"
deciduousReferenceTable = rawReferenceTable[rawReferenceTable$leaf_persistence %in% c("d"),]
deciduousReferenceTable = na.omit(deciduousReferenceTable)
# write the reference table into the local folder 
write.csv(deciduousReferenceTable,"ReferenceTable/species_list_GFBI_subset_deciduous_frost.csv")

# load the TNRS corrected and raw GFBI species names
GFBIRawCorrectedNames = fread("GFBISpeciesNames/GFBI_raw_and_tnrs_corrected_binomial_df.csv",head=T)[,-1]
# parallel running based on each row
genusFilteredGFBI = GFBIRawCorrectedNames [GFBIRawCorrectedNames$CorrectedGenus %in% deciduousReferenceTable$genus,]
# for (ro in 1:nrow(GFBIRawCorrectedNames))
parallelSearchFunc = function(ro)
{
    # get each row in the GFBIRawCorrectedNames
    perRowGFBI = genusFilteredGFBI[ro,]
    # subet the genus level matched reference data
    subsetReferenceTable = deciduousReferenceTable[deciduousReferenceTable$genus %in% perRowGFBI$CorrectedGenus,]
    # subset the reference table to genus level
    if (perRowGFBI$CorrectedSpecies %in% subsetReferenceTable$species)
    {
        # if the species names matched then return the corresponding leafout information
        speciesSubsetReference = subsetReferenceTable[subsetReferenceTable$species == perRowGFBI$CorrectedSpecies,]
        # append this to the perRowGFBI row data
        perRowGFBI$Frost = speciesSubsetReference$frost
        perRowGFBI$Source = "Species"
        perRowGFBI$habit = speciesSubsetReference$habit
    }else
    {
        # if not match, then return the average in that genus
        perRowGFBI$Frost= mean(subsetReferenceTable$frost)
        # 
        perRowGFBI$Source = "genus"
        # here we are getting the maximum habit type from that genus
        perRowGFBI$habit = names(which.max(table(subsetReferenceTable$habit)))
    }
    # return the out put for each row
    print(paste("--- The row ",ro," has been cacluted ---",sep=""))
    return(perRowGFBI)  
}

system.time(resultList <- mclapply(1:nrow(genusFilteredGFBI),parallelSearchFunc,mc.cores=10,mc.preschedule=F))
# rbind the result
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_matched_deciduous_species_with_Frost_20190502.csv")


BiomassBiomeTPH                 <- fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")[,-1]
# load the deciduous leaf out day reference table
deciduousFrostReference = fread("GFBI_matched_deciduous_species_with_Frost_20190502.csv")[,-1]
# get the plot names
plotNames                       <- as.vector(unique(BiomassBiomeTPH$NewPLT))

plotLevelStatFunc = function(pn=PlotName,inputPlot = InputPlot)
{
    
    # get the subset data for each plot by plot name
    perPlotDataFrame = inputPlot
    # calculate the basal area for each indiviudal
    perPlotDataFrame$basalArea = pi*((perPlotDataFrame$DBH/2)^2)
    # get the total tree number in per plot
    individualNumber = nrow(perPlotDataFrame)
    # get the species number
    speciesNumber = length(unique(perPlotDataFrame$SPCD))
    # get the individual number in the plot
    individualNumber = nrow(perPlotDataFrame)
    # individual number percentage in the reference table
    logicVectorInd = perPlotDataFrame$SPCD %in% deciduousFrostReference $RawName
    individualPercent = sum(logicVectorInd == "TRUE")/individualNumber
    # get the species number percentage in the reference table
    logicVectorSpe = unique(perPlotDataFrame$SPCD) %in% deciduousFrostReference $RawName
    speciesPercent = sum(logicVectorSpe == "TRUE")/speciesNumber
    xRow =  which(names(perPlotDataFrame)=="SPCD")

    # get the leafout day for each row
    rowLeafoutDay = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduousFrostReference$RawName)
        {
            # get the leaf out day for that row
            specificFrost= deciduousFrostReference[deciduousFrostReference$RawName %in% speciesName,][,c("Frost")]
            return(specificFrost)
        }else
        {
            return(NA)
        }
    }
    # apply the function to each row
    perPlotDataFrame$Frost =unlist(apply(perPlotDataFrame,1,rowLeafoutDay))

    rowHabit = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% deciduousFrostReference$RawName)
        {
            # get the leaf out day for that row
            specificHabit = deciduousFrostReference[deciduousFrostReference$RawName %in% speciesName,][,c("habit")]
            return(specificHabit)
        }else
        {
            return(NA)
        }
    }
    # allocate names to this data frame
    perPlotDataFrame$Habit = unlist(apply (perPlotDataFrame,1,rowHabit))
    # remove the NA values
    plotFrost = na.omit(perPlotDataFrame)
    plotFrost$Freq = 1
    # if the percentage is 0 allocate NA to those plot
    if(individualPercent ==0)
    {
        meanFrost = NA
        # weighted mean by indiviudal frequencey 
        weightedFrostInd = NA
        # weigthe mean by basal area
        weightedFrostBas = NA
    }else
    {
        # mean leaf out day by individual
        meanFrost = mean(unique(plotFrost$Frost))
        # weighted mean by indiviudal frequencey 
        weightedFrostInd = mean(plotFrost$Frost)
        # weigthe mean by basal area
        weightedFrostBas = weighted.mean(plotFrost$Frost,plotFrost$basalArea)
    }
   
    # return the information row out
    outputRow = data.frame(PLT=pn,
                          LAT=unique(perPlotDataFrame$LAT),
                          LON=unique(perPlotDataFrame$LON),
                          YEAR=unique(perPlotDataFrame$Year),
                          IndividualNumber = individualNumber,
                          SpeciesNumber = speciesNumber,
                          IndividualPercent = individualPercent,
                          SpeciesPercent = speciesPercent,
                          PlotArea = 1/(perPlotDataFrame$TPH[1]),
                          TotalBiomass = sum(perPlotDataFrame$Biomass),
                          MeanDBH = mean(perPlotDataFrame$DBH),
                          MeanFrost = meanFrost,
                          WeightedFrostInd = weightedFrostInd,
                          WeightedFrostBas = weightedFrostBas)
    print(paste("--- the metadata for plot ",pn," has been calculated ---"))
    return(outputRow)   
}

# in order to get the plot with time series, we just to check the plot with multiple years records
MultipleYearTPHStatistic = function(pn)
{
    # get the per plot data 
    perPlotDF = fread(paste("PerPLT/PLT_",pn,".csv",sep=""))[,-1]
    perPlotDF = na.omit(perPlotDF)
    # check is the plot has more than one years observations
    startDatFrame = data.frame()
    if (length(unique(perPlotDF$Year)) > 1)
    {
        # get the years in that plot
        yearsVector = unique(perPlotDF$Year)
        # lopp by year
        for (yr in yearsVector)
        {
            # subset the yearly data frame 
            yearlyDataFrame = perPlotDF[perPlotDF$Year == yr,]
            # check how many possible TPHs in there
            if (length(unique(yearlyDataFrame$TPH)) > 1)
            {
                TPHVector = unique(yearlyDataFrame$TPH)
                # loopby TPHVector
                for (tph in TPHVector)
                {
                    # get the tph subset data frame
                    perTPHDataFrame = yearlyDataFrame[yearlyDataFrame$TPH == tph,]
                    startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perTPHDataFrame))
                }
            }else
            {
                startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,yearlyDataFrame))
            }
        }
    }else
    {
        startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perPlotDF))
    } 
    return(startDatFrame) 
}
# parallel running
system.time(OutputList  <- mclapply(plotNames,MultipleYearTPHStatistic,mc.cores = 30, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(OutputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Deciduous_Frost.csv")

# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 24 do the new frost risk GFBI calculation 
# TRAIT frost for evergreen
library(data.table)
library(parallel)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# load the raw reference data frame
rawReferenceTable = fread("ReferenceTable/Munich_frost.csv")
# subset the data frame which has leaf_persistence "e" and angio_gymno "g"
evergreenReferenceTable = rawReferenceTable[rawReferenceTable$leaf_persistence %in% c("e"),]
evergreenReferenceTable = na.omit(evergreenReferenceTable)
# write the reference table into the local folder 
write.csv(evergreenReferenceTable,"ReferenceTable/species_list_GFBI_subset_evergreen_frost.csv")

# load the TNRS corrected and raw GFBI species names
GFBIRawCorrectedNames = fread("GFBISpeciesNames/GFBI_raw_and_tnrs_corrected_binomial_df.csv",head=T)[,-1]
# parallel running based on each row
genusFilteredGFBI = GFBIRawCorrectedNames[GFBIRawCorrectedNames$CorrectedGenus %in% evergreenReferenceTable$genus,]
# for (ro in 1:nrow(GFBIRawCorrectedNames))
parallelSearchFunc = function(ro)
{
    # get each row in the GFBIRawCorrectedNames
    perRowGFBI = genusFilteredGFBI[ro,]
    # subet the genus level matched reference data
    subsetReferenceTable = evergreenReferenceTable[evergreenReferenceTable$genus %in% perRowGFBI$CorrectedGenus,]
    # subset the reference table to genus level
    if (perRowGFBI$CorrectedSpecies %in% subsetReferenceTable$species)
    {
        # if the species names matched then return the corresponding leafout information
        speciesSubsetReference = subsetReferenceTable[subsetReferenceTable$species == perRowGFBI$CorrectedSpecies,]
        # append this to the perRowGFBI row data
        perRowGFBI$Frost = speciesSubsetReference$frost
        perRowGFBI$Source = "Species"
        perRowGFBI$habit = speciesSubsetReference$habit
    }else
    {
        # if not match, then return the average in that genus
        perRowGFBI$Frost= mean(subsetReferenceTable$frost)
        # 
        perRowGFBI$Source = "genus"
        # here we are getting the maximum habit type from that genus
        perRowGFBI$habit = names(which.max(table(subsetReferenceTable$habit)))
    }
    # return the out put for each row
    print(paste("--- The row ",ro," has been cacluted ---",sep=""))
    return(perRowGFBI)  
}

system.time(resultList <- mclapply(1:nrow(genusFilteredGFBI),parallelSearchFunc,mc.cores=10,mc.preschedule=F))
# rbind the result
resutlDataFrame = rbindlist(resultList)
# write the resutl data frame into the local folder
write.csv(resutlDataFrame,"GFBI_matched_evergreen_species_with_Frost_20190502.csv")


BiomassBiomeTPH                 <- fread("GFBI_Biomass_Tree_Level_Biomass_New_PLT_Data_Frame_20181024.csv")[,-1]
# load the deciduous leaf out day reference table
evergreenFrostReference = fread("GFBI_matched_evergreen_species_with_Frost_20190502.csv")[,-1]
# get the plot names
plotNames                       <- as.vector(unique(BiomassBiomeTPH$NewPLT))

plotLevelStatFunc = function(pn=PlotName,inputPlot = InputPlot)
{
    
    # get the subset data for each plot by plot name
    perPlotDataFrame = inputPlot
    # calculate the basal area for each indiviudal
    perPlotDataFrame$basalArea = pi*((perPlotDataFrame$DBH/2)^2)
    # get the total tree number in per plot
    individualNumber = nrow(perPlotDataFrame)
    # get the species number
    speciesNumber = length(unique(perPlotDataFrame$SPCD))
    # get the individual number in the plot
    individualNumber = nrow(perPlotDataFrame)
    # individual number percentage in the reference table
    logicVectorInd = perPlotDataFrame$SPCD %in% deciduousFrostReference $RawName
    individualPercent = sum(logicVectorInd == "TRUE")/individualNumber
    # get the species number percentage in the reference table
    logicVectorSpe = unique(perPlotDataFrame$SPCD) %in% deciduousFrostReference $RawName
    speciesPercent = sum(logicVectorSpe == "TRUE")/speciesNumber
    xRow =  which(names(perPlotDataFrame)=="SPCD")

    # get the leafout day for each row
    rowLeafoutDay = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% evergreenFrostReference$RawName)
        {
            # get the leaf out day for that row
            specificFrost= evergreenFrostReference[evergreenFrostReference$RawName %in% speciesName,][,c("Frost")]
            return(specificFrost)
        }else
        {
            return(NA)
        }
    }
    # apply the function to each row
    perPlotDataFrame$Frost =unlist(apply(perPlotDataFrame,1,rowLeafoutDay))

    rowHabit = function(x)
    {
        # get the species name in that row
        speciesName = x[xRow]
        if(speciesName %in% evergreenFrostReference$RawName)
        {
            # get the leaf out day for that row
            specificHabit = evergreenFrostReference[evergreenFrostReference$RawName %in% speciesName,][,c("habit")]
            return(specificHabit)
        }else
        {
            return(NA)
        }
    }
    # allocate names to this data frame
    perPlotDataFrame$Habit = unlist(apply (perPlotDataFrame,1,rowHabit))
    # remove the NA values
    plotFrost = na.omit(perPlotDataFrame)
    plotFrost$Freq = 1
    # if the percentage is 0 allocate NA to those plot
    if(individualPercent ==0)
    {
        meanFrost = NA
        # weighted mean by indiviudal frequencey 
        weightedFrostInd = NA
        # weigthe mean by basal area
        weightedFrostBas = NA
    }else
    {
        # mean leaf out day by individual
        meanFrost = mean(unique(plotFrost$Frost))
        # weighted mean by indiviudal frequencey 
        weightedFrostInd = mean(plotFrost$Frost)
        # weigthe mean by basal area
        weightedFrostBas = weighted.mean(plotFrost$Frost,plotFrost$basalArea)
    }
   
    # return the information row out
    outputRow = data.frame(PLT=pn,
                          LAT=unique(perPlotDataFrame$LAT),
                          LON=unique(perPlotDataFrame$LON),
                          YEAR=unique(perPlotDataFrame$Year),
                          IndividualNumber = individualNumber,
                          SpeciesNumber = speciesNumber,
                          IndividualPercent = individualPercent,
                          SpeciesPercent = speciesPercent,
                          PlotArea = 1/(perPlotDataFrame$TPH[1]),
                          TotalBiomass = sum(perPlotDataFrame$Biomass),
                          MeanDBH = mean(perPlotDataFrame$DBH),
                          MeanFrost = meanFrost,
                          WeightedFrostInd = weightedFrostInd,
                          WeightedFrostBas = weightedFrostBas)
    print(paste("--- the metadata for plot ",pn," has been calculated ---"))
    return(outputRow)   
}

# in order to get the plot with time series, we just to check the plot with multiple years records
MultipleYearTPHStatistic = function(pn)
{
    # get the per plot data 
    perPlotDF = fread(paste("PerPLT/PLT_",pn,".csv",sep=""))[,-1]
    perPlotDF = na.omit(perPlotDF)
    # check is the plot has more than one years observations
    startDatFrame = data.frame()
    if (length(unique(perPlotDF$Year)) > 1)
    {
        # get the years in that plot
        yearsVector = unique(perPlotDF$Year)
        # lopp by year
        for (yr in yearsVector)
        {
            # subset the yearly data frame 
            yearlyDataFrame = perPlotDF[perPlotDF$Year == yr,]
            # check how many possible TPHs in there
            if (length(unique(yearlyDataFrame$TPH)) > 1)
            {
                TPHVector = unique(yearlyDataFrame$TPH)
                # loopby TPHVector
                for (tph in TPHVector)
                {
                    # get the tph subset data frame
                    perTPHDataFrame = yearlyDataFrame[yearlyDataFrame$TPH == tph,]
                    startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perTPHDataFrame))
                }
            }else
            {
                startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,yearlyDataFrame))
            }
        }
    }else
    {
        startDatFrame = rbind(startDatFrame,plotLevelStatFunc(pn,perPlotDF))
    } 
    return(startDatFrame) 
}
# parallel running
system.time(OutputList  <- mclapply(plotNames,MultipleYearTPHStatistic,mc.cores = 30, mc.preschedule=FALSE))
OutputDF                        <- rbindlist(OutputList)
write.csv(OutputDF,"GFBI_Plots_Statistics_for_Evergreen_Frost.csv")


# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 25 train the machine learning model for leafout day
library(h2o) 
library(data.table)
library(parallel)
library(ggplot2)
library(raster)
# set the working directory
setwd("/Volumes/Scratch/Lidong_Mo/FrostRiskProject/")
# set the temperary files folder
rasterOptions(tmpdir = "/Volumes/Scratch/Lidong_Mo/Temp/")
# source the function
source("biomeLevelMachineLeaningFunc.R") # which is the function defined below

completeStack = stack("Leafout_ML_data/complete.rasters.grd")
# subset the target layers for prediction
selectedRasterNames = c("Q95_DegreeZeroToFrostZero","Distance_to_Sea","wc2.0_bio_2.5m_01","wc2.0_bio_2.5m_02","wc2.0_bio_2.5m_07","wc2.0_bio_2.5m_12","wc2.0_bio_2.5m_15","topo_elevation","sunrad_ave","WWF_Biomes_HalfDegree")
# chose the layers we need for the prediction
selectedLayers = completeStack[[selectedRasterNames]]
# transform this into data frame for the follwoing calculation
gloabalVarialeTable = as.data.frame(selectedLayers,na.rm=T,xy=T)
gloabalVarialeTable$Easting = sin(pi*(gloabalVarialeTable$x)/180)
# write this into the local folder 
write.csv(gloabalVarialeTable,"Leafout_ML_data/Global_Environment_variables_data_frame.csv")
# as there are three differen folders for different dependent variables
# write a loop for it
dependentVariables = c("MeanLeafoutDay","MeanLeafoutSD","MeanFrost")
portVal = 54321
for (dv in dependentVariables)
{   
    
    if (dv == "MeanLeafoutSD")
    {
        biomeVector = c(4,8)
    }else
    {
        biomeVector = c(4,5,6,8)
    }
    for (bm in biomeVector)
    {   
        portVal = portVal +2
        biomeLevelMachineLeaningFunc(dependentVar = dv, biome=bm,portValue = portVal)
    }
    # get all the layers for each dependent variable
    rasterList = paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_biome_",biomeVector,"_seed.tif",sep="")
    # stack all the layers
    rasterStacked = stack(rasterList)
    # sum tha values into one layer
    mergedRaster = sum(rasterStacked,na.rm=T)
    # replace the 0 into NA
    mergedRaster[mergedRaster==0] = NA
    # write into local folder
    writeRaster(mergedRaster,paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_all_biome_merged.tif",sep=""),overwrite=T)
    if (dv != "MeanLeafoutSD")
    {
        # get all the layers for each dependent variable
        rasterList = paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_biome_",c(4,8),"_seed.tif",sep="")
        # stack all the layers
        rasterStacked = stack(rasterList)
        # sum tha values into one layer
        mergedRaster = sum(rasterStacked,na.rm=T)
        # replace the 0 into NA
        mergedRaster[mergedRaster==0] = NA
        # write into local folder
        writeRaster(mergedRaster,paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_4_8_biome_merged.tif",sep=""),overwrite=T)
    
        # get all the layers for each dependent variable
        rasterList = paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_biome_",c(5,6),"_seed.tif",sep="")
        # stack all the layers
        rasterStacked = stack(rasterList)
        # sum tha values into one layer
        mergedRaster = sum(rasterStacked,na.rm=T)
        # replace the 0 into NA
        mergedRaster[mergedRaster==0] = NA
        # write into local folder
        writeRaster(mergedRaster,paste("FrostModellingFolder/",dv,"_ML_Predicted_Raster_for_5_6_biome_merged.tif",sep=""),overwrite=T)

    }
    
    print(paste("--- ",dv," has been finished ------"))
}

# read the raw data frame
biomeLevelMachineLeaningFunc = function(dependentVar = dv, biome=bm, portValue = portVal)
{
    rawDataFrame = fread(paste("Leafout_ML_data/",dependentVar,"/table_",dependentVar,"_biome",biome,".csv",sep=""))
    rawDataFrame$Easting = sin(rawDataFrame$x*pi/180)
    # generate the predictors' name  vector
    # trainVaribles = c("CONTINENT","continent_sub","Q95_DegreeFiveToFrostFour","Q95_DegreeFiveToFrostZero","Q95_DegreeZeroToFrostFour","Q95_DegreeZeroToFrostZero","M_DegreeFiveToFrostFour","M_DegreeFiveToFrostZero","M_DegreeZeroToFrostFour","M_DegreeZeroToFrostZero","SeaDist","wc2.0_bio_2.5m_01","wc2.0_bio_2.5m_02","wc2.0_bio_2.5m_03","wc2.0_bio_2.5m_04","wc2.0_bio_2.5m_05","wc2.0_bio_2.5m_06","wc2.0_bio_2.5m_07","wc2.0_bio_2.5m_08","wc2.0_bio_2.5m_09","wc2.0_bio_2.5m_10","wc2.0_bio_2.5m_11","wc2.0_bio_2.5m_12","wc2.0_bio_2.5m_13","wc2.0_bio_2.5m_14","wc2.0_bio_2.5m_15","wc2.0_bio_2.5m_16","wc2.0_bio_2.5m_17","wc2.0_bio_2.5m_18","wc2.0_bio_2.5m_19","topo_elevation","sunrad_ave","biome","NAM","EU","EAS") 
    trainVaribles = c("Q95_DegreeZeroToFrostZero","Distance_to_Sea","wc2.0_bio_2.5m_01","wc2.0_bio_2.5m_02","wc2.0_bio_2.5m_07","wc2.0_bio_2.5m_12","wc2.0_bio_2.5m_15","topo_elevation","sunrad_ave","Easting")
    rawDataFrame = subset(rawDataFrame,select=(c(dependentVar,trainVaribles))) 
    rawDataFrame = na.omit(rawDataFrame)
    # load and transfer the data frame into h2o object
    localH2O = h2o.init(ip = 'localhost', port = portValue, nthreads= 10,max_mem_size = '80g') #portNumber[portOrder]
    TrainData.hex = as.h2o(rawDataFrame, destination_frame = "TrainData.hex")
    RF_Parameters = list(ntrees = seq(20,1000,20),
                         max_depth = seq(10,200,10),
                         sample_rate = 0.632,
                         mtries = c(seq(2,9,1)))
    RF_Search_Criteria = list(strategy = "RandomDiscrete", 
                              max_models = 50)
    
    # run the parameter grid
    RF_Grid = h2o.grid("randomForest", x = trainVaribles, y = c(dependentVar),
                       grid_id = "RF_Grid",
                       training_frame = TrainData.hex,
                       seed = 1000,
                       hyper_params = RF_Parameters,
                       search_criteria = RF_Search_Criteria)
    # get the parameter result
    RF_GridResult = h2o.getGrid(grid_id = "RF_Grid",
                                sort_by = "mse",
                                decreasing = F)
    # get the best model which is listed in the first row
    OptimizedModelParameters = as.data.frame(RF_GridResult@summary_table[1,])
    # get the top model
    TopFullModel = h2o.getModel(RF_GridResult@model_ids[[1]])
    # do the full prediction
    FullPrediction = h2o.predict(TopFullModel,TrainData.hex)
    # transform to data frame
    FullPredictionDF = as.data.frame(FullPrediction)
    # comine them into a data frame
    TrainAndPredictedDF = data.frame(Predict=FullPredictionDF$predict,Train=rawDataFrame[,c(get(dependentVar))])
    # allocate the coefficient to the data frame
    OptimizedModelParameters$Cor = cor(TrainAndPredictedDF$Predict,TrainAndPredictedDF$Train) 
    # add the r square to the data frame
    OptimizedModelParameters$R2 = summary(lm(TrainAndPredictedDF$Train~TrainAndPredictedDF$Predict))$r.squared
    # add the slop to the data frame
    OptimizedModelParameters$Slop = lm(TrainAndPredictedDF$Train~TrainAndPredictedDF$Predict)$coefficients[2]
    # write the opimized model parameters into the local folder
    write.csv(OptimizedModelParameters,paste("FrostModellingFolder/",dependentVar,"_Optimized_Full_Model_Parameter_and_Performance_biome_",biome,".csv",sep=""))
    # write the train and predict data frame into the local folder
    write.csv(TrainAndPredictedDF,paste("FrostModellingFolder/",dependentVar,"_Train_And_Predicted_Data_Frame_Full_Model_biome_",biome,".csv",sep=""))
    # initial the pdf file for the ploting
    pdf(paste("FrostModellingFolder/",dependentVar,"_Full_Model_Prediction_vs_Train_plot_biome_",biome,".pdf",sep=""))
    plotFull = ggplot(TrainAndPredictedDF, aes(x=Predict, y=Train)) +
                      stat_bin2d(bins=150) +
                      labs(x = "Predicted", y = "Training") +
                      coord_cartesian(xlim=c(min(TrainAndPredictedDF),max(TrainAndPredictedDF)), ylim=c(min(TrainAndPredictedDF),max(TrainAndPredictedDF)))+
                      stat_smooth(se=F, colour="yellow", size=0.5, method="lm") +
                      scale_fill_gradientn(colours = rev(rainbow(10)))+geom_abline(slope=1, intercept=0,na.rm = FALSE, show.legend = NA,  linetype="dashed")+
                      theme_bw()
    # shut down the the pdf generater
    plot(plotFull)
    dev.off()
    OptimizedModelParameters = fread(paste("FrostModellingFolder/",dependentVar,"_Optimized_Full_Model_Parameter_and_Performance_biome_",biome,".csv",sep=""))
    FullRandomForestModel = h2o.randomForest(x=trainVaribles, y=c(dependentVar),
     	                                     training_frame=TrainData.hex,
     	                                     ntrees=OptimizedModelParameters$ntrees,
     	                                     nfolds = 10,
     	                                     seed=1000,
     	                                     max_depth=OptimizedModelParameters$max_depth,
     	                                     mtries=OptimizedModelParameters$mtries,
     	                                     sample_rate=OptimizedModelParameters$sample_rate,
     	                                     keep_cross_validation_predictions=T)
    # get the crossvalidation predicts
    CrossPrediction = h2o.cross_validation_predictions(FullRandomForestModel)
    # trandsform the h2o data frame into data frame
    CrossPredictionList =lapply(CrossPrediction,as.data.frame)
    # bind the list into a data frame 
    CrossPredictionDF = do.call(cbind,CrossPredictionList)
    # summ each row to get the predicted values for each observation
    PredictColumn = apply(CrossPredictionDF,1,sum)
    # make the training data column and predicted column into a data frame
    CVTrainAndPredictedDF = data.frame(Predict=PredictColumn,Train=rawDataFrame[,c(get(dependentVar))])
    write.csv(CVTrainAndPredictedDF,paste("FrostModellingFolder/",dependentVar,"_Cross_Validation_Data_Frame_Train_VS_Predict_biome_",biome,".csv",sep=""))
    pdf(paste("FrostModellingFolder/",dependentVar,"_Cross_Validation_Model_Prediction_vs_Train_biome_",biome,".pdf",sep=""))
    plotFull = ggplot(CVTrainAndPredictedDF, aes(x=Predict, y=Train)) +
                                            stat_bin2d(bins=100) +
                                            labs(x = "Predicted", y = "Training") +
                                            coord_cartesian(xlim=c(min(CVTrainAndPredictedDF),max(CVTrainAndPredictedDF)), ylim=c(min(CVTrainAndPredictedDF),max(CVTrainAndPredictedDF)))+
                                            stat_smooth(se=F, colour="yellow", size=0.5, method="lm") +
                                            scale_fill_gradientn(colours = rev(rainbow(10)))+geom_abline(slope=1, intercept=0,na.rm = FALSE, show.legend = NA,  linetype="dashed")+
                                            theme_bw()
    plot(plotFull)
    # shut dwon the pdf machine
    dev.off()

    CrossValidationParameters = data.frame(Cor=cor(CVTrainAndPredictedDF$Predict,CVTrainAndPredictedDF$Train),
                                           R2=summary(lm(CVTrainAndPredictedDF$Train~CVTrainAndPredictedDF$Predict))$r.squared,
                                           Slop=lm(CVTrainAndPredictedDF$Train~CVTrainAndPredictedDF$Predict)$coefficients[2])
    #write the paramter data frame into the local folder
    write.csv(CrossValidationParameters,paste("FrostModellingFolder/",dependentVar,"_Parameter_for_Cross_Validation_Train_VS_Predict_biome_",biome,".csv",sep=""))

    # load the gloable enviroment varaible data frame
    gloabalVarialeTable = fread("Leafout_ML_data/Global_Environment_variables_data_frame.csv")[,-1]
    # subset the data frame by the target biome
    predictTable = gloabalVarialeTable[gloabalVarialeTable$WWF_Biomes_HalfDegree == biome,]
    predictTable.hex = as.h2o(predictTable,destination_frame = "predictTable.hex")
    perPrediction = h2o.predict(FullRandomForestModel,predictTable.hex)
    perPredictionDF = as.data.frame(perPrediction)
    # combine the predicted and coordinates
    perPredictionDF = data.frame(predictTable[,c("x","y")],Predict=perPredictionDF$predict)
    # initialize an empty raster
    startRaster = raster(nrows=280, ncols=690, xmn=-165, xmx=180, ymn=-60,ymx=80,crs="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",resolution=c(0.5, 0.5))
    # backgroud=0, is an option to set the na values in the data absence cells
    perBiomeRaster = rasterize(perPredictionDF[,c("x","y")], startRaster, perPredictionDF$Predict, fun=mean)
        # write the raster into local folder
    writeRaster(perBiomeRaster,paste("FrostModellingFolder/",dependentVar,"_ML_Predicted_Raster_for_biome_",biome,"_seed.tif",sep=""),overwrite=T)
    # shut donw the h2o port connection whichi is important for the initialization of the next job port connection
    h2o.shutdown(prompt = F)

    print(paste("--- the calculation of the dependent variable ",dependentVar," for the biome ",biome," is done ---"))
}


# # #*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*
# STEP 26 money calculation
# load the agriculture land map with 0.5 degree which has been aggregated to 0.5 degree after downloaded from the earthenv database
agricultureLand = raster("Agriculture_half_degree.tif")
# calucate the absolute value of the agriculture land area in each pixel
# pixel area 
pixelArea = area(agricultureLand)
# multply the pxiel area with the agriculture land propostion in each pxiel
absoluteAgricultureLand = pixelArea*agricultureLand/100
writeRaster(absoluteAgricultureLand,"FinancialCalculation/Absolute_Agriculture_Land_Area.tif",overwrite=T)
# load the country border map
countryBorder = shapefile("WORLD_BORDERS/TM_WORLD_BORDERS-0.3.shp")
# subset the polygons of all the countries we need to do analysis

# get the country level polygon
countryNames = c("Germany","France","Italy","Poland","Spain","Switzerland","Austria","Belgium","Netherlands")

for (cn in countryNames)
{
    # load the deggree days to the last frost 
    frostTypes = c("Zero","Four","Two")
    # load the degree data raster 
    degreeDayZeroStack = stack(paste("DegreedayToFrost/DegreeZeroToFrost",frostTypes,"/Degreeday_for_year_2017.tif",sep=""))
    degreeDayFiveStack = stack(paste("DegreedayToFrost/DegreeFiveToFrost",frostTypes,"/Degreeday_for_year_2017.tif",sep=""))
    # crop the rasters 
    absoluteAgricultureLand = raster("FinancialCalculation/Absolute_Agriculture_Land_Area.tif")
    cropAgricultureLand = crop(absoluteAgricultureLand,extent(degreeDayFiveStack))
    degreeDayStack = stack(degreeDayZeroStack,degreeDayFiveStack,cropAgricultureLand)
    # get the country polygon
    countryPolygon = countryBorder[countryBorder$NAME == cn,]
    # crop the raster by country ploygon
    cropedStack = crop(degreeDayStack,countryPolygon)
    # transfer the stack into a data frame
    perDataFrame = as.data.frame(cropedStack,na.rm=T)
    # change the names 
    names(perDataFrame) = c("degreeZeroFrostZero","degreeZeroFrostFour","degreeZeroFrostTwo","degreeFiveFrostZero","degreeFiveFrostFour","degreeFiveFrostTwo","AgricultureArea")
    perDataFrame$Country = cn
    write.csv(perDataFrame,paste("FinancialCalculation/CountryTables/",cn,"_Financial_table.csv",sep=""))
}

# merge all the tables in the country tables folder
tableList = list.files(path ="FinancialCalculation/CountryTables",pattern=".csv")
# rbind all those tables
rbindedTable = rbindlist(lapply(paste("FinancialCalculation/CountryTables/",tableList,sep=""),fread))
# write the data frame into the local folder
write.csv(rbindedTable,"FinancialCalculation/Countrys_Degreeday_Agriculture_Table.csv")
#
countryDegreeToFrostTable = fread("FinancialCalculation/Countrys_Degreeday_Agriculture_Table.csv")[,-1]
# load the money losing table
moneyLosingTable = fread("FinancialCalculation/FrostEconomicLosses2017.csv")
# this is the function to find the best threshold wich can privide the highes R2
degreeDayFrostTypes = c("degreeZeroFrostZero","degreeZeroFrostFour","degreeZeroFrostTwo","degreeFiveFrostZero","degreeFiveFrostFour","degreeFiveFrostTwo")
rSquareOptimizationFunc = function(dft)
{
    # subset the country degreeday to frost table
    perDataTypeTable = subset(countryDegreeToFrostTable,select=c(dft,"AgricultureArea","Country"))
    # 
    # generate a threshold vector of degree days
    degreeDaysThres = seq(0,1000, by=25)
    # generate a vector for loading the r square
    emptyVector = vector()
    for (ddt in degreeDaysThres)
    {
        # subset the perDataTypeTable by the threshold
        thresholdSubetTable = subset(perDataTypeTable,get(dft)>ddt,select=c(dft,"AgricultureArea","Country"))
        if(length(unique(thresholdSubetTable$Country))<6)
        {
            emptyVector = append(emptyVector,NA)
        }else
        {
            # aggregate the data frame by countries
            aggregatedTable = aggregate(x=thresholdSubetTable[,c("AgricultureArea")],by=thresholdSubetTable[,c("Country")],FUN=sum)
            # merge the moneylosing table witht the aggregated table
            mergedAgrMoneyTable = merge(aggregatedTable,moneyLosingTable,by="Country")
            # train the linear model
            rSquare = summary(lm(AgricultureArea~Lose,mergedAgrMoneyTable))$r.squared
            emptyVector = append(emptyVector,rSquare)
        }
    }
    # transfer this into a data frame
    rSquareTable = data.frame(rSqures = emptyVector,Threshold = degreeDaysThres,Type=dft)
    # add a column for the degree day threshold
    return(rSquareTable)
}

outputList = mclapply(degreeDayFrostTypes,rSquareOptimizationFunc,mc.cores=6)
rbindTable = rbindlist(outputList)
write.csv(rbindTable,"FinancialCalculation/Approach_1_r_Square_table.csv")

# 525 degreeday to frost for the Type "degreeZeroFrostZero"
dft="degreeZeroFrostZero"

perDataTypeTable = subset(countryDegreeToFrostTable,select=c(dft,"AgricultureArea","Country"))
# generate a threshold vector of degree days
ddt=525
# subset the perDataTypeTable by the threshold
thresholdSubetTable = subset(perDataTypeTable,get(dft)>ddt,select=c(dft,"AgricultureArea","Country"))
# aggregate the data frame by countries
aggregatedTable = aggregate(x=thresholdSubetTable[,c("AgricultureArea")],by=thresholdSubetTable[,c("Country")],FUN=sum)
# merge the moneylosing table witht the aggregated table
mergedAgrMoneyTable = merge(aggregatedTable,moneyLosingTable,by="Country")
write.csv(mergedAgrMoneyTable,"FinancialCalculation/degreeZeroFrostZero_degree_threshold_525_table.csv")
# train the linear model
rSquare = summary(lm(AgricultureArea~Lose,mergedAgrMoneyTable))$r.squared

degreeDayFrostTypes = c("degreeZeroFrostZero","degreeZeroFrostFour","degreeZeroFrostTwo","degreeFiveFrostZero","degreeFiveFrostFour","degreeFiveFrostTwo")
countryDegreeToFrostTable

sumAgricultureLand = aggregate(x=countryDegreeToFrostTable[,c("AgricultureArea")],by=countryDegreeToFrostTable[,c("Country")],FUN=sum)

meanDegreeDay = aggregate(x=countryDegreeToFrostTable[,c("degreeZeroFrostZero",)],by=countryDegreeToFrostTable[,c("Country")],FUN=mean)
mergedAgrMoneyTable = merge(meanDegreeDay,moneyLosingTable,by="Country")
mergedAgrMoneyTable = merge(mergedAgrMoneyTable,sumAgricultureLand,by="Country")
mergedAgrMoneyTable$moneyArea = mergedAgrMoneyTable$Lose/mergedAgrMoneyTable$AgricultureArea*100000000
# train the linear model
rSquare = summary(lm(degreeZeroFrostZero ~moneyArea,mergedAgrMoneyTable))$r.squared

  plotFull                 <- ggplot(mergedAgrMoneyTable, aes(x=moneyArea, y=degreeZeroFrostZero)) +
                                       stat_bin2d(bins=150) +
                                       labs(x = "moneyArea", y = "degreeZeroFrostZero") +
                                       stat_smooth(se=F, colour="yellow", size=0.5, method="lm") +
                                       scale_fill_gradientn(colours = rev(rainbow(10)))+geom_abline(slope=1, intercept=0,na.rm = FALSE, show.legend = NA,  linetype="dashed")+
                                       theme_bw()








yearVector = 1959:2016 
# load the paster years degree day stack
degreeDayStack = stack(paste("DegreedayToFrost/DegreeZeroToFrostZero/Degreeday_for_year_",yearVector,".tif",sep=""))
# get the mean of this stack
meanDegreedayRaster = mean(degreeDayStack)
# load the 2017 year degree day stack
latestRaster = raster(paste("DegreedayToFrost/DegreeZeroToFrostZero/Degreeday_for_year_2017.tif",sep=""))
# different raster
diffRaster = latestRaster-meanDegreedayRaster 
# change all the values smaller than 0 to 0
countryBorder = shapefile("WORLD_BORDERS/TM_WORLD_BORDERS-0.3.shp")
countryNames = c("Germany","France","Italy","Poland","Spain","Switzerland","Austria","Belgium","Netherlands")
moneyLosingTable = fread("FinancialCalculation/FrostEconomicLosses2017.csv")
for (ths in seq(0,500, by=5))
{
    diffRaster = latestRaster-meanDegreedayRaster 
    diffRaster[diffRaster<=0] = NA

    for (cn in countryNames)
    {
        # crop the rasters 
        absoluteAgricultureLand = raster("FinancialCalculation/Absolute_Agriculture_Land_Area.tif")
        cropAgricultureLand = crop(absoluteAgricultureLand,extent(diffRaster))
        degreeDayStack = stack(diffRaster,cropAgricultureLand)
        # get the country polygon
        countryPolygon = countryBorder[countryBorder$NAME == cn,]
        # crop the raster by country ploygon
        cropedStack = crop(degreeDayStack,countryPolygon)
        # transfer the stack into a data frame
        perDataFrame = as.data.frame(cropedStack,na.rm=T)

        if (nrow(perDataFrame!=0))
        {
            # sortPerDataFrame = perDataFrame[order(perDataFrame$layer),]                      
            # perDataFrameFiltered = sortPerDataFrame[ceiling(nrow(sortPerDataFrame)*ths):nrow(sortPerDataFrame),] 
            perDataFrameFiltered = perDataFrame[perDataFrame$layer >ths,]
            # change the names 
            names(perDataFrameFiltered) = c("degreeZeroFrostZero","AgricultureArea")
            perDataFrameFiltered$Country = cn
            write.csv(perDataFrameFiltered,paste("FinancialCalculation/CountryTables/",cn,"_Financial_table.csv",sep=""))
        }
    }

    # merge all the tables in the country tables folder
    tableList = list.files(path ="FinancialCalculation/CountryTables",pattern=".csv")
    # rbind all those tables
    rbindedTable = rbindlist(lapply(paste("FinancialCalculation/CountryTables/",tableList,sep=""),fread))
    # write the data frame into the local folder
    # write.csv(rbindedTable,"FinancialCalculation/Countrys_Degreeday_Agriculture_Table.csv")
    #

    aggregatedTable = aggregate(x=rbindedTable[,c("AgricultureArea")],by=rbindedTable[,c("Country")],FUN=sum)
    # merge the moneylosing table witht the aggregated table
    mergedAgrMoneyTable = merge(aggregatedTable,moneyLosingTable,by="Country")
    # train the linear model
    rSquare = summary(lm(AgricultureArea~Lose,mergedAgrMoneyTable))$r.squared
    print(paste("--with threshold ",ths," we can get the r square ",rSquare," . ---"))
}

meanDegreedayRaster[meanDegreedayRaster<=0] =NA

for (ths in seq(0,5, by=0.2))
{
    diffRaster = latestRaster/meanDegreedayRaster 
    diffRaster[diffRaster<=1] = NA

    for (cn in countryNames)
    {
        # crop the rasters 
        absoluteAgricultureLand = raster("FinancialCalculation/Absolute_Agriculture_Land_Area.tif")
        cropAgricultureLand = crop(absoluteAgricultureLand,extent(diffRaster))
        degreeDayStack = stack(diffRaster,cropAgricultureLand)
        # get the country polygon
        countryPolygon = countryBorder[countryBorder$NAME == cn,]
        # crop the raster by country ploygon
        cropedStack = crop(degreeDayStack,countryPolygon)
        # transfer the stack into a data frame
        perDataFrame = as.data.frame(cropedStack,na.rm=T)

        if (nrow(perDataFrame!=0))
        {                   
            perDataFrameFiltered = perDataFrame[perDataFrame$layer >ths,]
            # change the names 
            names(perDataFrameFiltered) = c("degreeZeroFrostZero","AgricultureArea")
            perDataFrameFiltered$Country = cn
            write.csv(perDataFrameFiltered,paste("FinancialCalculation/CountryTables/",cn,"_Financial_table.csv",sep=""))
        }
    }

    # merge all the tables in the country tables folder
    tableList = list.files(path ="FinancialCalculation/CountryTables",pattern=".csv")
    # rbind all those tables
    rbindedTable = rbindlist(lapply(paste("FinancialCalculation/CountryTables/",tableList,sep=""),fread))
    # write the data frame into the local folder
    # write.csv(rbindedTable,"FinancialCalculation/Countrys_Degreeday_Agriculture_Table.csv")
    #

    aggregatedTable = aggregate(x=rbindedTable[,c("AgricultureArea")],by=rbindedTable[,c("Country")],FUN=sum)
    # merge the moneylosing table witht the aggregated table
    mergedAgrMoneyTable = merge(aggregatedTable,moneyLosingTable,by="Country")
    # train the linear model
    rSquare = summary(lm(AgricultureArea~Lose,mergedAgrMoneyTable))$r.squared
    print(paste("--with threshold ",ths," we can get the r square ",rSquare," . ---"))
}


  plotFull                 <- ggplot(mergedAgrMoneyTable, aes(x=Lose, y=AgricultureArea)) +
                                       stat_bin2d(bins=150) +
                                       labs(x = "Lose", y = "area") +
                                       stat_smooth(se=F, colour="yellow", size=0.5, method="lm") +
                                       scale_fill_gradientn(colours = rev(rainbow(10)))+geom_abline(slope=1, intercept=0,na.rm = FALSE, show.legend = NA,  linetype="dashed")+
                                       theme_bw()
