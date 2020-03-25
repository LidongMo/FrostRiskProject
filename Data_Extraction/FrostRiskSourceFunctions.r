library(data.table)
library(parallel)
library(raster)

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

degreeDayZeroCalc = function(x)
{
    if (any(is.na(x))|x[1]==0)
    {
        return(NA)
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
        return(NA)
    }else
    {
        # subse the vec to 200 days
        dailyTempVect = x[2:(x[1])]
        # change all the values smaller than 0 to 0
        degreeDayVect = replace(dailyTempVect, dailyTempVect < 5, 0)
        # retrun the degree day accumulation
        return(sum(degreeDayVect))
    }
}