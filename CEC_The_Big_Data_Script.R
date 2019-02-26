# WRITTEN BY devtoobh, SUMMER 2018
#####
# THIS CODE NO LONGER WORKS FULLY AS CDEC CHANGED ITS WEBSITE, WEBSITE URL, AND HOW THEY ORGANIZE THEIR DATA
#####

library(data.table) # V 1.11.4
library(fitdistrplus)
library(lubridate)
library(plyr)
library(Rssa)
library(rvest)
library(zoo)

##########################################################################################################################################################################################################################################
#                                                                                                PART 1 - STUFF THAT YOU SELECT AND RUN                                                                                                  #    
##########################################################################################################################################################################################################################################
# NEW VERSION OF R SEEMS TO CAUSE PROBLEMS IN PULLING DATA, GIVES ERROR THAT 'File is empty: /var/folders/bx/zg9c8y7n2qxf4ybnfp5m5x040000gn/T//RtmpTuPyUk/filebb92bc31b0f"
# JUST RUN IT AGAIN UNTIL IT WORKS...

# Inputs:
# Make sure to edit the i min and max for the first for loop in webscrape.CDEC if the min/max longitudes and latitudes are changed
min.lon <- -120.1158515
max.lon <- -121.5761153
min.lat <- 38.14279922
max.lat <- 38.84412848

CDEC.url                <- read_html(paste0('https://cdec.water.ca.gov/cgi-progs/staSearch?sta=&sensor=211&dur=&active=&loc_chk=on&lon1=', max.lon,'&lon2=', min.lon,'&lat1=', min.lat,'&lat2=', max.lat,'&elev1=-5&elev2=99000&operator=%2B&display=sta'))
station.variables.list  <- webscrape.CDEC(CDEC.url, startyear = 2014, endyear = 2018) 


var.interest  <- 'PA' # ('PA', 'ATM', 'RH', 'TEMP', 'WD', 'WS', 'SR')
stations      <- as.matrix(station.variables.list[[var.interest]][,1])
temporal.type <- as.matrix(station.variables.list[[var.interest]][,7])

start.source.date <- 'no' 
if(start.source.date == 'yes'){
  start.date<- as.matrix(station.variables.list[[var.interest]][,8])
  }else{start.date <- '01/01/2000'} # CUSTOM DATE IF start.source.date != 'yes'

contain.event <- 'no' # 'yes' or 'no'
if(contain.event == 'no'){
  stations <- as.matrix(stations[-which(temporal.type == ' (event) ')])
  if(start.source.date == 'yes'){start.date <- as.matrix(start.date[-which(temporal.type == ' (event) ')])}
  temporal.type <- as.matrix(temporal.type[-which(temporal.type == ' (event) ')])}


# General inputs
skewremove.1 <- TRUE
SkR.max      <- 100
SkR.min      <- -1
startset     <- start.date
endset       <- '12/31/2017'
ssval        <- 124
  
# Precipitation  
espi.apply   <- TRUE
lagg.apply   <- FALSE
skewremove.2 <- TRUE  
skewvalmax   <- 4

# Temperature, Relative Humidity, Pressure
upper.threshold <- 5
lower.threshold <- 5
wind            <- 30
rm.outlier      <- TRUE
showplot        <- FALSE
L               <- 8000

# Wind speed, Wind direction
NAconcurrent <- TRUE
okayNAnumber <- 500

# Formatting inputs:
fold.loc   <- '/Users/vedbhoot/Desktop/processed_data_CDEC/Temp_K/'
f.names    <- list.files(path = fold.loc, pattern = "*.txt")
fold.loc.2 <- '/Users/vedbhoot/Desktop/processed_data_CDEC/wind/wind_direction/' # for wind direction only
f.names.2  <- list.files(path = fold.loc.2, pattern = "*.txt")
  
# Functions you run:
Process.Precip(var.interest, stations, temporal.type, skewremove.1, SkR.max, SkR.min, startset, 
               endset, espi.apply , lagg.apply, skewremove.2, skewvalmax, ssval )

Process.T.RH.P(var.interest, stations, temporal.type, skewremove.1, SkR.max, SkR.min, startset, 
               endset, upper.threshold, lower.threshold, window, rm.outlier, showplot, L)

Process.WD.WS(var.interest, stations, temporal.type, skewremove.1, SkR.max, SkR.min, startset, 
              endset, ssval, NAconcurrent, okayNAnumber)

fmt.meteodata(var.interest, file.names = f.names, folder.location = fold.loc, file.names.2 = f.names.2,
              folder.location.2 = fold.loc.2, espi = espi.apply, espi.lagg = lagg.apply)

##########################################################################################################################################################################################################################################
#                                                                                                PART 2 - FUNCTIONS FOR RUNNING - CDEC                                                                                                   #    
##########################################################################################################################################################################################################################################
Process.Precip <- function(var.interest, stations, temporal.type, skewremove.1 = FALSE, SkR.max = NA, SkR.min = NA, 
                              startset, endset, espi.apply = TRUE, lagg.apply = TRUE, skewremove.2 = FALSE, skewvalmax, ssval){
    for(i in 1:nrow(stations)){
      if(temporal.type[i,1] == ' (hourly) '){
        if(length(startset) == 1){
          date.adj <- date.format(startset)
        }else{date.adj <- date.format(startset[i])}
        
        tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1] 
                                ,'&sensor_num=2&start_date=', date.adj,'+23:00&end_date=', endset,'+23:00&dur_code=H&download=y')))
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        if(length(startset) > 1){station <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset, endset)}

        stationfixed.PA <- CFI.station(station, espi.apply, lagg.apply, skewremove.2, skewvalmax, ssval)
        if(espi.apply == TRUE & lagg.apply == TRUE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed_lagged_ESPI.txt'))}
        if(espi.apply == TRUE & lagg.apply == FALSE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed_ESPI.txt'))}
        if(espi.apply == FALSE & lagg.apply == FALSE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed.txt'))}
        print(paste0(stations[i,1], ' has finished processing and been exported.'))
      }else{
        if(length(startset) >1){tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                   '&sensor_num=2&start_date=', startset[i], '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))
        }else{tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                '&sensor_num=2&start_date=', startset, '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))}
        
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        
        if(length(startset) > 1){station <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset, endset)}
        stationfixed.PA <- CFI.station(station, espi.apply, lagg.apply, skewremove.2, skewvalmax, ssval)
        
        if(espi.apply == TRUE & lagg.apply == TRUE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed_lagged_ESPI.txt'))}
        if(espi.apply == TRUE & lagg.apply == FALSE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed_ESPI.txt'))}
        if(espi.apply == FALSE & lagg.apply == FALSE){write.table(stationfixed.PA, paste0(stations[i,1], '_hourly_', var.interest,'_processed.txt'))}
        print(paste0(stations[i,1], ' has finished processing and been exported.'))
      }}
  }
Process.T.RH.P <- function(var.interest, stations, temporal.type, skewremove.1 = FALSE, SkR.max = NA, SkR.min = NA, 
                           startset, endset, upper.threshold, lower.threshold, window, rm.outlier = TRUE, showplot = FALSE, L){
    is.RH <- FALSE
    if(var.interest == 'ATM'){s.number <- 17};if(var.interest == 'TEMP'){s.number <- 4};if(var.interest == 'RH'){s.number <- 12; is.RH <- TRUE}

    for(i in 1:nrow(stations)){
      if(temporal.type[i,1] == ' (hourly) '){
                if(length(start.date) == 1){
          date.adj <- date.format(startset)
        }else{date.adj <- date.format(startset[i])}
        
        tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                '&sensor_num=', s.number ,'&start_date=', date.adj,'+23:00&end_date=', endset,'+23:00&dur_code=H&download=y')))
        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        if(length(startset) > 1){station.OV <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.OV <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset, endset)}

        station.OV    <- MAD.outlier(station.OV, upper.threshold, lower.threshold, window, rm.outlier, showplot)
        station.OV    <- CF.station.OV(station.OV)
        station.OV    <- SSA.nafill(station.OV, L, is.RH)
        L.adj <- L
        while(length(which(is.na(station.OV[,6]))) != 0){
          print(paste0('For SSA, a window of ', L.adj, ' did not fill in well, ', L.adj/2, ' is being attempted...'))
          L.adj <- L.adj/2
          station.OV <- SSA.nafill(station.OV, L.adj, is.RH)
          print(paste0('A window of ', L.adj, ' results in ', length(which(is.na(station.OV[,6]))), " NA's"))
        }
        #station.OV           <- as.data.frame(station.OV[,-6])
        colnames(station.OV) <- c('V1', 'V2', 'V3', 'V4', 'V5', 'V6')
        
        write.table(station.OV, paste0(stations[i,1], '_hourly_', var.interest,'_processed.txt'))
        print(paste0(stations[i,1], " processed and exported, there are ", length(which(is.na(station.OV[,6]))), " NA's left."))
        
      }else{
        if(length(startset) >1){tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                                         '&sensor_num=', s.number,'&start_date=', startset[i], '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))
        }else{tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                       '&sensor_num=', s.number,'&start_date=', startset, '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))}

        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        
        if(length(startset) > 1){station.OV <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.OV <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset, endset)}
        station.OV    <- as.data.frame(MAD.outlier(station.OV, upper.threshold, lower.threshold, window, rm.outlier))
        station.OV    <- CF.station.OV(station.OV)
        station.OV    <- SSA.nafill(station.OV, L, is.RH)
        L.adj <- L
        while(length(which(is.na(station.OV[,6]))) != 0){
          print(paste0('For SSA, a window of ', L.adj, ' did not fill in well, ', L.adj/2, ' is being attempted...'))
          L.adj <- L.adj/2
          station.OV <- SSA.nafill(station.OV, L.adj, is.RH)
          print(paste0('A window of ', L.adj, ' results in ', length(which(is.na(station.OV[,6]))), " NA's"))
        }
        
        #station.OV           <- as.data.frame(station.OV[,-6])
        colnames(station.OV) <- c('V1', 'V2', 'V3', 'V4', 'V5', 'V6')
        
        write.table(station.OV, paste0(stations[i,1], '_hourly_', var.interest,'_processed.txt'))
        print(paste0(stations[i,1], " processed and exported, there are ", length(which(is.na(station.OV[,6]))), " NA's left."))
      }}
  }
Process.WD.WS  <- function(var.interest, stations, temporal.type, skewremove.1 = FALSE, SkR.max = NA, SkR.min = NA, 
                              startset, endset, ssval, NAconcurrent = TRUE, okayNAnumber){ # okayNAnumber is necessary if NAconcurrent = TRUE
  s.number.wd <- 10
  s.number.ws <- 9 
      for(i in 1:nrow(stations)){
      if(temporal.type[i,1] == ' (hourly) '){
                if(length(start.date) == 1){
          date.adj <- date.format(startset)
        }else{date.adj <- date.format(startset[i])}
        
        tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                '&sensor_num=', s.number.ws ,'&start_date=', date.adj,'+23:00&end_date=', endset,'+23:00&dur_code=H&download=y')))
        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        if(length(startset) > 1){station.WS <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.WS <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset, endset)}
        
        tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                '&sensor_num=', s.number.wd ,'&start_date=', date.adj,'+23:00&end_date=', endset,'+23:00&dur_code=H&download=y')))
        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        if(length(startset) > 1){station.WD <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.WD <- format.CDECdata(tempvar, formatv1 = FALSE, formatv2 = TRUE, skewremove.1, SkR.max, SkR.min, startset, endset)}
        
        fulldata <- MkvWind(station.WS, station.WD, ssval, NAconcurrent = TRUE, okayNAnumber) 
        
        station.WS[,6]  <- fulldata[,7]
        station.WD[,6]  <- fulldata[,6]
        station.WD$'V7' <- fulldata[,8] 
        
        write.table(station.WS, paste0(stations[i,1], '_hourly_WS_processed.txt'))
        write.table(station.WD, paste0(stations[i,1], '_hourly_WD_processed.txt'))
        print(paste0(stations[i,1], " processed and exported."))

      }else{

        if(length(startset) >1){tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                                         '&sensor_num=', s.number.wd, '&start_date=', startset[i], '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))
        }else{tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                       '&sensor_num=', s.number.wd, '&start_date=', startset, '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))}
        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        
        if(length(startset) > 1){station.WD <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.WD <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset, endset)}
        
        if(length(startset) >1){tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                                         '&sensor_num=', s.number.ws, '&start_date=', startset[i], '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))
        }else{tempvar <- suppressWarnings(fread(paste0('http://cdec.water.ca.gov/cgi-progs/selectQuery?station_id=', stations[i,1], 
                                                       '&sensor_num=', s.number.ws, '&start_date=', startset, '+00:00&end_date=', endset,'+23:00&dur_code=E&download=y')))}
        if(ncol(tempvar) < 3){print(paste0("Something went wrong with pulling the data for ", stations[i,1]));next}
        tempvar <- as.data.frame(tempvar[,c(2,3,6)])
        tempvar <- as.data.frame(tempvar[-nrow(tempvar),])
        
        if(length(startset) > 1){station.WS <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset[i], endset)
        }else{station.WS <- format.CDECdata(tempvar, formatv1 = TRUE, formatv2 = FALSE, skewremove.1, SkR.max, SkR.min, startset, endset)}

        fulldata <- MkvWind(station.WS, station.WD, ssval, NAconcurrent = TRUE, okayNAnumber) 
        
        station.WS[,6]  <- fulldata[,7]
        station.WD[,6]  <- fulldata[,6]
        station.WD$'V7' <- fulldata[,8]
        
        write.table(station.WS, paste0(stations[i,1], '_hourly_WS_processed.txt'))
        write.table(station.WD, paste0(stations[i,1], '_hourly_WD_processed.txt'))
        print(paste0(stations[i,1], " processed and exported."))
        }}
}  
fmt.meteodata <- function(meteo.var = c('PA', 'ATM', 'RH', 'TEMP', 'WS'), file.names, file.names.2, folder.location, folder.location.2, 
                          espi = FALSE, espi.lagged = FALSE){
  station.names <- substr(file.names, start = 1, stop = 3)
  if(meteo.var == 'PA'){
    if(espi == TRUE){
      for(i in 1:length(file.names)){
        data2fmt     <- read.table(paste0(folder.location, file.names[i]))
        data2fmt[,8] <- data2fmt[,8]/3600 # (mm/h)(1h/3600s)=(mm/s)
        data2fmt     <- data2fmt[,c(1:5,8)]
        
        monthvar <- data2fmt[1,2]
        dayvar   <- data2fmt[1,3]
        yearvar  <- data2fmt[1,4]         
        if(espi== TRUE & espi.lagged == TRUE){ write.table(data2fmt, paste0(station.names[i], '_precip_lagESPI_mms_', monthvar, dayvar, yearvar,'.txt'))}
        if(espi== TRUE & espi.lagged == FALSE){ write.table(data2fmt, paste0(station.names[i], '_precip_ESPI_mms_', monthvar, dayvar, yearvar,'.txt'))}
        print(paste0(station.names[i], ' precipitation formatted and exported.'))
      }
    }else{
      for(i in 1:length(file.names)){
        data2fmt     <- read.table(paste0(folder.location, file.names[i]))
        data2fmt[,6] <- data2fmt[,6]/3600 # (mm/h)(1h/3600s)=(mm/s)
        
        monthvar <- data2fmt[1,2]
        dayvar   <- data2fmt[1,3]
        yearvar  <- data2fmt[1,4] 
        
        write.table(data2fmt, paste0(station.names[i], '_precip_mms_', monthvar, dayvar, yearvar,'.txt'))
        print(paste0(station.names[i], ' precipitation formatted and exported.'))
      }}}
  if(meteo.var == 'ATM'){
    for(i in 1:length(file.names)){
      data2fmt     <- read.table(paste0(folder.location, file.names[i]))
      data2fmt[,6] <- data2fmt[,6] * 3386.39 # (inHg)(3386.39pa/1inHg) = (3386.39pa)
       
      monthvar <- data2fmt[1,2]
      dayvar   <- data2fmt[1,3]
      yearvar  <- data2fmt[1,4] 
        
      write.table(data2fmt, paste0(station.names[i], '_ATM_pascals_', monthvar, dayvar, yearvar,'.txt'))
      print(paste0(station.names[i], ' pressure formatted and exported.'))
    }}
  if(meteo.var == 'TEMP'){
    for(i in 1:length(file.names)){
      data2fmt     <- read.table(paste0(folder.location, file.names[i]))
      data2fmt[,6] <- (data2fmt[,6] + 459.67) * (5/9) # (T(°F) + 459.67) × 5/9
      
      monthvar <- data2fmt[1,2]
      dayvar   <- data2fmt[1,3]
      yearvar  <- data2fmt[1,4] 
        
      write.table(data2fmt, paste0(station.names[i], '_Temp_Kelvin_', monthvar, dayvar, yearvar,'.txt'))
      print(paste0(station.names[i], ' temperature formatted and exported.'))
    }}
  if(meteo.var == 'WS'){
    for(i in 1:length(file.names)){
      data2fmt.ws  <- read.table(paste0(folder.location, file.names[i]))
      data2fmt.wd  <- read.table(paste0(folder.location.2, file.names.2[i]))
      
      data2fmt.ws[,7] <- (data2fmt.ws[,6] * 0.44704) * cos(data2fmt.wd[,6] * (pi/180)) # mph(0.44704m/s/1mph) = (0.44704m/s)
      data2fmt.ws[,8] <- (data2fmt.ws[,6] * 0.44704) * sin(data2fmt.wd[,6] * (pi/180)) # 
      
      monthvar <- data2fmt.ws[1,2]
      dayvar   <- data2fmt.ws[1,3]
      yearvar  <- data2fmt.ws[1,4] 
      
      data2fmt.ws.u <- as.data.frame(data2fmt.ws[,c(1:5,7)])
      data2fmt.ws.v <- as.data.frame(data2fmt.ws[,c(1:5,8)])
        
      write.table(data2fmt.ws.u, paste0(station.names[i], '_WS_u_ms_', monthvar, dayvar, yearvar,'.txt'))
      write.table(data2fmt.ws.v, paste0(station.names[i], '_WS_v_ms_', monthvar, dayvar, yearvar,'.txt'))
      print(paste0(station.names[i], ' wind speeds, u and v, formatted and exported.'))
    }}
}

##########################################################################################################################################################################################################################################
#                                                                                                 PART 3a - DATA PULL AND FORMAT                                                                                                         #    
##########################################################################################################################################################################################################################################
webscrape.CDEC  <- function(CDEC.url, startyear, endyear){ #IF endyear IS CURRENT YEAR SET AS 'present'!

    var <- CDEC.url
    namevar <- list()
    for(i in 2:170){ # IF LOCATION IS CHANGED, THIS i SHOULD BE CHANGED FOR HOWEVER MANY STATIONS FOUND FOR THE AREA
      tempvar <- var %>%
        html_node(paste0("tr:nth-child(", i,") a")) %>%
        html_text()
      namevar[[i]] <- tempvar
    }
    namevar <- as.matrix(ldply(namevar, rbind))
    
    # THIS PORTION OBTAINS DETAILS OF THE STATION, I COULDN'T GET EXACT SO I JUST GOT A GIANT LIST OF ALL OF THEM
    stnvarlist <- list()
    fullvar    <- list()
    
    for(i in 1:nrow(namevar)){
      
      var <- read_html(paste0("http://cdec.water.ca.gov/dynamicapp/staMeta?station_id=",as.character(namevar[i,1])))
      
      tempvar1 <- c("'")
      
      stationvars1 <- as.matrix(as.matrix(var %>% html_nodes('td:nth-child(1), tr:nth-child(2), tb tr') %>% html_text())[-c(1:6)]) 
      whichvec.sv1 <- which(grepl(paste(tempvar1, collapse = "|"), stationvars1) == TRUE)
      if(length(whichvec.sv1 != 0)){stationvars1 <- stationvars1[-whichvec.sv1]}
      whichvec.sv1.date <- which(is.na(as.POSIXlt(stationvars1, format = '%m/%d/%Y')) == FALSE)
      if(length(whichvec.sv1.date) != 0){stationvars1 <- stationvars1[-whichvec.sv1.date]}  
          
      stationvars2 <- as.matrix(var %>% html_nodes('td:nth-child(3), tr:nth-child(2), tb tr') %>% html_text())[-c(1:6)]
      whichvec.sv2 <- which(grepl(paste(tempvar1, collapse = "|"), stationvars2) == TRUE)
      if(length(whichvec.sv2) != 0){stationvars2 <- stationvars2[-whichvec.sv2]}
      whichvec.sv2.date <- which(is.na(as.POSIXlt(stationvars2, format = '%m/%d/%Y')) == FALSE)
      if(length(whichvec.sv2.date) != 0){stationvars2 <- stationvars2[-whichvec.sv2.date]}
      
      stationvars3 <- as.matrix(var %>% html_nodes('td:nth-child(4), tr:nth-child(2), tb tr') %>% html_text())[-c(1:6)]
      whichvec.sv3 <- which(grepl(paste(tempvar1, collapse = "|"), stationvars3) == TRUE)
      if(length(whichvec.sv3) != 0){stationvars3 <- stationvars3[-whichvec.sv3]}
      whichvec.sv3.date <- which(is.na(as.POSIXlt(stationvars3, format = '%m/%d/%Y')) == FALSE)
      if(length(whichvec.sv3.date) != 0){stationvars3 <- stationvars3[-whichvec.sv3.date]}

      stationvars <- as.matrix(cbind(stationvars1, stationvars2, stationvars3))
      timeprd     <- as.matrix(as.matrix(var %>% html_nodes('td:nth-child(7), tr:nth-child(2), tb tr') %>% html_text())[-1])
      if(length(timeprd) == 0){next}
      
      whichvec.tprd <- which(grepl(' to ', timeprd) == FALSE)
      if(length(whichvec.tprd) != length(timeprd) & length(whichvec.tprd) != 0){timeprd <- as.matrix(timeprd[-whichvec.tprd])}
      
      whichvec.tprd2 <- which(grepl('\\.', timeprd) == TRUE)
      if(length(whichvec.tprd2) != 0){timeprd <- as.matrix(timeprd[-whichvec.tprd2])}
      
      for(j in 1:length(timeprd)){timeprd[j,] <- gsub('to ', '', as.character(timeprd[j,]))}
      timeprd     <- strsplit(timeprd, ' ')
      timeprd     <- as.data.frame(as.data.frame(ldply(timeprd, rbind))[,-1])

      stationvars <- as.data.frame(cbind(stationvars, timeprd))
      stationvars <- data.frame(lapply(stationvars, as.character), stringsAsFactors = FALSE)
      
      fullvar[[i]]    <- t(as.matrix(var %>% html_nodes('tr:nth-child(1) td , tr:nth-child(4) td') %>% html_text())[c(2,4,6,8)])
      stnvarlist[[i]] <- stationvars
    }
    
    # THIS LAST SECTION CHECKS IN THAT LIST WHICH ONES HAVE THE VARIABLES WE ARE INTERESTED IN
    all.variables <- list()
    for(v in c('PA', 'ATM', 'RH', 'TEMP', 'WD', 'WS', 'SR')){
      var.interest <- v
      goodstation     <- data.frame()
    goodstationvars <- data.frame()
    enddate.check <- stnvarlist
    for(i in 1:length(stnvarlist)){ #VARIABLES WE NEED ARE PA (2 | 16), ATM (17), RH (12), TEMP (4), WD (10), WS (9)
      if(is.null(enddate.check[[i]][which(enddate.check[[i]][,5] == 'present'),5]) == FALSE){
      enddate.check[[i]][which(enddate.check[[i]][,5] == 'present'),5] <- '01/01/2100'} # THIS IS FOR FORMATTING PURPOSES WHEN THE which() 
                                                                                        # STATEMENT CHECKS THE END YEAR (CONVERSION TO YEAR 
                                                                                        # FROM A DATE USING POSIXlt(). IF THE ENTRY IS NOT 
                                                                                        # IN THE CORERCT FORMAT OR IS A CHARACTER (IN THESE 
                                                                                        # CASES 'present') OR AS SUCH, IT WILL RESULT IN AN NA).
                                                                                        # SO WE SET IT AS A DATE THAT WILL SET IT AS TRUE SINCE
                                                                                        # 'present' IMPLIES CURRENT DATA.
      else{next}
      checkvar.pa   <- which((stnvarlist[[i]][,2] == as.character(2) | stnvarlist[[i]][,2] == as.character(16)) & 
                               (stnvarlist[[i]][,1] == 'PRECIPITATION, ACCUMULATED, INCHES') & (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') &
                               year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.atm  <- which((stnvarlist[[i]][,2] == as.character(17)) & (stnvarlist[[i]][,1] == 'ATMOSPHERIC PRESSURE, INCHES') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.rh   <- which((stnvarlist[[i]][,2] == as.character(12)) & (stnvarlist[[i]][,1] == 'RELATIVE HUMIDITY,   %') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.temp <- which((stnvarlist[[i]][,2] == as.character(4)) & (stnvarlist[[i]][,1] == 'TEMPERATURE, AIR, DEG F') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.wd   <- which((stnvarlist[[i]][,2] == as.character(10)) & (stnvarlist[[i]][,1] == 'WIND, DIRECTION, DEG') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.ws   <- which((stnvarlist[[i]][,2] == as.character(9)) & (stnvarlist[[i]][,1] == 'WIND, SPEED, MPH') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)
      checkvar.sr   <- which((stnvarlist[[i]][,2] == as.character(26)) & (stnvarlist[[i]][,1] == 'SOLAR RADIATION, W/M^2') & 
                               (stnvarlist[[i]][,3] == ' (hourly) ' | stnvarlist[[i]][,3] == ' (event) ') & year(as.POSIXlt(stnvarlist[[i]][,4], format = '%m/%d/%Y')) <= 
                               startyear & year(as.POSIXlt(enddate.check[[i]][,5], format = '%m/%d/%Y')) >= endyear)

      checkvar.list <- list()
      checkvar.list['PA'] <- length(checkvar.pa); checkvar.list['ATM'] <- length(checkvar.atm); checkvar.list['RH'] <- length(checkvar.rh)
      checkvar.list['TEMP'] <- length(checkvar.temp); checkvar.list['WD'] <- length(checkvar.wd); checkvar.list['WS'] <- length(checkvar.ws); checkvar.list['SR'] <- length(checkvar.sr)
      if(as.numeric(checkvar.list[var.interest]) > 0){ 
        goodstation <- rbind(goodstation, as.data.frame(fullvar[i]))
        if(checkvar.list[var.interest] > 1){
          for(l in 1:(as.numeric(checkvar.list[var.interest]) - 1)){ goodstation <- rbind(goodstation, as.data.frame(fullvar[i]))}
        }
        if(i != 1){
          if(var.interest == 'PA'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.pa,])}; if(var.interest == 'ATM'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.atm,])}
          if(var.interest == 'RH'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.rh,])}; if(var.interest == 'TEMP'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.temp,])}
          if(var.interest == 'WD'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.wd,])}; if(var.interest == 'WS'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.ws,])}
          if(var.interest == 'SR'){placervar <- as.data.frame(stnvarlist[[i]][checkvar.sr,])}
          names(placervar) <- names(goodstationvars)
        }
        goodstationvars <- rbind(goodstationvars, placervar) 
      }else{next}
    }
    goodstations           <- as.data.frame(cbind(goodstation, goodstationvars))
    if(nrow(goodstations) == 0 | ncol(goodstations) == 0){print(paste0('No good stations for: ', var.interest)); next}
    colnames(goodstations) <- c('Station', 'Elevation (ft)', 'Latitude', 'Longitude', 'Variable', 'Sensor #', 'Temporal Type', 'Earliest Collection', 'End Collection')
    rownames(goodstations) <- c(1:nrow(goodstations))
    all.variables[[v]] <- goodstations
    print(paste0('Stations for ', v, ' found.'))
    }
    return(all.variables)
}
format.CDECdata <- function(raw.CDEC.data, formatv1 = TRUE, formatv2 = FALSE, skewremove.1 = FALSE, SkR.max, SkR.min, startset, endset){ #startset/endset FORMAT IS 'month/day/Year i.e. 01/01/2000
  time1 <- Sys.time()
    if(formatv1 == formatv2){
      warning('formatv1 and formatv2 cannot be the same! 
            formatv1 is for hourly data,formatv2 is for event data!')
      stop()
    }
    ####################################
    #                                  #
    #       Pull and Format pt1        #
    #                                  #
    ####################################  
    if(formatv2 == TRUE){
      
      raw.CDEC.data[,3] <- sapply(raw.CDEC.data[,3],function(x) gsub("</td><td><a","",as.character(x)))
      
      start   <- as.Date(startset, format = '%m/%d/%Y')
      end     <- as.Date(endset, format = '%m/%d/%Y')
      theDate <- start
      
      fulltimevar <- list()
      i           <- 1
      while(theDate <= end){
        fulltimevar[[i]] <- theDate
        i                <- i + 1
        theDate          <- theDate + 1
      }
      
      fulltimelength <- nrow(fulltimevar)
      
      fulltimevar1 <- list()
      for(i in 1:length(fulltimevar)){
        monthvar <- as.matrix(month(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d')))
        monthvar <- rep(monthvar, 24)
        dayvar   <- as.matrix(day(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d')))
        dayvar   <- rep(dayvar, 24)
        yearvar  <- as.matrix(year(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d'))) 
        yearvar  <- rep(yearvar, 24)
        
        fulltimevar1[[i]] <- as.matrix(cbind(monthvar, dayvar, yearvar))
      }
      fulltimevar <- as.matrix(ldply(fulltimevar1, rbind))
      remove(fulltimevar1)
      
      oneto24  <- as.matrix(c(0:23))
      oneto24  <- as.matrix(rep(oneto24, nrow(fulltimevar)/24))
      daycount <- data.frame()
      for(i in 1:(nrow(fulltimevar)/24)){ 
        daycounttemp <- as.matrix(rep(i, 24))
        daycount <- rbind(daycount,daycounttemp)
      }
      fulltimevar <- as.matrix(cbind(daycount, fulltimevar, oneto24)) 
      
      raw.CDEC.data <- raw.CDEC.data[,-c(1,2)]
      raw.CDEC.data <- as.data.frame(cbind(fulltimevar, raw.CDEC.data))
      
      for(j in 1:ncol(raw.CDEC.data)){
        suppressWarnings(raw.CDEC.data[,j] <- as.numeric(levels(raw.CDEC.data[,j]))[raw.CDEC.data[,j]])
      }
      
      if(skewremove.1 == TRUE){
        whichvec.badval                <- which(raw.CDEC.data[,6] < SkR.min | raw.CDEC.data[,6] > SkR.max) # REMOVE ANY EXCEPTIONALLY SKEWED VALUES
        raw.CDEC.data[whichvec.badval,6] <- NA                                                  
      }
      
      colnames(raw.CDEC.data) <- c('V1', 'V2', 'V3', 'V4', 'V5', 'V6')
      time2                   <- Sys.time()
      
      print(paste('Total runtime for formatting went from: ', time1, ' to ', time2))
  
      return(raw.CDEC.data)
    }
    if(formatv1 == TRUE){
      
      raw.CDEC.data[,1] <- sapply(raw.CDEC.data[,1],function(x) gsub('nowrap>','',as.character(x)))
      raw.CDEC.data[,2] <- sapply(raw.CDEC.data[,2],function(x) gsub('</td><td','',as.character(x)))
      raw.CDEC.data[,3] <- sapply(raw.CDEC.data[,3],function(x) gsub("</td><td><a",'',as.character(x)))
      raw.CDEC.data[,2] <- sapply(raw.CDEC.data[,2],function(x) gsub(":",'.',as.character(x)))
      
      section2 <- list()
      for(i in 1:nrow(raw.CDEC.data)){
        section2[[i]] <- as.integer(raw.CDEC.data[i,2])
      }
      section2 <- as.data.frame(ldply(section2, rbind))
      section2 <- as.matrix(section2)
      
      section3 <- list()
      for(i in 1:nrow(raw.CDEC.data)){
        section3[[i]] <- as.numeric(raw.CDEC.data[i,3])
      }
      section3   <- as.data.frame(ldply(section3, rbind))
      section2.3 <- as.data.frame(cbind(section2, section3))
      section2.3 <- as.matrix(section2.3)
      
      locatorvar <- as.matrix(unique(raw.CDEC.data[,1]))
      monthvar   <- as.matrix(month(as.POSIXlt(raw.CDEC.data[,1], format = '%m/%d/%Y')))
      dayvar     <- as.matrix(day(as.POSIXlt(raw.CDEC.data[,1], format = '%m/%d/%Y')))
      yearvar    <- as.matrix(year(as.POSIXlt(raw.CDEC.data[,1], format = '%m/%d/%Y'))) - 2000   #THIS PART NEEDS TO BE EDITED IF DATASET IS BEFORE 2000
      timevar    <- as.matrix(cbind(monthvar, dayvar, yearvar, section2))
      
      locatorvar.1 <- as.matrix(unique(timevar))
      
      na.set       <- rep(NA, nrow(locatorvar.1))
      locatorvar.1 <- as.data.frame(cbind(locatorvar.1, na.set))
      i = 1
      j = 1
      while(i <= nrow(timevar)){
        if((i+60) < nrow(locatorvar.1)){
          whichvec.similar <- which(timevar[i:(i+60), 1] == timevar[i, 1] & timevar[i:(i+60), 2] == timevar[i, 2] & timevar[i:(i+60), 3] == timevar[i, 3] &
                                      timevar[i:(i+60), 4] == timevar[i, 4])
          if(length(whichvec.similar) == 1){
            locatorvar.1[j,5] <- section2.3[i:(i+60),2][whichvec.similar]
            j <- j + 1
            i <- i + 1
          }else{
            locatorvar.1[j,5] <- sum(section2.3[i:(i+60), 2][whichvec.similar])/length(section2.3[i:(i+60),2][whichvec.similar])
            j <- j + 1
            i <- i + length(whichvec.similar)
          }
        }else{
          whichvec.similar <- which(timevar[i:nrow(timevar), 1] == timevar[i, 1] & timevar[i:nrow(timevar), 2] == timevar[i, 2] & timevar[i:nrow(timevar), 3] == timevar[i, 3] &
                                      timevar[i:nrow(timevar), 4] == timevar[i, 4])
          if(length(whichvec.similar) == 1){
            locatorvar.1[j,5] <- section2.3[i:nrow(section2.3),2][whichvec.similar]
            j <- j + 1
            i <- i + 1
          }else{
            locatorvar.1[j,5] <- sum(section2.3[i:nrow(section2.3), 2][whichvec.similar])/length(section2.3[i:nrow(section2.3),2][whichvec.similar])
            j <- j + 1
            i <- i + length(whichvec.similar)
          }
        }
      }
      origset <- locatorvar.1
      
      ####################################
      #                                  #
      #       Set the time portion       #
      #                                  #
      ####################################
 
      start   <- as.Date(startset, format = '%m/%d/%Y') 
      end     <- as.Date(endset, format = '%m/%d/%Y') # 

      theDate <- start
      
      fulltimevar <- list()
      i           <- 1
      while(theDate <= end){
        fulltimevar[[i]] <- theDate
        i                <- i + 1
        theDate          <- theDate + 1
      }
      
      fulltimelength <- nrow(fulltimevar)
      
      fulltimevar1 <- list()
      for(i in 1:length(fulltimevar)){
        monthvar <- as.matrix(month(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d')))
        monthvar <- rep(monthvar, 24)
        dayvar   <- as.matrix(day(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d')))
        dayvar   <- rep(dayvar, 24)
        yearvar  <- as.matrix(year(as.POSIXlt(fulltimevar[[i]], format = '%Y-%m-%d'))) 
        yearvar  <- rep(yearvar, 24)
        
        fulltimevar1[[i]] <- as.matrix(cbind(monthvar, dayvar, yearvar))
      }
      fulltimevar <- as.matrix(ldply(fulltimevar1, rbind))
      remove(fulltimevar1)
      
      NAvar       <- rep(NA, nrow(fulltimevar))
      oneto24     <- as.matrix(c(0:23))
      oneto24     <- as.matrix(rep(oneto24, nrow(fulltimevar)/24))
      rowtrack    <- as.matrix(c(1:nrow(fulltimevar)))
      fulltimevar <- as.matrix(cbind(fulltimevar, oneto24, NAvar, rowtrack))  # SETS FULL SET OF NA FOR IMPUTTING THE DATA INTO THIS MATRIX SO TEMPORAL SCALE IS CONSISTENT
      
      whichvec.time <- data.frame()
      for(i in c(min(fulltimevar[,3]):max(fulltimevar[,3]))){
        fulltimevar.year <- fulltimevar[which(fulltimevar[,3] == i),]
        origset.year     <- origset[which(origset[,3] ==  i),]
        if(nrow(origset.year) == 0){
          if(i == max(fulltimevar[,3])){
            whichvec.time <- rbind(whichvec.time, fulltimevar.year)
            break
          }else{
            whichve.time <- rbind(whichvec.time, fulltimevar.year)
            next
          }
        }
        for(j in c(min(fulltimevar.year[,1]):max(fulltimevar.year[,1]))){
          fulltimevar.month <- fulltimevar.year[which(fulltimevar.year[,1] == j),]
          origset.month     <- origset.year[which(origset.year[,1] == j),]
          
          if(nrow(origset.month) == 0){
            whichvec.time <- rbind(whichvec.time, fulltimevar.month)
            next
          }
          for(k in c(min(fulltimevar.month[,2]):max(fulltimevar.month[,2]))){
            fulltimevar.day <- fulltimevar.month[which(fulltimevar.month[,2] == k),]
            origset.day     <- origset.month[which(origset.month[,2] == k),]
            if(nrow(origset.day) == 0){
              whichvec.time <- rbind(whichvec.time, fulltimevar.day)
              next
            }
            for(l in fulltimevar.day[,4]){
              whichvec.time.ftvd <- which(fulltimevar.day[,4] == l)
              whichvec.time.osd  <- which(origset.day[,4] == l)
              if(length(whichvec.time.osd) == 0){
                next
              }
              fulltimevar.day[whichvec.time.ftvd,5] <- origset.day[whichvec.time.osd,5]
            }
            whichvec.time <- rbind(whichvec.time, fulltimevar.day)
          }
        }
      }
      
      fulltimevar <- as.data.frame(whichvec.time)
      fulltimevar           <- fulltimevar[,c(6,1,2,3,4,5)]
      daycount <- data.frame()
      for(i in 1:(nrow(fulltimevar)/24)){ 
        daycounttemp <- as.matrix(rep(i, 24))
        daycount <- rbind(daycount,daycounttemp)
      }
      fulltimevar[,1] <- daycount
      
      colnames(fulltimevar) <- c('V1', 'V2', 'V3', 'V4', 'V5', 'V6')
      
      if(skewremove.1 == TRUE){
        whichvec.badval                <- which(fulltimevar[,6] < SkR.min | fulltimevar[,6] > SkR.max) # REMOVE ANY EXCEPTIONALLY SKEWED VALUES
        fulltimevar[whichvec.badval,5] <- NA                                                  
      }
      
      time2 <- Sys.time()
      print(paste('Total runtime for formatting went from: ', time1, ' to ', time2))
      return(fulltimevar)
    }
  }
date.format     <- function(the.date){
  the.date <- as.character(as.Date(the.date, format = '%m/%d/%Y') - 1)
  date.Y <- year(as.POSIXlt(the.date))
  date.d <- day(as.POSIXlt(the.date))
  if(date.d < 10){
    date.d <- paste0('0', date.d)
  }
  date.m <- month(as.POSIXlt(the.date))
  if(date.m < 10){
    date.m <- paste0('0', date.m)
  }
  
  adj.date <- paste0(date.m,'/',date.d,'/',date.Y)
  return(adj.date)
}

##########################################################################################################################################################################################################################################
#                                                                                                  PART 3b - PRECIPITATION PROCESSING                                                                                                    #    
##########################################################################################################################################################################################################################################
CFI.station    <- function(station, espi.apply = TRUE, lagg.apply = FALSE, skewremove.2 = TRUE, skewvalmax, ssval){
    
    ###################################################################################
    # FIRST WE NEED TO EXTRACT NA'S SO WE CAN ACTUALLY WORK WITH THE DATA WITH VALUES #
    ###################################################################################
    
    organizevec           <- c(1:nrow(station))
    station               <- as.data.frame(cbind(station, organizevec))
    whichvec.1            <- which(station[,6] < 0 | is.nan(station[,6]) == TRUE)
    station[whichvec.1,6] <- NA
    whichvec.na           <- which(is.na(station[,6]) == TRUE)
    stationtemp1          <- station[whichvec.na,]
    stationtemp2          <- station[-whichvec.na,]
    
    # NOW WE CAN WORK WITH stationtemp2 TO FIX DATA WITHOUT NEEDING TO WORRY ABOUT NA'S
    
    ####################################################
    # NEXT WE NEED TO TAKE CARE OF SINGULAR CASES OF 0 #
    ####################################################
    
    # WE NEED TO EXTRACT THE LOCATIONS FOR SINGULAR CASES OF 0
    whichvec.0     <- as.data.frame(stationtemp2[,6] == 0)                                            # SET T/F FOR 0
    whichvec.0.T   <- as.data.frame(which(whichvec.0 == TRUE))                                        # FIND ROWS WHICH ARE TRUE
    whichvec.0.TS  <- as.data.frame(whichvec.0.T - 1)                                                 # ADJUSTMENT VARIABLE
    checkvec.0     <- whichvec.0.T[1:(nrow(whichvec.0.T)-1),] - whichvec.0.TS[2:nrow(whichvec.0.TS),] # COMPARED VARIABLE
    whichvec.0.T.2 <- as.data.frame(checkvec.0 == 0)                                                  # WHICH IN COMPARED VARIABLE ARE 0, 0'S TELL US THAT THERE ARE CONSECUTIVE 0'S IN THE ORIGINAL DATA
    F.vec          <- c(FALSE)
    whichvec.0.T.2 <- as.data.frame(rbind(whichvec.0.T.2, F.vec))
    
    loc.sing.0      <- NA
    loc.sing.remove <- NA
    
    for(i in 1:nrow(whichvec.0.T.2)){
      if(i == 1){
        if(whichvec.0.T.2[i, ] == TRUE){
          next
        }else{
          loc.sing.0      <- rbind(loc.sing.0, whichvec.0.T[i,])
          loc.sing.remove <- rbind(loc.sing.remove, i)
        }
      }else{
        if((whichvec.0.T.2[i,] == FALSE) & (whichvec.0.T.2[i-1,] == FALSE)){
          loc.sing.0      <- rbind(loc.sing.0, whichvec.0.T[i,])
          loc.sing.remove <- rbind(loc.sing.remove, i)
        }else{
          next}}}
    
    loc.sing.0      <- as.matrix(loc.sing.0[-1])
    loc.sing.remove <- as.matrix(loc.sing.remove[-1])
    
    organizevec2 <- c(1:nrow(stationtemp2))
    stationtemp2 <- as.data.frame(cbind(stationtemp2, organizevec2)) 
    
    # NOW TO FIX THE SINGULAR CASES
    for(i in loc.sing.0){
      if((stationtemp2[i-1, 6] <= stationtemp2[i+1, 6]) | ((stationtemp2[i-1, 6] - stationtemp2[i+1, 6]) < 2)){ 
        
        stationtemp2[i, 6] <- stationtemp2[i-1, 6]
      }else{next}}
    
    ############################################
    # NOW WE NEED TO LOOK AT CONSECUTIVE ZEROS #
    ############################################

    whichvec.0.T.2[loc.sing.remove,] <- NA
    whichvec.0.T.2.F <- as.data.frame(which(whichvec.0.T.2 == FALSE)) 
    whichvec.0.T.2.T <- as.data.frame(which(whichvec.0.T.2 == TRUE))  
    
    for(i in 1:nrow(whichvec.0.T.2.F)){
      if(i == 1){
        which.itvl.vec <- as.matrix(which(whichvec.0.T.2.T < whichvec.0.T.2.F[i,]))
        intervalvec    <- as.matrix(cbind(whichvec.0.T.2.T[which.itvl.vec,], whichvec.0.T.2.F[i,]))
        max.itvl       <- max(whichvec.0.T[intervalvec,])
        min.itvl       <- min(whichvec.0.T[intervalvec,])
        if((stationtemp2[max.itvl+1,6] >= stationtemp2[min.itvl-1,6]) | ((stationtemp2[min.itvl-1,6]-stationtemp2[max.itvl+1,6]) < 2) ){
          stationtemp2[min.itvl:max.itvl,6] <- stationtemp2[min.itvl-1,6]
        }else{
          next
        }
      }else{
        which.itvl.vec <- as.matrix(which((whichvec.0.T.2.T < whichvec.0.T.2.F[i,]) &(whichvec.0.T.2.T > whichvec.0.T.2.F[i-1,])))
        intervalvec    <- as.matrix(cbind(whichvec.0.T.2.T[which.itvl.vec,], whichvec.0.T.2.F[i,]))
        max.itvl       <- max(whichvec.0.T[intervalvec,])
        min.itvl       <- min(whichvec.0.T[intervalvec,])
        if(((stationtemp2[max.itvl+1,6] >= stationtemp2[min.itvl-1,6]) | ((stationtemp2[min.itvl-1,6]-stationtemp2[max.itvl+1,6]) < 2)) & 
           (is.na(stationtemp2[max.itvl+1,6]) == FALSE)){
          stationtemp2[min.itvl:max.itvl,6] <- stationtemp2[min.itvl-1,6]
        }else{next}}}
    
    #########################################################
    # NOW TO FIX FUNKY IRREGULAR VALUES AND VALUE DECREASES #
    #########################################################
      for(i in 1:(nrow(stationtemp2)-2)){
      if(stationtemp2[i,6] == 0){
        next
      }else{
        if((stationtemp2[i+1,6] != 0) & (stationtemp2[i,6] > stationtemp2[i+1,6]) & 
           round(stationtemp2[(i+1),6], digits = 0) != 0 & (abs(stationtemp2[i+2,6]-stationtemp2[(i+1),6])/stationtemp2[i+2,6]) > 0.10){
          stationtemp2[i+1,6] <- stationtemp2[i,6]
        }else{
          if((stationtemp2[i+1,6] != 0) & (stationtemp2[i,6] < stationtemp2[i+1,6]) & 
             (stationtemp2[i+1,6] >= stationtemp2[i+2,6]) & (stationtemp2[i+1,6] > stationtemp2[i+3,6])){
            stationtemp2[i+1,6] <- stationtemp2[i,6]
          }else{next}}}}
    
    ###############################
    # REMOVING CUMULATIVE ASPECT  #
    ###############################
    regprecip <- list()
    for(i in 1:nrow(stationtemp2)){
      if(i == 1){
        regprecip[[i]] <- stationtemp2[i,6]
      }else{
        if((stationtemp2[i,6] - stationtemp2[i-1,6]) >= 0){
          if(stationtemp2[i, 6] != 0){
            regprecip[[i]] <- stationtemp2[i,6] - stationtemp2[i-1,6]
          }else{
            regprecip[[i]] <- stationtemp2[i,6]
          }
        }else{
          regprecip[[i]] <- stationtemp2[i,6]
        }}}
    regprecip        <- as.data.frame(ldply(regprecip, rbind))
    stationtemp2[,6] <- regprecip
    
   #################################################
    # LASTLY TO PLACE NA'S BACK INTO THE SAME PLACE #
    #################################################
    stationfixed      <- data.frame()
    stationtemp2      <- as.data.frame(stationtemp2[,-8])                  # 1
    stationfixed      <- as.data.frame(rbind(stationtemp2, stationtemp1))  # 2
    stationfixed      <- stationfixed[order(stationfixed$organizevec),]    # 3
    stationfixed      <- as.data.frame(stationfixed[,-7])                  # 4
    
    stationfixed <- impute.station(stationfixed, espi.apply, lagg.apply, skewremove.2, skewvalmax, ssval)
    
    return(stationfixed)
  }
impute.station <- function(stationfixed, espi.apply, lagg.apply, skewremove.2, skewvalmax, ssval){ 
    if(skewremove.2 == TRUE){
      bigval <- as.matrix(which(stationfixed[,6] > skewvalmax))
      stationfixed[bigval,6] <- NA
    }
    
    if(espi.apply == TRUE){
      stationfixed <- ESPI.implement(stationfixed, lagg.apply)
      
      stationtemp3                            <- as.matrix(stationfixed[,8])
      stationtemp3[which(stationtemp3 > 0),1] <- 1                                          # SETS ALL WET AS 1
      stationfixed[,8]                        <- stationfixed[,8] * 25.4                    # INCHES TO MM
      tracker                                 <- c(1:nrow(stationfixed))
      stationfixed                            <- as.data.frame(cbind(stationfixed, tracker))
      
      ####################################################################################
      # FOR LOOP TO IMPUTE THROUGH MONTH SETS AT A TIME (IE ALL JANUARYS, ALL FEBRUARYS) #
      ####################################################################################
      
      for(m in 1:12){
        whichvec.month <- which(stationfixed[,2] == m) 
        
        P11store     <- list()
        nval.p11     <- list()
        P12store     <- list()
        nval.p12     <- list()
        stationtemp4 <- as.data.frame(stationtemp3[whichvec.month,])
        
        # FOR LOOP TO PRODUCE TRANSITION MATRIX PROBABILITES 

        for(n in min(stationfixed[,4]):max(stationfixed[,4])){ # CHANGE THIS SO THAT YOU CAN HAVE DIFFERENT YEARLY LENGTHS OF DATA
          whichvec.year <- which(stationfixed[,4] == n)
          ######################################
          # P11 = P(dry|dry) and P21 = 1 - P11 #
          ######################################
          
          P11        <- 0
          whichvec.0 <- which(stationtemp4 == 0) # WHICH DRY
          
          for(i in whichvec.0){
            if(i == nrow(stationtemp4)){
              break
            }else{
              if(stationtemp4[i+1,] == 0 & is.na(stationtemp4[i+1,]) == FALSE){
                P11 <- P11 + 1
              }else{
                next}}}
          P11store[[n+1]] <- P11
          nval.p11[[n+1]] <- length(whichvec.0)
          ######################################
          # P12 = P(dry|wet) and P22 = 1 - P12 #
          ######################################
          
          P12          <- 0
          whichvec.1   <- which(stationtemp4 == 1) # WHICH WET
          
          for(i in whichvec.1){
            if(i == nrow(stationtemp4)){
              break
            }else{
              if(stationtemp4[i+1,] == 0 & is.na(stationtemp4[i+1,]) == FALSE){
                P12 <- P12 + 1
              }else{
                next}}}
          P12store[[n+1]] <- P12
          nval.p12[[n+1]] <- length(whichvec.1)
        }
        P11 <- sum(ldply(P11store, rbind))/sum(ldply(nval.p11, rbind))
        P12 <- sum(ldply(P12store, rbind))/sum(ldply(nval.p12, rbind))
        P21 <- 1 - P11
        P22 <- 1 - P12
        
        T.mtx <- matrix(c(P11, P12, P21, P22), ncol = 2, nrow = 2)
        print(paste0('Month ',m, ', P11:',round(P11, digits = 2),', P12:',round(P12, digits = 2),', P21:',round(P21, digits = 2),', P22:',round(P22, digits = 2)))
        
        ##########################################################################################################################
        # Methods and functions (Rainday & MarkovRain) from Acevedo (2013) Simulation of Ecological and Environmental Variables. #
        ##########################################################################################################################
        
        set.seed(ssval)
        
        # REMOVE 0'S
        stationfixed.partial <- as.data.frame(stationfixed[whichvec.month,])
        whichvec.0.part2     <- which(stationfixed.partial[,8] == 0)
        stationfixed.partial <- as.data.frame(stationfixed.partial[-c(whichvec.0.part2),])
        whichvec.na.part2    <- which(is.na(stationfixed.partial[,8]) == TRUE)
        stationfixed.partial <- as.data.frame(stationfixed.partial[-c(whichvec.na.part2),])
        stationfixed.partial <- as.matrix(stationfixed.partial[,8])
        
        rainVals2 <- as.vector(stationfixed.partial)
        if(length(rainVals2) <= 1 | length(rainVals2[which(round(rainVals2, digits = 3) == round(rainVals2[1], digits = 3))]) == length(rainVals2)){
          if(length(rainVals2) > 1){
            print(paste0('Input data (month = ', m,') is not good enough to predict shape/scale for Weibull PDF.')) 
            print('All rates are the same: '); print(rainVals2)
            print('Only that value will be used for predicted wet hours.')
            whichvec.na.lastsection <- which(is.na(stationfixed[whichvec.month,6]))
            for(q in 1:length(whichvec.na.part2)){
              if(stationfixed[whichvec.month,6][whichvec.na.lastsection[q]-1] > 0){
                rain.predict <- sample(c(1,0), 1, replace = TRUE, c(P22, P12))
                if(rain.predict == 1){
                  stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- rainVals2[1]
                }else{
                  stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- 0}
              }else{
                if(stationfixed[whichvec.month,6][whichvec.na.lastsection[q]-1] == 0){
                  rain.predict <- sample(c(1,0), 1, replace = TRUE, c(P21, P11))
                  if(rain.predict == 1){
                    stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- rainVals2[1]
                  }else{
                    stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- 0}}}
            }
          }else{
          print(paste0('You are missing too much data for month:',m,'. You cannot fit a distribution with that poor data! Does not make sense!!!!'))}
        }else{
          
          # FIT DISTRIBUTIONS
          fit_w  <- fitdist(rainVals2, "weibull")
          fit_g  <- fitdist(rainVals2, "gamma")
          
          # PLOTS OF DISTRIBUTION COMPARISONS AND SUCH
          #par(mfrow=c(2,2))
          #plot.legend <- c("Weibull", "gamma")
          #denscomp(list(fit_w, fit_g), legendtext = plot.legend)
          #cdfcomp (list(fit_w, fit_g), legendtext = plot.legend)
          #qqcomp  (list(fit_w, fit_g), legendtext = plot.legend)
          #ppcomp  (list(fit_w, fit_g), legendtext = plot.legend)
          
          # PARAMETER EXTRACTION FROM WEIBULL
          #summary(fit_w) 
          mu    = mean(rainVals2)
          std   = sd(rainVals2)
          shape = fit_w$estimate[1]
          
          
          amount.param <-list(mu, std, skew = NA, shape, model.pdf="w")                 # NO NEED TO WORRY ABOUT SKEW
          ndays        <- length(which(is.na(stationfixed[whichvec.month,8]) == TRUE))  # SIMULATE FOR ALL NA'S PRESENT
          P            <- matrix(c(P11, P12, P21, P22),ncol = 2,byrow = TRUE)           
          rainVals     <- MarkovRain(P, ndays, amount.param, plot.out = FALSE)[[1]]     # CREATE A SAMPLED RECORD
          rainVals     <- as.data.frame(rainVals)
          
          ######################
          # IMPUTATION PORTION #
          ######################
          
          station.holder <- stationfixed[whichvec.month,] 
          stationfixed   <- stationfixed[-whichvec.month,]
          whichvec.na    <- as.matrix(which(is.na(station.holder[,8] == TRUE)))
          
          for(i in 1:nrow(rainVals)){
            station.holder[whichvec.na[i,1],8] <- rainVals[i,]
          }
          
          station.holder[,8] <- station.holder[,8] * exp(station.holder[,7])
          stationfixed       <- as.data.frame(rbind(station.holder, stationfixed))
          stationfixed       <- stationfixed[order(stationfixed$tracker),]
        }
      }        
      stationfixed           <- as.data.frame(stationfixed[,-9])
      colnames(stationfixed) <- c('Day Count', 'Month', 'Day', 'Year', 'Hour', 'Original Rates', 'ESPI', 'Rates + Imputed Rates')
      
      return(stationfixed)
    }else{
      
      stationtemp3                            <- as.matrix(stationfixed[,6])
      stationtemp3[which(stationtemp3 > 0),1] <- 1 # SETS ALL WET AS 1
      stationfixed[,6]                        <- stationfixed[,6] * 25.4 # inches to mm
      tracker                                 <- c(1:nrow(stationfixed))
      stationfixed                            <- as.data.frame(cbind(stationfixed, tracker))
      
      ####################################################################################
      # FOR LOOP TO IMPUTE THROUGH MONTH SETS AT A TIME (IE ALL JANUARYS, ALL FEBRUARYS) #
      ####################################################################################
      
      for(m in 1:12){
        
        whichvec.month <- which(stationfixed[,2] == m)  
        
        P11store     <- list()
        nval.p11     <- list()
        P12store     <- list()
        nval.p12     <- list()
        stationtemp4 <- as.data.frame(stationtemp3[whichvec.month,])
        
        # FOR LOOP TO PRODUCE TRANSITION MATRIX PROBABILITES 
        
        for(n in min(stationfixed[,4]):max(stationfixed[,4])){
          whichvec.year <- which(stationfixed[,4] == n)
          
          ######################################
          # P11 = P(dry|dry) and P21 = 1 - P11 #
          ######################################
          P11        <- 0
          whichvec.0 <- which(stationtemp4 == 0) # WHICH DRY
          
          for(i in whichvec.0){
            if(i == nrow(stationtemp4)){
              break
            }else{
              if(stationtemp4[i+1,] == 0 & is.na(stationtemp4[i+1,]) == FALSE){
                P11 <- P11 + 1
              }else{
                next}}}
          
          P11store[[n+1]] <- P11
          nval.p11[[n+1]] <- length(whichvec.0)
          
          ######################################
          # P12 = P(dry|wet) and P22 = 1 - P12 #
          ######################################
          P12        <- 0
          whichvec.1 <- which(stationtemp4 == 1) # WHICH WET
          
          for(i in whichvec.1){
            if(i == nrow(stationtemp4)){
              break
            }else{
              if(stationtemp4[i+1,] == 0 & is.na(stationtemp4[i+1,]) == FALSE){
                P12 <- P12 + 1
              }else{
                next}}}
          P12store[[n+1]] <- P12
          nval.p12[[n+1]] <- length(whichvec.1) 
        }
        P11 <- sum(ldply(P11store, rbind))/sum(ldply(nval.p11, rbind))
        P12 <- sum(ldply(P12store, rbind))/sum(ldply(nval.p12, rbind))
        P21 <- 1 - P11
        P22 <- 1 - P12
        
        T.mtx <- matrix(c(P11, P12, P21, P22), ncol = 2, nrow = 2)
        print(paste0('Month ',m, ', P11:',round(P11, digits = 3),', P12:',round(P12, digits = 3),', P21:',round(P21, digits = 3),', P22:',round(P22, digits = 3)))
        
        ##########################################################################################################################
        # Methods and functions (Rainday & MarkovRain) from Acevedo (2013) Simulation of Ecological and Environmental Variables. #
        ##########################################################################################################################
        
        set.seed(ssval)
        
        # REMOVE 0'S
        stationfixed.partial <- as.data.frame(stationfixed[whichvec.month,])
        whichvec.0.part2     <- which(stationfixed.partial[,6] == 0)
        stationfixed.partial <- as.data.frame(stationfixed.partial[-c(whichvec.0.part2),])
        whichvec.na.part2    <- which(is.na(stationfixed.partial[,6]) == TRUE)
        stationfixed.partial <- as.data.frame(stationfixed.partial[-c(whichvec.na.part2),])
        stationfixed.partial <- as.matrix(stationfixed.partial[,6])
        
        rainVals2 <- as.vector(stationfixed.partial)
        if(length(rainVals2) <= 1 | length(rainVals2[which(round(rainVals2, digits = 3) == round(rainVals2[1], digits = 3))]) == length(rainVals2)){
          if(length(rainVals2) > 1){
            print(paste0('Input data (month = ', m,') is not good enough to predict shape/scale for Weibull PDF.')) 
            print('All rates are the same: '); print(rainVals2)
            print('Only that value will be used for predicted wet hours.')
            whichvec.na.lastsection <- which(is.na(stationfixed[whichvec.month,6]))
            for(q in 1:length(whichvec.na.part2)){
              if(stationfixed[whichvec.month,6][whichvec.na.lastsection[q]-1] > 0){
                rain.predict <- sample(c(1,0), 1, replace = TRUE, c(P22, P12))
                if(rain.predict == 1){
                  stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- rainVals2[1]
                }else{
                  stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- 0}
              }else{
                if(stationfixed[whichvec.month,6][whichvec.na.lastsection[q]-1] == 0){
                  rain.predict <- sample(c(1,0), 1, replace = TRUE, c(P21, P11))
                  if(rain.predict == 1){
                    stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- rainVals2[1]
                  }else{
                    stationfixed[whichvec.month,6][whichvec.na.lastsection[q]] <- 0}}}
            }
            }else{print(paste0('You are missing too much data for month: ',m,'. You cannot fit a distribution with that poor data! Does not make sense!!!!'))}
        }else{
          
          # FIT DISTRIBUTIONS
          fit_w  <- fitdist(rainVals2, "weibull")
          #fit_g  <- fitdist(rainVals2, "gamma")
          
          # PLOTS OF DISTRIBUTIONS AND SUCH
          #par(mfrow=c(2,2))
          #plot.legend <- c("Weibull", "gamma")
          #denscomp(list(fit_w, fit_g), legendtext = plot.legend)
          #cdfcomp (list(fit_w, fit_g), legendtext = plot.legend)
          #qqcomp  (list(fit_w, fit_g), legendtext = plot.legend)
          #ppcomp  (list(fit_w, fit_g), legendtext = plot.legend)
          
          # EXTRACT PARAMETERS FROM THE WEIBULL DISTRIBUTION
          #summary(fit_w) 
          mu    = mean(rainVals2)
          std   = sd(rainVals2)
          shape = fit_w$estimate[1]
          
          amount.param <-list(mu, std, skew = NA, shape, model.pdf="w")                # NO NEED TO WORRY ABOUT SKEW
          ndays        <- length(which(is.na(stationfixed[whichvec.month,6]) == TRUE)) #simulate for  all na's present
          P            <- matrix(c(P11, P12, P21, P22),ncol = 2,byrow = TRUE)          #Need to modify this for our rain record
          rainVals     <- MarkovRain(P, ndays, amount.param, plot.out = FALSE)[[1]]    #Create a fake record
          rainVals     <- as.data.frame(rainVals)
          
          ######################
          # IMPUTATION PORTION #
          ######################
          
          station.holder <- stationfixed[whichvec.month,]  
          stationfixed   <- stationfixed[-whichvec.month,]
          whichvec.na    <- as.matrix(which(is.na(station.holder[,6] == TRUE)))
          
          for(i in 1:nrow(rainVals)){
            station.holder[whichvec.na[i,1],6] <- rainVals[i,]
          }
          stationfixed <- as.data.frame(rbind(station.holder, stationfixed))
          stationfixed <- stationfixed[order(stationfixed$tracker),]
        }}
      
      stationfixed           <- as.data.frame(stationfixed[,-7])
      colnames(stationfixed) <- c('Day Count', 'Month', 'Day', 'Year', 'Hour', 'Rates + Imputed Rates')
      return(stationfixed)
    }
}
ESPI.implement <- function(stationfixed, lagg.apply){
    if(lagg.apply == TRUE){
      ESPI           <- as.matrix(read.table(url('http://eagle1.umd.edu/GPCP_ICDR/Data/ESPI.txt')))
      colnames(ESPI) <- as.character(ESPI[1,])
      ESPI           <- ESPI[-1,]
      ESPI           <- ESPI[241:456,]
      class(ESPI)    <- 'numeric'
      ESPI[,1]       <- as.matrix(ESPI[,1])
      ESPIval        <- data.frame()
      ESPI[,1]       <- as.matrix((ESPI[,1] + 1))
    }else{
      ESPI           <- as.matrix(read.table(url('http://eagle1.umd.edu/GPCP_ICDR/Data/ESPI.txt')))
      colnames(ESPI) <- as.character(ESPI[1,])
      ESPI           <- ESPI[-1,]
      ESPI           <- ESPI[253:473,]
      class(ESPI)    <- 'numeric'
      ESPI[,1]       <- as.matrix(ESPI[,1])
      ESPIval        <- data.frame()
    }
    
    for(i in 1:nrow(ESPI)){
      whichvec.year  <- as.matrix(which(stationfixed[,4] == ESPI[i,1]))
      whichvec.month <- as.matrix(which(stationfixed[whichvec.year,2] == ESPI[i,2]))
      ESPIval        <- rbind(ESPIval, as.matrix(rep(ESPI[i,3], nrow(whichvec.month))))
    }
    
    ESPIval          <- as.matrix(ESPIval)
    stationfixed     <- cbind(stationfixed, ESPIval)
    Transformed.data <- stationfixed[,6] * exp(-1*stationfixed[,7]) 
    stationfixed     <- as.matrix(cbind(stationfixed, Transformed.data))
    
    return(stationfixed)
  }
RainDay        <- function(param) {
    # mu,std, and skew of daily rain used for skew
    # shape for gamma, exp, and Weibull
    # model.pdf "s" skewed, "w" Weibull, "e" exponential, "g" gamma
    mu <- param[[1]]
    std <- param[[2]]
    skew <- param[[3]]
    shape <- param[[4]]
    model.pdf <- param[[5]]
    
    if (model.pdf == "e") {
      u <- runif(1, 0, 1)
      scale <- mu
      y <- scale * (-log(u))
    }
    if (model.pdf == "w") {
      u <- runif(1, 0, 1)
      scale <- mu/gamma(shape + 1)
      y <- scale * (-log(u))^shape
    }
    if (model.pdf == "g") {
      scale <- mu/shape
      y <- rgamma(1, scale, shape)
    }
    if (model.pdf == "s") {
      z <- rnorm(1, 0, 1)
      y <- mu + 2 * (std/skew) * ((((skew/6) * (z - skew/6) + 
                                      1))^3 - 1)
    }
    return(y)
  }# BY AVECEDO (2013)
MarkovRain     <- function(P, ndays, amount.param, plot.out = FALSE) {
    mu <- amount.param[[1]]
    x <- rep(0, ndays)
    wet <- 0
    y <- runif(1, 0, 1)
    if (y > 0.5) {
      x[1] <- RainDay(amount.param)
      wet <- wet + 1
    }
    
    for (i in 2:ndays) {
      y <- runif(1, 0, 1)
      if (x[i - 1] == 0) {
        if (y > P[1, 1]) 
          x[i] <- RainDay(amount.param)
      }
      else {
        if (y > P[1, 2]) 
          x[i] <- RainDay(amount.param)
      }
      if (x[i] > 0) 
        wet <- wet + 1
    }
    
    expec.wet.days <- ndays * P[2, 1]/(P[1, 2] + P[2, 1])
    expec.dry.days <- ndays - ndays * P[2, 1]/(P[1, 2] + P[2, 
                                                           1])
    dry <- ndays - wet
    rain.tot <- round(sum(x), 2)
    expec.rain.tot <- expec.wet.days * mu
    rain.avg = round(mean(x), 2)
    expec.avg <- expec.rain.tot/ndays
    rain.wet.avg = round(sum(x)/wet, 2)
    expec.wet.avg <- mu
    
    if (plot.out == TRUE) {
      mat <- matrix(1:2, 2, 1, byrow = TRUE)
      layout(mat, c(7, 7), c(3.5, 3.5), respect = TRUE)
      par(mar = c(4, 4, 1, 0.5), xaxs = "r", yaxs = "r")
      plot(x, type = "s", xlab = "Day", ylab = "Rain (mm/day)")
      Rain <- x
      hist(Rain, prob = TRUE, main = "", xlab = "Rain (mm/day)")
    }
    
    return(list(x = round(x, 2), wet.days = wet, expected.wet.days = expec.wet.days, 
                dry.days = dry, expected.dry.days = expec.dry.days, rain.tot = rain.tot, 
                expec.rain.tot = expec.rain.tot, rain.avg = rain.avg, 
                expected.rain.avg = expec.avg, rain.wet.avg = rain.wet.avg, 
                expected.wet.avg = expec.wet.avg))
  }# BY AVECEDO (2013)


##########################################################################################################################################################################################################################################
#                                                                                                  PART 3c - TEMP, RH, P(?) PROCESSING                                                                                                   #    
##########################################################################################################################################################################################################################################
MAD.outlier   <- function(station, upper.threshold, lower.threshold, window, rm.outlier = TRUE, showplot = FALSE){ 
    y <- station[,6]
    x <- c(1:length(y))
    
    window      <- 24*window             
    threshold   <- upper.threshold    
    threshold.1 <- lower.threshold  
    
    z      <- rollapply(zoo(y), window, function(x){
    na.var <- c(which(is.na(x) == TRUE))
    
    if(length(na.var) != 0){m = median(x[-na.var]); m + threshold * median(abs(x[-na.var] - m))}
    else{m = median(x); m + threshold * median(abs(x - m))}}, align="right")
    
    z.1    <- rollapply(zoo(y), window, function(x){
    na.var <- c(which(is.na(x) == TRUE))
    
    if(length(na.var) != 0){m = median(x[-na.var]); m - threshold * median(abs(x[-na.var] - m))}
    else{m = median(x); m - threshold.1 * median(abs(x - m))}}, align = "right")
    
    z   <- c(rep(z[-c(which(is.na(z) == TRUE))][1], window-1), z) 
    z.1 <- c(rep(z.1[-c(which(is.na(z.1) == TRUE))][1], window-1), z.1)
    
    outliers   <- which(y > z)   
    outliers.1 <- which(y < z.1) 
    
    if(showplot == TRUE){
    par(mfrow = c(3,1))
    plot(y, main = 'Original', type = 'l', col = '#E00000', ylab = 'Temperature', xlab = 'Time (Hour count)')
    plot(x, y, type="l", lwd=2, col="#E00000", ylim=c(min(y[-which(is.na(y))]), max(y[-which(is.na(y))])), 
         main = paste0('Window = 24*', window/24, ', upper threshold = ', threshold, ', lower threshold = ', 
                       threshold.1), ylab = 'Temperature', xlab = 'Time (Hour count)')
    lines(x, z, col="Gray")
    lines(x, z.1, col = 'Blue')
    points(x[outliers], y[outliers], pch=19)
    points(x[outliers.1], y[outliers.1], pch = 19)
    }
    if(rm.outlier == TRUE){
    tempvec <- y
    station[outliers, 6]   <- NA
    station[outliers.1, 6] <- NA
    }
    if(showplot == TRUE){
    plot(station[,6], main = 'Outliers removed', 
         type = 'l', col = '#E00000', ylab = 'Temperature', xlab = 'Time (Hour count)')
    }
    return(station)
}
CF.station.OV <- function(station){
  
    station.OV                                  <- station
    whichvec.1                                  <- as.data.frame(which(is.nan(station.OV[,6]) == TRUE | is.na(station.OV[,6]) == TRUE))
    station.OV[whichvec.1[1:nrow(whichvec.1),], 6] <- NA
    whichvec.shift.1                            <- (whichvec.1 - 1) 
    whichvec.shift.2                            <- as.data.frame(whichvec.shift.1[2:nrow(whichvec.shift.1),]) 
    whichvec.2                                  <- as.data.frame(whichvec.1[1:(nrow(whichvec.1)-1),]) 
    whichvec.TF                                 <- (whichvec.2 == whichvec.shift.2) 
    F.vec                                       <- c(FALSE) 
    whichvec.TF                                 <- as.data.frame(rbind(whichvec.TF, F.vec))
    whichvec.TF.F                               <- as.data.frame(which(whichvec.TF == FALSE))
    
    for(i in 2:nrow(whichvec.TF.F)){
      if(i == nrow(whichvec.TF.F)){ 
        if(whichvec.TF[whichvec.TF.F[i,]-1,] == FALSE){
          station.ss <- station.OV
          station.ss[whichvec.1[whichvec.TF.F[i,],],6] <- station[whichvec.1[whichvec.TF.F[i,],]-1,6]
          station.OV <- station.ss
          
        }else{next}
      }else{
        if(whichvec.TF[whichvec.TF.F[i,]-1,] == FALSE){
          station.ss <- station.OV
          station.ss[whichvec.1[whichvec.TF.F[i,],],6] <- station[whichvec.1[whichvec.TF.F[i,],]-1,6]
          station.OV <- station.ss
          
        }else{
          len.set <- length((whichvec.TF.F[i-1,]+1):whichvec.TF.F[i,])
          
          if(len.set < 24){
            station.ms <- station.OV
            len.set.adj <- len.set + 2
            interpvec   <- station.ms[(whichvec.1[(whichvec.TF.F[i-1,]+1),]-1):(whichvec.1[(whichvec.TF.F[i,]),]+1),6]
            interpvec   <- as.data.frame(na.approx(interpvec, na.rm = FALSE))
            station.ms[(whichvec.1[(whichvec.TF.F[i-1,]+1),]-1):(whichvec.1[(whichvec.TF.F[i,]),]+1),6] <- as.data.frame(interpvec)
            station.OV <- station.ms
            
          }else{
            station.ls <- station.OV
           
            if((station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],4]-1) > 0){ 
              whichvec.prevyr <- which(station.ls[,4] == (station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],4]-1) 
                                       & station.ls[,3] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],3] 
                                       & station.ls[,2] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],2] 
                                       & station.ls[,5] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],5])
            }else{ 
              whichvec.prevyr <- which(station.ls[,4] == (station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],4]+1) 
                                       & station.ls[,3] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],3] 
                                       & station.ls[,2] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],2] 
                                       & station.ls[,5] == station.ls[whichvec.1[whichvec.TF.F[i-1,]+1,],5])
            }
            
            station.ls[whichvec.1[(whichvec.TF.F[i-1,]+1):whichvec.TF.F[i,],],6] <- station.ls[whichvec.prevyr:(whichvec.prevyr + len.set - 1),6]
            station.OV <- station.ls
          }}}}
    
    print(paste('There are', length(which(is.na(station.OV[,6]) == TRUE)), "NA's left after LOCF+linear interpolation."))
    return(station.OV)
}   
SSA.nafill    <- function(station, L = (nrow(station) + 1) %/% 2, is.RH = FALSE){ 
    
    data6a  <- station
    ssadata <- data6a 
    indx    <- which(is.na(data6a[,6])) 
    
    s <- ssa(ssadata[,6], L) 
    g <- gapfill(s, groups = list(1:7))
    r <- reconstruct(s, groups = list(c(1,2,3, 4,5,6,7,8)))
    
    ssadata[,6][indx] <- g[indx] 
    
    if(is.RH == TRUE){ssadata[which(ssadata[,6] > 100),6] <- 100}
    if(is.RH == TRUE){ssadata[which(ssadata[,6] < 0),6] <- 0}
    
    return(ssadata)
  }
  

##########################################################################################################################################################################################################################################
#                                                                                              PART 3d - WIND DIRECTION + SPEED PROCESSING                                                                                               #    
##########################################################################################################################################################################################################################################     
MkvWind <- function(WSdata, WDdata, ssval, NAconcurrent = TRUE, okayNAnumber){
  
  set.seed(ssval)
  
  station.WS <- WSdata
  station.WD <- WDdata
  
  station.WD[which(is.nan(station.WD[,6]) | station.WD[,6] > 360 | station.WD[,6] < 0),6] <- NA
  station.WS[which(is.nan(station.WS[,6]) | station.WS[,6] == -9999 | station.WS[,6] > 100),6] <- NA
  if(NAconcurrent == TRUE){
    NA.WD.WS <- which(is.na(station.WS[,6]) & is.na(station.WD[,6]))
    NA.WD    <- length(which(is.na(as.matrix(station.WD[-NA.WD.WS,6]))))
    NA.WS    <- length(which(is.na(as.matrix(station.WS[-NA.WD.WS,6]))))
    
    controller.TF <- (NA.WS < okayNAnumber | NA.WD < okayNAnumber)
    if(NA.WS < okayNAnumber | NA.WD < okayNAnumber){
      station.WD[which(is.na(station.WS[,6])),6] <- NA 
      station.WS[which(is.na(station.WD[,6])),6] <- NA  
    }
  }
  if(NAconcurrent == FALSE){ controller.TF <- FALSE}
  #################
  # FIRST SECTION ###########################################
  # THIS SECTION ASSIGNS DIRECTIONS FOR THE OBSERVED ANGLES #
  ###########################################################
  
  dirs.1  <-unique(station.WS[,6]) 
  dirs.2  <- c("North","NNE","NE","ENE","East","ESE","SE","SSE","South","SSW","SW","WSW","West","WNW","NW","NNW")
  ang     <-seq(0,(360-22.5),by=22.5) #*pi/180 # removed pi/180 as WD is in degrees
  dirAng  <-data.frame(dirs.2,ang,x=cos(ang*pi/180),y=sin(ang*pi/180))
  dirAng2 <-data.frame(dirs=station.WD[,6],s=station.WS[,6])
  
  dirAng2$direction <- rep(NA, nrow(dirAng2))
  
  whichvec.dir             <- which(dirAng2[,1] > 348.75 & dirAng2[,1] <= 360 | dirAng2[,1] >= 0 & dirAng2[,1] < 11.25)
  dirAng2[whichvec.dir, 4] <- as.character(dirAng[1,1])
  
  for(i in 2:nrow(dirAng)){
    whichvec.dir                <- which(dirAng2[,1] > (dirAng[i,2]-11.25) & dirAng2[,1] < (dirAng[i,2]+11.25))
    dirAng2[c(whichvec.dir), 3] <- as.character(dirAng[i,1])
  }
  
  dirAng2 <- as.data.frame(cbind(station.WD[,1:5], dirAng2)) 
  
   
  
  #################
  # SECOND SECTION ###################################################
  # THIS SECTION IS FOR THE MARKOV CHAIN AND DISTRIBUTION IMPUTATION #
  ####################################################################
  processed.data <- dirAng2
  NA.pdata       <- length(which(is.na(processed.data[,7]))) + length(which(is.na(processed.data[,6])))
  NA.pdata.2     <- 1
  i.val          <- 1
  print(paste0("There are ", NA.pdata, " total NA's currently."))
  
  while(NA.pdata != NA.pdata.2){
    NA.pdata.2 <- NA.pdata[-1]
    NA.pdata <- length(which(is.na(processed.data[,7]))) + length(which(is.na(processed.data[,6])))
    
    for(m in 1:12){ 
      angle.P.list     <- list()
      dir.P.list       <- list()
      magnitude.P.list  <- vector('list')
      
      dirAng2.m         <- dirAng2[which(dirAng2[,2] == m),]
      dir.P.var.set     <- data.frame(cbind(dirs.2, c(rep(0, 16))))
      dir.P.var.set[,2] <- as.numeric(as.character(dir.P.var.set[,2]))
      dir.P.var.set[,1] <- as.character(dir.P.var.set[,1])
      
      for(n in 1:length(dirs.2)){ 
        i <- dirs.2[n]
        
        for(y in min(dirAng2[,4]):max(dirAng2[,4])){ 
          dirAng2.m.y           <- dirAng2.m[which(dirAng2.m[,4] == y),]
          dirAng2.m.y.dirs2.loc <- which(dirAng2.m.y[,8] == i)
          magnitude.P.list[[i]] <- c(magnitude.P.list[[i]], dirAng2.m.y[dirAng2.m.y.dirs2.loc,7])
          angle.P.list[[i]]     <- c(angle.P.list[[i]], dirAng2.m.y[dirAng2.m.y.dirs2.loc,6])
          
          if(length(dirAng2.m.y.dirs2.loc) > 0){ 
            for(j in 1:length(dirAng2.m.y.dirs2.loc)){
              if((j+1) < nrow(dirAng2.m.y) & is.na(dirAng2.m.y[(j+1),8]) == FALSE){ 
                dirvar                                              <- dirAng2.m.y[(dirAng2.m.y.dirs2.loc[j] + 1),8]
                dir.P.var.set[which(dir.P.var.set[,1] == dirvar),2] <- dir.P.var.set[which(as.character(dir.P.var.set[,1]) == dirvar),2] + 1
                
              }else{next}
            }}}
        
        if(sum(dir.P.var.set[,2] > 0)){
          dir.P.var.set[,2] <- dir.P.var.set[,2]/sum(dir.P.var.set[,2]) 
          dir.P.list[[i]]   <- dir.P.var.set[,2]
          
        }else{dir.P.list[[i]] <- dir.P.var.set[,2]}
      }
      transition.mtx           <- as.data.frame(ldply(dir.P.list, rbind))
      transition.mtx           <- as.data.frame(transition.mtx[,-1])
      transition.mtx           <- t(transition.mtx)
      colnames(transition.mtx) <- dirs.2
      rownames(transition.mtx) <- dirs.2
      
      weibullparam.list.mag <- list()
      densitycurve.angle    <- list()
      for(f in dirs.2){ 
        if(length(magnitude.P.list[[f]]) > 1){
          weibullparam.list.mag[[f]] <- magnitude.P.list[[f]][which(magnitude.P.list[[f]] > 0)]
          shape.est.mag              <- suppressWarnings(fitdistr(weibullparam.list.mag[[f]], 'weibull')[1])
          scale.est.mag              <- shape.est.mag$estimate[2]
          shape.est.mag              <- shape.est.mag$estimate[1]
          weibullparam.list.mag[[f]] <- c(shape.est.mag, scale.est.mag)
          
          densitycurve.angle[[f]] <- density(angle.P.list[[f]])
          
        }else{
          densitycurve.angle[[f]]    <- NA
          weibullparam.list.mag[[f]] <- NA
        }} 
      whichvec.year.na  <- which(is.na(dirAng2[,8]) & dirAng2[,2] == m)
      
      if(whichvec.year.na[1] == 1 & dirAng2[whichvec.year.na[1], 4] == min(dirAng2[,4])){whichvec.year.na <- whichvec.year.na[-1]}
      for(l in whichvec.year.na){
        if(is.na(processed.data[l-1,8])){next} 

        whichvec.dir        <- which(dirs.2 == processed.data[l-1,8]) 
        predict.dir         <- sample(dirs.2, size = 1, replace = FALSE, c(transition.mtx[,whichvec.dir])) 
        
        if(is.na(processed.data[l,8])){
          processed.data[l,8] <- predict.dir
          predict.angle       <- sample(densitycurve.angle[[predict.dir]]$x, 1, replace = TRUE, densitycurve.angle[[predict.dir]]$y)
          dirAng.loc.finder   <- which(dirAng[,1] == predict.dir)
          
          if(predict.dir == 'North'){
            good.angle.low  <- 0 + 11.25
            good.angle.high <- 360 - 11.25
            
            while((predict.angle >= good.angle.low & predict.angle <= good.angle.high)| predict.angle < 0 | predict.angle > 360){ 
              predict.angle <- sample(densitycurve.angle[[predict.dir]]$x, 1, replace = TRUE, densitycurve.angle[[predict.dir]]$y)
            }
          }else{
            good.angle.low  <- dirAng[dirAng.loc.finder,2] - 11.25
            good.angle.high <- dirAng[dirAng.loc.finder,2] + 11.25
            
            while(predict.angle <= good.angle.low | predict.angle >= good.angle.high){
              predict.angle <- sample(densitycurve.angle[[predict.dir]]$x, 1, replace = TRUE, densitycurve.angle[[predict.dir]]$y)
            }}
          processed.data[l,6] <- predict.angle
        }
        predict.dir <- processed.data[l,8]
        
        if(is.na(processed.data[l,7])){
          P.wind          <- length(which(magnitude.P.list[[predict.dir]] > 0))/length(magnitude.P.list[[predict.dir]]) 
          P.nowind        <- 1 - P.wind
          wind.probs      <- c(P.wind, P.nowind)
          wind.occurrence <- sample(c(1,0), size = 1, replace = TRUE, wind.probs)
          
          if(wind.occurrence == 0){
            processed.data[l,7] <- 0
          }else{
            processed.data[l,7] <- rweibull(1, shape = as.numeric(weibullparam.list.mag[[predict.dir]][1]), 
                                            scale = as.numeric(weibullparam.list.mag[[predict.dir]][2])) # Need to think about this, sample size 1 okay?
          }}}}
    NA.pdata.2 <- length(which(is.na(processed.data[,7]))) + length(which(is.na(processed.data[,6])))
    if(NA.pdata.2 != NA.pdata){print(paste0("There are now ", NA.pdata.2, " total NA's left."))}
    i.val <- i.val + 1
  }
  print(paste0('There are ', NA.pdata, " NA's remaining. Markov chain imputing has run through."))
  
  
  #################
  # THIRD SECTION ################################################
  # THIS SECTION TAKES CARE OF INITAL NA'S, IF ANY, THROUGH LOCF #
  ################################################################
  
  if(length(which(is.na(processed.data[,6] | processed.data[,7]))) > 0){ # LOCF portion
    print("LOCF processing for persistent initial NA's...")
    max.locatorvar <- max(which(is.na(processed.data[,6])))
    len.nadir      <- length(which(is.na(processed.data[,6])))
    
    if(max.locatorvar != len.nadir){
      print("NA's were not initial values. Markov imputing went wrong...")
      stop()
    }
    data.fill.dir                      <- processed.data[(max.locatorvar+1):(max.locatorvar + len.nadir),8]
    data.fill.angle                    <- processed.data[(max.locatorvar+1):(max.locatorvar + len.nadir),6]
    processed.data[1:max.locatorvar,8] <- data.fill.dir
    processed.data[1:max.locatorvar,6] <- data.fill.angle
    
    max.locatorvar <- max(which(is.na(processed.data[,7])))                          
    len.naspeed <- length(which(is.na(processed.data[,7])))
    
    if(max.locatorvar != len.naspeed){
      print("NA's were not initial values. Markov imputing went wrong...")
      stop()
    }
    data.fill.speed                    <- processed.data[(max.locatorvar+1):(max.locatorvar + len.naspeed),7]
    processed.data[1:max.locatorvar,7] <- data.fill.speed  
    
    NA.pdata <- length(which(is.na(processed.data[,7]))) + length(which(is.na(processed.data[,6])))
    print(paste0('There are ', NA.pdata, " NA's remaining."))
  }
  
  processed.data <- as.data.frame(processed.data[,1:8])
  return(processed.data)
}  
  
