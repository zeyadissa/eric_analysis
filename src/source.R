# Functions -------

library(tidyverse)
library(data.table)
library(rvest)
library(stringr)

#Gets links from any single url; string matches
GetLinks <- function(url_name,string){
  files <- c()
  #this is inefficient and bad practice but it's a small vector.
  for(i in seq_along(url_name)){
    pg <- rvest::read_html(url_name[i])
    pg<-(rvest::html_attr(rvest::html_nodes(pg, "a"), "href"))
    files <- c(files,pg[grepl(string,pg,ignore.case = T)])
    files <- files %>% unique()
  }
  return(files)
}

#Read all csvs from urls; unz for zips
UnzipCSV <- function(files){
  #creates temp file to read in the data
  temp <- tempfile()
  download.file(files,temp)
  #This is needed because a zip file may have multiple files
  file_names <- unzip(temp,list=T)$Name
  data<- lapply(file_names,
                function(x){
                  da <- data.table::fread(unzip(temp,x))
                  #janitor to clean unruly names
                  names(da) <- names(da) %>% janitor::make_clean_names()  
                  return(da)
                })
  #unlink the temp file, important to do
  unlink(temp)
  data}

eric_url <- 'https://digital.nhs.uk/data-and-information/publications/statistical/estates-returns-information-collection'
eric_links <- GetLinks(eric_url,'estates-returns-information-collection/')

# Site level data -------

site_links <- sapply(eric_links,
                      function(x){
                        if(x == "/data-and-information/publications/statistical/estates-returns-information-collection/england-historical-data-files-1999-2000-to-2013-14"){
                          filter_dat <- '.xls|.XLS'
                        } else if(x == '/data-and-information/publications/statistical/estates-returns-information-collection/estates-returns-information-collection-eric-england-2014-15'){
                          filter_dat <- 'est-ret-info-col-2014-2015-dat.csv'
                        } else {
                          filter_dat <- 'site'
                        }
                        test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                       filter_dat)
                        test<-test[grepl('.csv|xls|XLS',test)]
                        #remove double counting
                        test<-test[!grepl('eric-201617',test)]
                        return(test)
                      })  %>%
  unlist(use.names=F)

  site_data <- lapply(site_links[1:19],
                    function(x){
                      if(grepl('xls|XLS',x) == TRUE){
                        temp = tempfile(fileext = ".xls")
                        download.file(x, destfile=temp, mode='wb',skip=2)
                        output <- readxl::read_xls(temp, sheet ='Site Data',skip=1)
                      } else if(x %in% c('https://files.digital.nhs.uk/publicationimport/pub18xxx/pub18726/est-ret-info-col-2014-2015-dat.csv',
                                         "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-site-data.csv")){
                        output <- data.table::fread(x,skip=1)
                      } else{
                        output <- data.table::fread(x)
                      }
                      output <- output %>% 
                        janitor::clean_names() %>%
                        dplyr::mutate(date=match(x,site_links)) %>%
                        dplyr::rename(.,any_of(c(trust_code='organisation_code'))) %>%
                        select(date,
                               trust_code,
                               site_code,
                               site_type,
                               contains(c('low','high','significant','moderate')) & contains('cost')& contains('backlog')) %>%
                        pivot_longer(cols=!c(date,trust_code,site_code,site_type),names_to='risk',values_to='cost') %>%
                        mutate(risk = str_extract(pattern='high|low|significant|moderate',risk))
                      return(output)
                    }) %>%
    data.table::rbindlist(fill=T) %>%
    dplyr::filter(trust_code != 'Trust Code') %>%
    dplyr::mutate(cost = as.numeric(gsub(",","",cost))) %>%
    drop_na() %>%
    dplyr::filter(cost!=0)

# Trust level data -------

trust_links <- sapply(eric_links,
                     function(x){
                       if(x == "/data-and-information/publications/statistical/estates-returns-information-collection/england-historical-data-files-1999-2000-to-2013-14"){
                         filter_dat <- '.xls|.XLS'
                         test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                        filter_dat)
                         test<-test[grepl('.csv|xls|XLS',test)]
                         #remove double counting
                         test<-test[!grepl('eric-201617',test)]
                       } else if(x == '/data-and-information/publications/statistical/estates-returns-information-collection/estates-returns-information-collection-eric-england-2014-15'){
                         filter_dat <- 'est-ret-info-col-2014-2015-tru-lev'
                         test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                        filter_dat)
                         test<-test[grepl('.csv',test)]
                         #remove double counting
                         test<-test[!grepl('eric-201617',test)]
                       } else {
                         filter_dat <- 'trust'
                         test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                        filter_dat)
                         test<-test[grepl('.csv',test)]
                         #remove double counting
                         test<-test[!grepl('eric-201617',test)]
                       }
                       return(test)
                     })  %>%
  unlist(use.names=F)

trust_data <- lapply(trust_links[1:19],
                    function(x){
                      if(grepl('xls|XLS',x) == TRUE){
                        temp = tempfile(fileext = ".xls")
                        download.file(x, destfile=temp, mode='wb',skip=2)
                        output <- readxl::read_xls(temp, sheet ='Trust Data',skip=1)
                      } else if(x %in% c( "https://files.digital.nhs.uk/publicationimport/pub18xxx/pub18726/est-ret-info-col-2014-2015-tru-lev-dat-v2.csv",
                                          "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-trust-data.csv")){
                        output <- data.table::fread(x,skip=1)
                      } else{
                        output <- data.table::fread(x)
                      }
                      output <- output %>% 
                        janitor::clean_names() %>%
                        dplyr::mutate(date=match(x,trust_links)) %>%
                        dplyr::rename(.,any_of(c(trust_code='organisation_code'))) %>%
                        select(date,
                               trust_code,
                               starts_with('investment'),
                               any_of(starts_with('capital_investment_for_maintaining'))) %>%
                        rename(.,any_of(c(capital_investment_for_maintaining_lifecycle_existing_buildings='investment')))
                      return(output)
                    }) %>%
  data.table::rbindlist(fill=T) %>%
  filter(trust_code != 'Trust Code') %>%
  dplyr::mutate(investment1 = as.numeric(gsub(",","",investment_to_reduce_backlog_maintenance)),
                investment2 = as.numeric(gsub(",","",capital_investment_for_maintaining_lifecycle_existing_buildings))) %>%
  replace(is.na(.), 0) %>%
  dplyr::mutate(investment = investment1+investment2)%>%
  dplyr::select(!c(investment1,investment2,starts_with('investment_to'),capital_investment_for_maintaining_lifecycle_existing_buildings)) %>%
  drop_na() %>%
  dplyr::filter(investment!=0)

# Merge -------

FINAL_data <- site_data %>%
  group_by(date,trust_code,risk) %>%
  summarise(cost = sum(cost,na.rm=T)) %>%
  ungroup()%>%
  pivot_wider(names_from=risk,values_from=cost) %>%
  left_join(.,trust_data,site_date,by=c('date','trust_code')) %>%
  mutate(date = 2023 - date) %>%
  group_by(date,trust_code)%>%
  summarise(low = sum(low,na.rm=T),
            moderate = sum(moderate,na.rm=T),
            high = sum(high,na.rm=T),
            significant = sum(significant,na.rm=T),
            investment = sum(investment,na.rm=T)) %>%
  replace(is.na(.), 0) %>%
  ungroup() %>%
  mutate(cost = low + high + moderate + significant) %>%
  group_by(trust_code) %>% 
  mutate(total = cost + lag(investment,n=1,order_by=date)) %>%
  mutate(total_growth = (total -lag(total,n=1,order_by=date))/lag(total,n=1,order_by=date),
         invest_growth = (investment - lag(investment,n=1,order_by=date))/lag(investment,n=1,order_by=date),
         cost_growth = (cost -lag(cost,n=1,order_by=date))/lag(cost,n=1,order_by=date))
  
baseline <- FINAL_data %>%
  group_by(date) %>%
  summarise(cost = sum(cost,na.rm=T),
            investment=sum(investment,na.rm=T),
            total = sum(total,na.rm=T)) %>%
  mutate(total_growth = (total -lag(total,n=1,order_by=date))/lag(total,n=1,order_by=date),
         invest_growth = (investment -lag(investment,n=1,order_by=date))/lag(investment,n=1,order_by=date),
         cost_growth = (cost -lag(cost,n=1,order_by=date))/lag(cost,n=1,order_by=date))

# Data -------

underlying_growth <- mean((baseline %>%
  filter(!date %in% c('2004','2005','2019')))$total_growth)

cost_growth <- mean((baseline %>%
                  filter(!date %in% c('2004','2005','2019')))$cost_growth)

GetBacklog <- function(backlog,growth,investment,invest_growth,t){
  result <- (backlog * (1+growth)^t) - (investment * (1+invest_growth)^t)
  return(result)
}

GetPolicy <- function(backlog,investment,growth,policy,t){
  result <- (((policy - (backlog * (1+growth)^t))/-investment)^(1/t))-1
  return(result)
}

GetBacklog(backlog=13000,investment=800,growth=growth,t=30,invest_growth=0.03)
GetPolicy(backlog=13000,investment=800,growth=growth,t=10,policy=10000)
  
# EDA / Plots -------

# Baseline growth
ggplot()+
  geom_line(data=FINAL_data %>% filter(!date %in% c('2004','2005')),aes(x=date,y=total_growth,group=trust_code),alpha=0.1) +
  geom_line(data=baseline%>% filter(!date %in% c('2004','2005')),aes(x=date,y=total_growth,group=1),lwd=1,col='red') +
  ylim(-1,1) +
  geom_hline(yintercept=0,linetype=2,lwd=1.5,col='black')+
  theme_bw()

# cost growth
ggplot()+
  geom_line(data=FINAL_data %>% filter(!date %in% c('2004','2005')),aes(x=date,y=cost_growth,group=trust_code),alpha=0.15) +
  geom_line(data=baseline%>% filter(!date %in% c('2004','2005')),aes(x=date,y=cost_growth,group=1),lwd=1,col='red') +
  ylim(-1,1) +
  theme_bw()

# cost growth
ggplot()+
  geom_function(fun=function(x){GetPolicy(backlog=11637,
                                          investment=615,
                                          growth=growth,
                                          t=15,
                                          policy=x)},col='green')+
  geom_function(fun=function(x){GetPolicy(backlog=11637,
                                          investment=615,
                                          growth=growth,
                                          t=10,
                                          policy=x)},col='blue')+
  geom_function(fun=function(x){GetPolicy(backlog=11637,
                                          investment=615,
                                          growth=growth,
                                          t=5,
                                          policy=x)},col='red')+
  xlim(0,11637) +
  xlab('Value backlog reduced to (£bn) by t years time') +
  ylab('Annual real growth rates needed to achieve backlog level') +
  theme_bw()
