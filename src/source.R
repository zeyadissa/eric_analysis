# Functions -------

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
                          filter_dat <- '.xls'
                        } else if(x == '/data-and-information/publications/statistical/estates-returns-information-collection/estates-returns-information-collection-eric-england-2014-15'){
                          filter_dat <- 'est-ret-info-col-2014-2015-dat.csv'
                        } else {
                          filter_dat <- 'site'
                        }
                        test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                       filter_dat)
                        test<-test[grepl('.csv|xls',test)]
                        #remove double counting
                        test<-test[!grepl('eric-201617',test)]
                        return(test)
                      })  %>%
  unlist(use.names=F)

  site_data <- lapply(site_links[1:9],
                    function(x){
                      if(grepl('xls',x) == TRUE){
                        output<-'crap in a bucket'
                      } else if(x %in% c('https://files.digital.nhs.uk/publicationimport/pub18xxx/pub18726/est-ret-info-col-2014-2015-dat.csv',
                                         "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-site-data.csv")){
                        output <- data.table::fread(x,skip=1)
                      } else{
                        output <- data.table::fread(x)
                      }
                      output <- output %>% 
                        janitor::clean_names() %>%
                        mutate(date = case_when(
                          x ==  "https://files.digital.nhs.uk/41/5787C9/ERIC%20-%202022_23%20-%20Site%20data.csv" ~ '2022/23',
                          x ==  "https://files.digital.nhs.uk/EE/7E330D/ERIC%20-%20202122%20-%20Site%20Data%20v3.csv" ~ '2021/22',
                          x == "https://files.digital.nhs.uk/0F/46F719/ERIC%20-%20202021%20-%20Site%20data%20v2.csv" ~ '2020/21',                 
                          x == "https://files.digital.nhs.uk/11/BC1043/ERIC%20-%20201920%20-%20SiteData%20-%20v2.csv" ~ '2019/20',                     
                          x == "https://files.digital.nhs.uk/63/ADBFFF/ERIC%20-%20201819%20-%20SiteData%20v4.csv" ~ '2018/19',                     
                          x == "https://files.digital.nhs.uk/A8/188D99/ERIC-201718-SiteData.csv" ~ '2017/18',
                          x == "https://files.digital.nhs.uk/F7/97436C/ERIC-201617-revalidation-Site%20Data-version2.csv" ~ '2016/17',
                          x == "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-site-data.csv" ~ '2015/16',
                          TRUE ~ '2014/15'
                        )) %>%
                        dplyr::rename(.,any_of(c(trust_code='organisation_code'))) %>%
                        select(date,
                               trust_code,
                               site_code,
                               site_type,
                               starts_with('cost_to_eradicate_high'),
                               starts_with('cost_to_eradicate_significant'),
                               starts_with('cost_to_eradicate_moderate'),
                               starts_with('cost_to_eradicate_low'))
                      return(output)
                    }) %>%
  data.table::rbindlist(fill=T) %>%
  tidyr::pivot_longer(cols=!c(date,trust_code,site_code,site_type),names_to='risk_category',values_to='cost') %>%
  dplyr::mutate(cost = as.numeric(gsub(",","",cost))) %>%
  drop_na() %>%
  dplyr::filter(cost!=0)

# Trust level data -------

trust_links <- sapply(eric_links,
                     function(x){
                       if(x == "/data-and-information/publications/statistical/estates-returns-information-collection/england-historical-data-files-1999-2000-to-2013-14"){
                         filter_dat <- '.xls'
                       } else if(x == '/data-and-information/publications/statistical/estates-returns-information-collection/estates-returns-information-collection-eric-england-2014-15'){
                         filter_dat <- 'est-ret-info-col-2014-2015-tru-lev'
                       } else {
                         filter_dat <- 'trust'
                       }
                       test<-GetLinks(paste0('https://digital.nhs.uk',x),
                                      filter_dat)
                       test<-test[grepl('.csv',test)]
                       #remove double counting
                       test<-test[!grepl('eric-201617',test)]
                       return(test)
                     })  %>%
  unlist(use.names=F)

trust_data <- lapply(trust_links[1:9],
                    function(x){
                      if(grepl('xls',x) == TRUE){
                        output<-'crap in a bucket'
                      } else if(x %in% c( "https://files.digital.nhs.uk/publicationimport/pub18xxx/pub18726/est-ret-info-col-2014-2015-tru-lev-dat-v2.csv",
                                          "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-trust-data.csv")){
                        output <- data.table::fread(x,skip=1)
                      } else{
                        output <- data.table::fread(x)
                      }
                      output <- output %>% 
                        janitor::clean_names() %>%
                        mutate(date = case_when(
                          x ==  "https://files.digital.nhs.uk/FB/BE3AC8/ERIC%20-%20202223%20-%20Trust%20data.csv" ~ '2022/23',
                          x ==  "https://files.digital.nhs.uk/08/84C46C/ERIC%20-%20202122%20-%20Trust%20data.csv"~ '2021/22',
                          x == "https://files.digital.nhs.uk/81/4A77B0/ERIC%20-%20202021%20-%20Trust%20data.csv" ~ '2020/21',                 
                          x == "https://files.digital.nhs.uk/84/07227E/ERIC%20-%20201920%20-%20TrustData.csv" ~ '2019/20',                     
                          x == "https://files.digital.nhs.uk/83/4AF81B/ERIC%20-%20201819%20-%20TrustData%20v4.csv" ~ '2018/19',                     
                          x == "https://files.digital.nhs.uk/1B/7C75CF/ERIC-201718-TrustData.csv"  ~ '2017/18',
                          x == "https://files.digital.nhs.uk/71/75349A/ERIC-201617-revalidation-Trust%20Data-version2.csv"   ~ '2016/17',
                          x == "https://files.digital.nhs.uk/publicationimport/pub21xxx/pub21992/est-ret-info-col-2015-2016-trust-data.csv" ~ '2015/16',
                          TRUE ~ '2014/15'
                        )) %>%
                        dplyr::rename(.,any_of(c(trust_code='organisation_code'))) %>%
                        select(date,
                               trust_code,
                               starts_with('investment'),
                               any_of(starts_with('capital_investment_for_maintaining'))) %>%
                        rename(.,any_of(c(capital_investment_for_maintaining_lifecycle_existing_buildings='investment')))
                      return(output)
                    }) %>%
  data.table::rbindlist(fill=T) %>%
  replace(is.na(.), 0) %>%
  dplyr::mutate(investment = as.numeric(
    gsub(",","",investment_to_reduce_backlog_maintenance)) +
      as.numeric(
        gsub(",","",capital_investment_for_maintaining_lifecycle_existing_buildings))) %>%
  dplyr::select(!c(starts_with('investment_to'),capital_investment_for_maintaining_lifecycle_existing_buildings)) %>%
  drop_na() %>%
  dplyr::filter(investment!=0)

# Merge -------

FINAL_data <- site_data %>%
  group_by(date,trust_code,risk_category) %>%
  summarise(cost = sum(cost,na.rm=T)) %>%
  ungroup()%>%
  pivot_wider(names_from=risk_category,values_from=cost) %>%
  left_join(.,trust_data,site_date,by=c('date','trust_code')) %>%
  group_by(date,trust_code)%>%
  summarise(low_risk = sum(cost_to_eradicate_low_risk_backlog,na.rm=T),
            moderate_risk = sum(cost_to_eradicate_moderate_risk_backlog,na.rm=T),
            high_risk = sum(cost_to_eradicate_high_risk_backlog,na.rm=T),
            sig_risk = sum(cost_to_eradicate_significant_risk_backlog,na.rm=T),
            investment = sum(investment,na.rm=T)) %>%
  replace(is.na(.), 0) %>%
  ungroup() %>%
  mutate(cost = low_risk + moderate_risk + high_risk + sig_risk) %>%
  group_by(trust_code) %>% 
  mutate(total = cost + lag(cost,n=1,order_by=date)) %>%
  mutate(growth = (total -lag(total,n=1,order_by=date))/lag(total,n=1,order_by=date),
         invest_growth = (investment -lag(investment,n=1,order_by=date))/lag(investment,n=1,order_by=date))
  
# Data -------

growth <- mean((baseline %>%
  filter(!date %in% c('2014/15','2015/16','2019/20')))$growth)

GetBacklog <- function(backlog,growth,investment,invest_growth,t){
  for(j in 0:t){
  result <- (backlog * (1+growth)^t) - (investment * (1+invest_growth)^t)
  }
  output <- data.frame('backlog'=result,'time'=j)
  return(output)
}

GetBacklog(backlog=13000,investment=800,growth=growth,t=30,invest_growth=0.03)
  
# FINAL -------


ggplot()+
  geom_line(data=FINAL_data %>% filter(!date %in% c('2014/15','2015/16')),aes(x=date,y=growth,group=trust_code),alpha=0.5) +
  geom_line(data=baseline%>% filter(!date %in% c('2014/15','2015/16')),aes(x=date,y=growth,group=1),col='red') +
  ylim(-1,1) +
  theme_bw()

baseline <- FINAL_data %>%
  group_by(date) %>%
  summarise(cost = mean(cost,na.rm=T),
            investment=sum(investment,na.rm=T),
            total = sum(total,na.rm=T)) %>%
  mutate(growth = (total -lag(total,n=1,order_by=date))/lag(total,n=1,order_by=date))


baseline_area <- FINAL_data %>%
  mutate(area_type = 
           case_when(area >= quantile(area,0.75) ~ 'Big',
                     area >= quantile(area,0.5) & area < quantile(area,0.75) ~ 'Med',
                     TRUE ~ 'Small')) %>%
  group_by(date,area_type) %>%
  summarise(cost = sum(cost,na.rm=T))

area_var <- FINAL_data %>%
  mutate(area_type = 
           case_when(area >= quantile(area,0.75) ~ 'Big',
                     area >= quantile(area,0.5) & area < quantile(area,0.75) ~ 'Med',
                     TRUE ~ 'Small')) %>%
  group_by(trust_code) %>%
  mutate(cv = sd(cost)/mean(cost)) %>%
  group_by(date,area_type) %>%
  summarise(cv = mean(cv,na.rm=T))
  
baseline_count <- FINAL_data %>%
  mutate(area_type = 
           case_when(area >= quantile(area,0.75) ~ 'Big',
                     area >= quantile(area,0.5) & area < quantile(area,0.75) ~ 'Med',
                     TRUE ~ 'Small')) %>%
  mutate(flag = case_when(abs(growth)> 1 ~ 1,
                          TRUE ~ 0)) %>%
  group_by(date,area_type) %>%
  summarise(odd = sum(flag,na.rm=T),
            count = n())%>%
  mutate(
            per = odd/count)
  

test <- FINAL_data %>%
  group_by(date) %>%
  summarise(cost = sum(cost),
            investment = sum(investment),
            rat = cost/investment)
  
ggplot()+
  geom_line(data = baseline_count,
            aes(x=date,y=per,group=area_type,col=area_type)) 

ggplot()+
  geom_line(data = baseline,
            aes(x=date,y=growth,group =1),col='red') +
  geom_line(data=FINAL_data,
            aes(x=date,y=growth,group=trust_code),alpha=0.1)+
  ylim(-1,1)


ggplot()+
  geom_line(data=baseline_area,
            aes(x=date,y=cost,group=area_type,col=area_type),alpha=1)

ggplot()+
  geom_line(data=area_var,
            aes(x=date,y=cv,group=area_type,col=area_type),alpha=1)

ggplot()+
  geom_line(data=FINAL_data %>% filter(trust_code==unique(FINAL_data$trust_code)[10]),
            aes(x=date,y=cost,group=1))

ggplot()+
  geom_line(data=FINAL_data,
            aes(x=date,y=cost,group=trust_code),
            alpha=0.1) +
  ylim(0,1e7)
