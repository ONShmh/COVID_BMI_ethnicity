## this code: filters the cleaned_linked_census_vn2 for
#- valid BMI
#- linked to gpes
#- aged over 40 in 2020
#- samples: all of those who died before eos or were hosp for COVID, 1% of white british, and 10% of non-white
#- weights: white=100, nonwhite=10, hosp or died=1
# outputs /covid_bmi_narm2.RDS


library(dplyr)
library(sparklyr)
library(survival)
library(ggplot2)
library(forcats)
library(tidyverse)
library(stringr)
library(scales)
library(splines)
library(ggthemes)
library('ggsci')
library(modeest)
library(rms)
########################
#Set up the spark connection
#########################

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")


dir ="cen_dth_hes/COVID19_BMI"
r_white = 0.01
r_nonwhite = 0.1
out_dir= paste0(dir, "/Results")

eos_date = "2020-12-28" # set end of study date

## -----------------------##
##   SAMPLING             ##
## -----------------------##

## aged over 40 and valid bmi ##
df_ext <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cleaned_linked_census_vn2")%>%
mutate(death=ifelse(is.na(dod),0,1))%>%
filter(linked_gpes == 1, age_2020 >=40) %>%
filter(isNotNull(bmi))


hosp_df <- sdf_sql(sc,"SELECT * FROM  cen_dth_gps.analytical_COVID_hospitalisation_and_deaths_20210908") %>%
    filter(!is.na(census_person_id))%>%
        select(census_person_id, epistart_hes, COVID_HES_FLAG, conf_COVID_death = COVID_death)%>%
       distinct()

# data on hospitalisation
hosp <- hosp_df %>%
  mutate(hosp_date =substr(epistart_hes  ,1,10)) %>%
  filter( hosp_date >=  "2020-01-24" & hosp_date <= eos_date)%>%
  select(census_person_id, COVID_HES_FLAG, hosp_date)

# confimed covid
covid <- hosp_df %>%
        filter(conf_COVID_death  == 1)%>%
select(census_person_id, conf_COVID_death )
       

df_ext <- df_ext %>%
          left_join(hosp, by="census_person_id")%>%
         left_join(covid, by="census_person_id")%>%
          mutate(conf_COVID_death = ifelse(conf_COVID_death == 1 & death_covid == 1 ,1, 0),
                 hosp_covid = ifelse(!is.na(hosp_date),1,0))
                 

df_ext %>% group_by(hosp_covid)%>% count()

### select all people who died in the outcome period

died <- df_ext %>%
  filter(death ==1 |hosp_covid == 1) %>%
  mutate(w_ethnicity = 1,w=1, sample = "Died") %>%
  collect()


### random sample of white people who were still alive at end of study
gc()
white <- df_ext %>% 
  filter(death == 0 & hosp_covid == 0 & ethnicity == "White British") %>%
  sdf_sample(fraction=r_white, seed=11235, replacement=FALSE) %>%
  mutate(#w_ethnicity = round(1/r_white * eth_wgt_rescaled), #commented out: bc no eth_wgt_rescaled
         w = 1/r_white,
         sample = "Alive, w")%>%
     collect()
gc()

### random sample of non-white people who were still alive at end of study
nonwhite <- df_ext %>% 
  filter(death == 0 & hosp_covid == 0& ethnicity!="White British") %>%
  sdf_sample(fraction=r_nonwhite, seed=11235, replacement=FALSE) %>%
  mutate(#w_ethnicity = round(1/r_nonwhite * eth_wgt_rescaled),
         w = 1/r_nonwhite,
         sample = "Alive, nw") %>%
  collect()
gc()
covid_df <- bind_rows(died, white, nonwhite) 

rm(died, white, nonwhite)
gc()



count_na <- function(x){sum(is.na(x))}

na_summary <- covid_df %>%
              summarise_all(count_na) %>%
              t()

## save
saveRDS(covid_df, "Data/covid_bmi_hosp.RDS"  ) 
 
