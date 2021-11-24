## this code: filters the cleaned_linked_census_vn2 for
#- valid BMI
#- linked to gpes
#- aged over 40 in 2020
#- samples: all of those who died before eos, 1% of white british, and 10% of non-white
#- weights: white=100, nonwhite=10, died=1
# outputs /covid_bmi_narm2.RDS

library(dplyr)
library(sparklyr)
library(survival)
library(ggplot2)
library(forcats)
library(tidyverse)
library(stringr)
library(scales)

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


########################
#directories and macros
#########################
dir ="cen_dth_hes/COVID19_ethnicity"
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

### select all people who died in the outcome period

died <- df_ext %>%
  filter(death ==1) %>%
  mutate(w_ethnicity = 1,w=1, sample = "Died") %>%
  collect()

### random sample of white people who were still alive at end of study
gc()
white <- df_ext %>% 
  filter(death == 0 & ethnicity=="White British") %>%
  sdf_sample(fraction=r_white, seed=11235, replacement=FALSE) %>%
  mutate(#w_ethnicity = round(1/r_white * eth_wgt_rescaled), #commented out: bc no eth_wgt_rescaled
         w = 1/r_white,
         sample = "Alive, w")%>%
     collect()
gc()

### random sample of non-white people who were still alive at end of study
nonwhite <- df_ext %>% 
  filter(death == 0 & ethnicity!="White British",
         ) %>%
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
saveRDS(covid_df, "Data/covid_bmi_narm2.RDS"  ) #Data/covid_bmi_narm2.RDS
 



