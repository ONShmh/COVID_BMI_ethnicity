## this code: filters the cleaned_linked_census_vn2 for
#- valid BMI
#- linked to gpes
#- aged over 40 in 2020
#- samples: all of those who died before eos, 1% of white british, and 10% of non-white
#- weights: white=100, nonwhite=10, died=1
# outputs /covid_bmi_narm2.RDS

library(dplyr)
#library(dbplyr)
library(sparklyr)
library(survival)
library(ggplot2)
library(forcats)
library(tidyverse)
library(splines)

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


dir ="cen_dth_gps/COVID19_BMI"
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
filter(linked_gpes == 1, age_2020 >=40)

### select all people who died in the outcome period

died <- df_ext %>%
#select(-census_person_id)%>%
  filter(death ==1) %>%
  mutate(w_ethnicity = 1,w=1, sample = "Died") %>%
#select(-eth_wgt_rescaled, -residence_2019,-care_home_pr19)%>% #this raised subset columns that don't eist error
  collect()

#died <- died %>%
#mutate(dod = as.Date(reg_stat_dod, format = "%Y%m%d") )

### random sample of white people who were still alive at end of study
gc()
white <- df_ext %>% 
#select(-census_person_id)%>%
  filter(death == 0 & ethnicity=="White British") %>%
  sdf_sample(fraction=r_white, seed=11235, replacement=FALSE) %>%
  mutate(#w_ethnicity = round(1/r_white * eth_wgt_rescaled), #commented out: bc no eth_wgt_rescaled
         w = 1/r_white,
         sample = "Alive, w")%>%
#select(-eth_wgt_rescaled, -residence_2019,-care_home_pr19) %>%
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
#select(-eth_wgt_rescaled, -residence_2019,-care_home_pr19)%>%
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
saveRDS(covid_df, "Data/covid_bmi_with_na.RDS"  ) #Data/covid_bmi_narm2.RDS


###############################################################
## Modelling of missingness                                  ##
###############################################################


covid_df <- readRDS( "Data/covid_bmi_with_na.RDS" )

covid_df <- covid_df %>%
            mutate(missing_bmi = ifelse(is.na(bmi),1,0))

# Model with no variables

fit0 <-  glm(missing_bmi ~  pr_census_region,
              data = covid_df, weight=w, family="binomial")

# Pseudo R2
pseudo_R2_0 =  1 - fit0$deviance/fit0$null.deviance
pseudo_R2_0

#AUC

auc0 = pROC::auc(covid_df$missing_bmi,predict(fit0, covid_df, type="response"))
auc0

rm(fit0)
gc()

fit1 <-  glm(missing_bmi ~ ethnicity_short+
              + poly(age_2020,3) + sex + pr_census_region,
              data = covid_df, weight=w, family="binomial")

# Pseudo R2
pseudo_R2_1 =  1 - fit1$deviance/fit1$null.deviance
pseudo_R2_1

#AUC

auc1 = pROC::auc(covid_df$missing_bmi,predict(fit1, covid_df, type="response"))
auc1

rm(fit1)
gc()

fit2 <-  glm(missing_bmi ~ ethnicity_short+
              + poly(age_2020,3) + sex + pr_census_region
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 + b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant,
              data = covid_df, weight=w, family="binomial")

# Pseudo R2
pseudo_R2_2 =  1 - fit2$deviance/fit2$null.deviance
pseudo_R2_2

#AUC

auc2 = pROC::auc(covid_df$missing_bmi,predict(fit2, covid_df, type="response"))

rm(fit2)
gc()
## adding Covid as predictor


fit3 <-  glm(missing_bmi ~ death_covid+  ethnicity_short+
              + poly(age_2020,3) + sex + pr_census_region
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 + b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant,
              data = covid_df, weight=w, family="binomial")

# Pseudo R2
pseudo_R2_3 =  1 - fit3$deviance/fit3$null.deviance
pseudo_R2_3

#AUC

auc3 = pROC::auc(covid_df$missing_bmi,predict(fit3, covid_df, type="response"))

res <- data.frame(model=c("Region", "+age, sex, eth", "+ all", "+covid death"), 
                  pseudo_R2 = c(pseudo_R2_0, pseudo_R2_1,pseudo_R2_2,pseudo_R2_3),
                 Auc = c(auc0, auc1, auc2, auc3))

write.csv(res, paste0(out_dir, "/bmi_metrics_missingness.csv"))