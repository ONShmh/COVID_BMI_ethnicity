# author: a.c.summerfield; v.nafilyan
# date: 03/2021; last edited 09/2021
# description: 
# reads in covid_bmi_hosp.RDS (created in sampling_hosp.R),
# fits coxph models for hospitalisation and confirmed covid 
# plots the HR for diffferent ethnicities
# creates a table of estimates HR, from which can read off the HR equivalent for different ethnicities



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

######################################
###             ANALYSIS           ###
######################################


covid_df <- readRDS("Data/covid_bmi_hosp.RDS" )
max(covid_df$hosp_date, na.rm=T)
covid_df <- covid_df %>% 
    mutate( date_hosp = ifelse( hosp_date <  "2020-01-24", "NA", hosp_date ),
      date_hosp = as.Date( date_hosp),
           hosp_covid = ifelse(is.na(hosp_covid), 0, hosp_covid),
          # hosp_covid = ifelse(hosp_date <  "2020-01-24", 0, hosp_covid),
           t_hosp = ifelse(!is.na(date_hosp), difftime(date_hosp, "2020-01-24", units = "days"),
                                            difftime(eos_date, "2020-01-24", units = "days")))


#restrict to 40+
covid_df <- covid_df %>% filter(age_2020 >= 40)

# cut 2.5%/97.5%
# this section removes nas from bmi - irrelevant if using the dataframe with na.rm already
covid_df <- covid_df%>%
         mutate(bmi_na=ifelse(is.na(bmi),1,0))


covid_df_bmi <- covid_df %>% filter(bmi_na==0) %>% arrange(bmi)

# ordanise 
# (have chosen not to make sgpuk11 or renal cat ordinal, bc not sure if necessarily equal distance between the levels within them)
covid_df_bmi$IMD_decile <- factor(covid_df_bmi$IMD_decile, 
                                      order=TRUE, 
                                      levels=c('1', '2', '3', '4', '5', '6', '7', '8', '9', '10'))

covid_df_bmi$hlqpuk11 <- factor(covid_df_bmi$hlqpuk11,
                                  order=TRUE,
                                  levels = c('10', '11', '12', '13', '14', '15', '16'))


q100 = quantile(covid_df_bmi$bmi, c(.025, .975))
covid_df_bmi975 <- covid_df_bmi %>%
  filter((bmi <= 41.0) & (bmi >=17.4))

q100 = quantile(covid_df_bmi$bmi, c(.05, .95))
covid_df_bmi95 <- covid_df_bmi %>%
  filter((bmi <= 37.8) & (bmi >=18.8))

covid_df_full <- covid_df_bmi
rm(covid_df_bmi)
gc()



############################################
# COXPH MODELS HOSPITALISATION             #
############################################

fit_3_models <- function(dataset, y,time,df ){

dataset$y <- dataset[[y]] 
dataset$t <- dataset[[time]] 
  
# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1 <- coxph(Surv(t+1, y) ~ ns(bmi, df=df)*ethnicity_short + 
              +  poly(age_2020,3) + sex + strata(pr_census_region), dataset, weight=w)

fit2 <- coxph(Surv(t+1, y) ~ ns(bmi, df=df)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              dataset, weight=w)

fit3 <- coxph(Surv(t+1,y ) ~ ns(bmi, df=df)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
              dataset, weight=w)
  return(list(fit1, fit2, fit3))
}


# fit models
models_df4 <- fit_3_models(dataset= covid_df_bmi975, 
                           y = "hosp_covid",
                           time = "t_hosp",
                           df = 4)
fit1 <- models_df4[[1]]
fit2 <- models_df4[[2]]
fit3 <- models_df4[[3]]
attr(terms(fit1), "predvars")
attr(terms(fit2), "predvars")
attr(terms(fit3), "predvars")

summary(fit2)

ordered_cat <- c('deprived', 'IMD_decile', 'scgpuk11', 'hlqpuk11', 'n_hh')
continuous_cat <- c('age_2020', 'pop_density', 'proximity_to_others', 'exposure_to_disease')
nonordered_cat <- c('sex', 'pr_census_region', 'pop_density_indicator', 'tenhuk11_coll', 'typaccom',
                  'Multigen_2020', 'overcrowded', 'keyworker_type', 'hh_keyworker', 'child_in_hh',
                  'b_cerebralpalsy', 'b_asthma', 'b_AF', 'b_chd', 'b_bloodcancer', 
                  'b_copd', 'b_pulmrare', 'b_dementia', 'diabetes_cat', 'learncat', 'b_CCF',
                  'b_cirrhosis',   'b_neurorare',  'b_parkinsons', 'b_pvd', 'b_fracture4',
                  'b_ra_sle', 'b_semi', 'b_stroke', 'b_vte',  'b2_82', 'b2_leukolaba',
                  'b2_prednisolone', 'renalcat', 'p_solidtransplant', 'b_respcancer',
                  'b_epilepsy', 'ruralurban_name')
ignore <- c('bmi')

covid_df_bmi975$n_hh_f<- factor(covid_df_bmi975$n_hh, order=TRUE, levels=c('0-2', '3-4', '5-6', 'X'))
median(as.numeric(covid_df_bmi975$n_hh_f), na.rm=TRUE)
for (i in ordered_cat){
  print(i)
  print(median(as.numeric(as.character(unlist(covid_df_bmi975[i]))), na.rm=TRUE))
}
for (i in continuous_cat){
  print(i)
  print(median(as.numeric(unlist(covid_df_bmi975[i]))), na.rm=TRUE)
}
for (i in nonordered_cat){
  print(i)
  print(mfv(unlist(covid_df_bmi975[i]), na_rm=TRUE))
}

# function that creates a column for each variable, and (here) sets equal to the median/mode 
# value from the previous bit of code
baseline_chars_fun <- function(df){
  df <- df %>% 
        mutate(age_2020 = 66, sex='2', pr_census_region='L', 
        pop_density_indicator = 0, pop_density=4356, deprived= '2',IMD_decile='5',
        scgpuk11 = '3', hlqpuk11 = '12', tenhuk11_coll = '0', typaccom = '2', n_hh='0-2', Multigen_2020='0.0',
        overcrowded = '0.0', keyworker_type = 'Not keyworker', proximity_to_others = 55, exposure_to_disease=10,
        hh_keyworker = 'N', child_in_hh = '0.0',
        
        b_cerebralpalsy=0, b_asthma=0, b_AF=0, b_chd =0, b_bloodcancer =0, 
        b_copd=0, b_pulmrare =0, b_dementia =0, diabetes_cat=0, learncat=0, b_CCF=0,
        b_cirrhosis=0,   b_neurorare=0,  b_parkinsons=0, b_pvd=0, b_fracture4=0,
        b_ra_sle=0, b_semi =0, b_stroke=0, b_vte=0,  b2_82=0, b2_leukolaba =0,
        b2_prednisolone=0, renalcat=0, p_solidtransplant=0, b_respcancer=0, b_epilepsy=0, ruralurban_name='Urban major conurbation')

df$sex <- as.factor(df$sex)
df$IMD_decile <- as.factor(df$IMD_decile)
df$deprived <- as.factor(df$deprived)
df$scgpuk11 <- as.factor(df$scgpuk11)
df$tenhuk11_coll <- as.factor(df$tenhuk11_coll)
df$Multigen_2020 <- as.factor(df$Multigen_2020)
df$overcrowded <- as.factor(df$overcrowded)
df$typaccom <- as.factor(df$typaccom)
df$hlqpuk11 <- as.factor(df$hlqpuk11)
df$n_hh <- as.factor(df$n_hh)
return(df)
}

# this function plots the Hazard ratio and confidence intervals fo ethnicity_short
# formats the ribbon and colours nicely
HR_plot_fun <- function(dataset, title){
  
HR_plot1 <- ggplot(data=dataset, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=dataset, aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity_short)), alpha=0.30, show.legend=FALSE)+ 
geom_line(aes(colour=ethnicity_short))+
labs(x=(expression('Body Mass Index' ~kg/m^2)), y = 'Hazard ratio (reference ethnicity==white and BMI==22.5)')+
theme_few() + scale_colour_lancet() + scale_fill_lancet()+
ggtitle(title)+
theme(legend.title=element_blank()) 

  return(HR_plot1)
}


# use the functions to plot the models:
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=201),
                            bmi = rep(seq(20,40,0.1), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)

## use the model fits to predict linear predictors for risk, for the baseline tables.
# exp(lp-lp) == HR (HR=exp(lp2-lp1)  becuase: Hazard = Ho*exp(lp), so HR =Ho*exp(lp)/Ho*exp(lp)--> exp(lp-lp))
# plot the HR relative to white and bmi==22.5

# fit 1 ##
# fit the model to the pred_baseline table, extract the fit and se, calc the C.i
pred_lp_se1 <- predict(fit1, pred_baselines, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_se1$fit,
                        se = pred_lp_se1$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi1 <- cbind(pred_baselines, pred_lp_df1)
# set white person with healthy bmi as the reference and calculate the HR relative to this
reference <- subset(lp_and_bmi1, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi1 <- lp_and_bmi1 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot1 <- HR_plot_fun(lp_and_bmi1, 'Fit 1 - age, sex') 
HR_plot1

write.csv(lp_and_bmi1, paste0(out_dir, "/hr2_bmi_hosp.csv"))
## fit 2 ##
pred_lp_se2 <- predict(fit2, pred_baselines, type='lp', se.fit=TRUE)
pred_lp_df2 <- data.frame(fit = pred_lp_se2$fit,
                        se = pred_lp_se2$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi2 <- cbind(pred_baselines, pred_lp_df2)
reference <- subset(lp_and_bmi2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2 <- lp_and_bmi2 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2<- HR_plot_fun(lp_and_bmi2, 'Fit 2 -  age, sex, demographic variables')
HR_plot2 +ggsave(paste0(out_dir, "/hr2_bmi_hosp.png"))


write.csv(lp_and_bmi2, paste0(out_dir, "/hr2_bmi_hosp.csv"))


## HRs relative to white for different BMI values

white_ref <- lp_and_bmi2 %>%
           filter(ethnicity_short == "White")%>%
           select(bmi, white_fit = fit, white_ci1 = ci1,
                 white_ci2 = ci2)

hr_all <- lp_and_bmi2 %>%
           filter(ethnicity_short != "White")%>%
          select(ethnicity_short, bmi, fit, ci1, ci2)%>%
          left_join(white_ref)%>%
          mutate(HR = exp(fit - white_fit),
                    HRci1 = exp(ci1- white_fit),
                    HRci2 = exp(ci2 - white_fit))
write.csv(hr_all, paste0(out_dir,"/HR_ethnicity_different_bmi_hosp.csv"))

  
############################
##### CONFIRMED COVIDm #####
############################



# fit models
models_df4 <- fit_3_models(dataset= covid_df_bmi975, 
                           y = "conf_COVID_death",
                           time = "t",
                           df = 4)
fit1 <- models_df4[[1]]
fit2 <- models_df4[[2]]
fit3 <- models_df4[[3]]
attr(terms(fit1), "predvars")
attr(terms(fit2), "predvars")
attr(terms(fit3), "predvars")


ordered_cat <- c('deprived', 'IMD_decile', 'scgpuk11', 'hlqpuk11', 'n_hh')
continuous_cat <- c('age_2020', 'pop_density', 'proximity_to_others', 'exposure_to_disease')
nonordered_cat <- c('sex', 'pr_census_region', 'pop_density_indicator', 'tenhuk11_coll', 'typaccom',
                  'Multigen_2020', 'overcrowded', 'keyworker_type', 'hh_keyworker', 'child_in_hh',
                  'b_cerebralpalsy', 'b_asthma', 'b_AF', 'b_chd', 'b_bloodcancer', 
                  'b_copd', 'b_pulmrare', 'b_dementia', 'diabetes_cat', 'learncat', 'b_CCF',
                  'b_cirrhosis',   'b_neurorare',  'b_parkinsons', 'b_pvd', 'b_fracture4',
                  'b_ra_sle', 'b_semi', 'b_stroke', 'b_vte',  'b2_82', 'b2_leukolaba',
                  'b2_prednisolone', 'renalcat', 'p_solidtransplant', 'b_respcancer',
                  'b_epilepsy', 'ruralurban_name')
ignore <- c('bmi')

covid_df_bmi975$n_hh_f<- factor(covid_df_bmi975$n_hh, order=TRUE, levels=c('0-2', '3-4', '5-6', 'X'))
median(as.numeric(covid_df_bmi975$n_hh_f), na.rm=TRUE)
for (i in ordered_cat){
  print(i)
  print(median(as.numeric(as.character(unlist(covid_df_bmi975[i]))), na.rm=TRUE))
}
for (i in continuous_cat){
  print(i)
  print(median(as.numeric(unlist(covid_df_bmi975[i]))), na.rm=TRUE)
}
for (i in nonordered_cat){
  print(i)
  print(mfv(unlist(covid_df_bmi975[i]), na_rm=TRUE))
}

# function that creates a column for each variable, and (here) sets equal to the median/mode 
# value from the previous bit of code
baseline_chars_fun <- function(df){
  df <- df %>% 
        mutate(age_2020 = 66, sex='2', pr_census_region='L', 
        pop_density_indicator = 0, pop_density=4356, deprived= '2',IMD_decile='5',
        scgpuk11 = '3', hlqpuk11 = '12', tenhuk11_coll = '0', typaccom = '2', n_hh='0-2', Multigen_2020='0.0',
        overcrowded = '0.0', keyworker_type = 'Not keyworker', proximity_to_others = 55, exposure_to_disease=10,
        hh_keyworker = 'N', child_in_hh = '0.0',
        
        b_cerebralpalsy=0, b_asthma=0, b_AF=0, b_chd =0, b_bloodcancer =0, 
        b_copd=0, b_pulmrare =0, b_dementia =0, diabetes_cat=0, learncat=0, b_CCF=0,
        b_cirrhosis=0,   b_neurorare=0,  b_parkinsons=0, b_pvd=0, b_fracture4=0,
        b_ra_sle=0, b_semi =0, b_stroke=0, b_vte=0,  b2_82=0, b2_leukolaba =0,
        b2_prednisolone=0, renalcat=0, p_solidtransplant=0, b_respcancer=0, b_epilepsy=0, ruralurban_name='Urban major conurbation')

df$sex <- as.factor(df$sex)
df$IMD_decile <- as.factor(df$IMD_decile)
df$deprived <- as.factor(df$deprived)
df$scgpuk11 <- as.factor(df$scgpuk11)
df$tenhuk11_coll <- as.factor(df$tenhuk11_coll)
df$Multigen_2020 <- as.factor(df$Multigen_2020)
df$overcrowded <- as.factor(df$overcrowded)
df$typaccom <- as.factor(df$typaccom)
df$hlqpuk11 <- as.factor(df$hlqpuk11)
df$n_hh <- as.factor(df$n_hh)
return(df)
}

# this function plots the Hazard ratio and confidence intervals fo ethnicity_short
# formats the ribbon and colours nicely
HR_plot_fun <- function(dataset, title){
  
HR_plot1 <- ggplot(data=dataset, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=dataset, aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity_short)), alpha=0.30, show.legend=FALSE)+ 
geom_line(aes(colour=ethnicity_short))+
labs(x=(expression('Body Mass Index' ~kg/m^2)), y = 'Hazard ratio (reference ethnicity==white and BMI==22.5)')+
theme_few() + scale_colour_lancet() + scale_fill_lancet()+
ggtitle(title)+
theme(legend.title=element_blank()) 

  return(HR_plot1)
}


# use the functions to plot the models:
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=201),
                            bmi = rep(seq(20,40,0.1), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)

## use the model fits to predict linear predictors for risk, for the baseline tables.
# exp(lp-lp) == HR (HR=exp(lp2-lp1)  becuase: Hazard = Ho*exp(lp), so HR =Ho*exp(lp)/Ho*exp(lp)--> exp(lp-lp))
# plot the HR relative to white and bmi==22.5

# fit 1 ##
# fit the model to the pred_baseline table, extract the fit and se, calc the C.i
pred_lp_se1 <- predict(fit1, pred_baselines, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_se1$fit,
                        se = pred_lp_se1$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi1 <- cbind(pred_baselines, pred_lp_df1)
# set white person with healthy bmi as the reference and calculate the HR relative to this
reference <- subset(lp_and_bmi1, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi1 <- lp_and_bmi1 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot1 <- HR_plot_fun(lp_and_bmi1, 'Fit 1 - age, sex') 
HR_plot1

write.csv(lp_and_bmi1, paste0(out_dir, "/hr2_bmi_conf_covid.csv"))
## fit 2 ##
pred_lp_se2 <- predict(fit2, pred_baselines, type='lp', se.fit=TRUE)
pred_lp_df2 <- data.frame(fit = pred_lp_se2$fit,
                        se = pred_lp_se2$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi2 <- cbind(pred_baselines, pred_lp_df2)
reference <- subset(lp_and_bmi2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2 <- lp_and_bmi2 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2<- HR_plot_fun(lp_and_bmi2, 'Fit 2 -  age, sex, demographic variables')
HR_plot2 +ggsave(paste0(out_dir, "/hr2_bmi_conf_covid.png"))


write.csv(lp_and_bmi2, paste0(out_dir, "/hr2_bmi_conf_covid.csv"))


## HRs relative to white for different BMI values

white_ref <- lp_and_bmi2 %>%
           filter(ethnicity_short == "White")%>%
           select(bmi, white_fit = fit, white_ci1 = ci1,
                 white_ci2 = ci2)

hr_all <- lp_and_bmi2 %>%
           filter(ethnicity_short != "White")%>%
          select(ethnicity_short, bmi, fit, ci1, ci2)%>%
          left_join(white_ref)%>%
          mutate(HR = exp(fit - white_fit),
                    HRci1 = exp(ci1- white_fit),
                    HRci2 = exp(ci2 - white_fit))
write.csv(hr_all, paste0(out_dir,"/HR_ethnicity_different_bmi_conf_covid.csv"))

  