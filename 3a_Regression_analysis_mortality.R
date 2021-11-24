# author: a.c.summerfield; v.nafilyan
# date: 03/2021; last edited 09/2021
# description: 
# reads in covid_bmi_narm2 (created in sampling.R),
# create covid_df_bmi975 - dataframe with bmi trimmed at 97.5 and 2.5%iles
# fits 3 coxph models 
# plots the HR for diffferent ethnicities
# creates a table of estimates HR, from which can read off the HR equivalent for different ethnicities
# creates summary tables for the sample of data used for this analysis

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



########################
# directories and macro
#########################
dir ="cen_dth_gps/COVID19_BMI"
out_dir= paste0(dir, "/Results")

eos_date = "2020-11-30"
r_white = 0.01
r_nonwhite = 0.1




##################################
## read in data and trim bmi    ##
##################################

covid_df <- readRDS("Data/covid_bmi_narm2.RDS" )%>%
filter(is.na(dod)|dod >="2020-01-24")%>%
 mutate( 
        ethnicity = relevel(as.factor(ethnicity), ref="White British"),
        ethnicity_short = relevel(as.factor(ethnicity_short), ref="White"), 
        t = ifelse(death == 1,
                    difftime(dod, as.Date("2020-01-24", format = "%Y-%m-%d"), unit="days"),
                   difftime(as.Date("2020-12-28", format = "%Y-%m-%d"),
                            as.Date("2020-01-24", format = "%Y-%m-%d"),
                            unit="days")))

#restrict to 40+
covid_df <- covid_df %>% filter(age_2020 >= 40)

# cut 2.5%/97.5%
# this section removes nas from bmi - irrelevant if using the dataframe with na.rm already
covid_df <- covid_df%>%
         mutate(bmi_na=ifelse(is.na(bmi),1,0))

ggplot(covid_df, aes(x=age_2020, y=bmi))+
  geom_smooth()

# na plot - this is irrelevant if using the dataset with na.rm
covid_df$age_group <- as.numeric(cut(covid_df$age_2020, 10))
na_tib <- covid_df %>% group_by(age_group, ethnicity_short, death_covid) %>%
  summarise(sum_nas = sum(bmi_na), count = n(), mean_age = mean(age_2020)) %>%
  mutate(perc_na = 100*sum_nas/count)
plot2 <- ggplot(data=na_tib, aes(x = mean_age, y=perc_na, group=interaction(ethnicity_short, death_covid)))+
      geom_point(aes(shape=factor(death_covid), colour=ethnicity_short))
plot2

# investigate 
ggplot(covid_df) +
  geom_histogram(aes(bmi), bins=30)

ggplot(subset(covid_df, ethnicity_short=='South Asian') )+
  geom_histogram(aes(bmi), bins=30)

## remove missing BMI
covid_df_bmi <- covid_df %>% filter(bmi_na==0) %>% arrange(bmi)

##################################
## Recode variables            ##
##################################


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

table(covid_df_bmi975$ethnicity, covid_df_bmi975$ethnicity_short)


############################################
# COXPH MODELS AND HR PLOTS TY covariates  #
############################################

fit_3_models <- function(df, dataset){

# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short + 
              +  poly(age_2020,3) + sex + strata(pr_census_region), dataset, weight=w)

fit2 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              dataset, weight=w)

fit3 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short
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

fit_2_models <- function(df, dataset){


# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short + 
              +  poly(age_2020,3) + sex + strata(pr_census_region), dataset, weight=w)

fit2 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator+poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              dataset, weight=w)


  return(list(fit1, fit2))
}


models_df4 <- fit_3_models(4, covid_df_bmi975)
fit1 <- models_df4[[1]]
fit2 <- models_df4[[2]]
fit3 <- models_df4[[3]]
attr(terms(fit1), "predvars")
attr(terms(fit2), "predvars")
attr(terms(fit3), "predvars")


##### examine the VIF of bmi - rms::vif() needs a full rank matrix as an input ####

## Model 2 
# create matrix of full rank
mat <- model.matrix(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator+poly(pop_density,2) + deprived + IMD_decile
    + scgpuk11+ hlqpuk11+ tenhuk11_coll + typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name
                    + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
              covid_df_bmi975)
qr_mat <- qr(mat, tol=1e-9, LAPACK = FALSE)
rnkX <- qr_mat$rank
keep <- qr_mat$pivot[seq_len(rnkX)]

X<- mat[,keep]

Y <- Surv(covid_df_bmi975$t+1, covid_df_bmi975$death_covid )
W = covid_df_bmi975$w

m<- coxph(Y~X[,-1], weight=W)
print(broom::tidy(m))

rms::vif( m)
vif_res1 = rms::vif( m)
vif_res_df1 <- data.frame(name = attr(vif_res1, "names"),
                          vif=vif_res1)
write.csv(vif_res_df1, paste0(out_dir, '/model3_vif.csv'))     


## Model 3 
# create matrix of full rank
mat <- model.matrix(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator+poly(pop_density,2) + deprived + IMD_decile
    + scgpuk11+ hlqpuk11+ tenhuk11_coll + typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_bmi975)
qr_mat <- qr(mat, tol=1e-9, LAPACK = FALSE)
rnkX <- qr_mat$rank
keep <- qr_mat$pivot[seq_len(rnkX)]

X<- mat[,keep]

Y <- Surv(covid_df_bmi975$t+1, covid_df_bmi975$death_covid )
W = covid_df_bmi975$w

m<- coxph(Y~X[,-1], weight=W)
print(broom::tidy(m))

rms::vif( m)
vif_res1 = rms::vif( m)
vif_res_df1 <- data.frame(name = attr(vif_res1, "names"),
                          vif=vif_res1)
write.csv(vif_res_df1, paste0(out_dir, '/model2_vif.csv'))     
##########################
#plotting the models
##########################

## set up:
## create table with covar equal to a baseline, so ethnicity is only var that changes:
## need pred baselines to be medians and modes of non ordered ones - so this section calculates
## medians and modes which then go into baseline_chars (not v automated - sorry)
## in pred_baselines various vars get made into factors

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


## fit 3 ##
pred_lp_se3 <- predict(fit3, pred_baselines, type='lp', se.fit=TRUE)
pred_risk3 <- predict(fit3, pred_baselines, type='risk', se.fit=TRUE)
pred_lp_df3 <- data.frame(fit = pred_lp_se3$fit,
                        se = pred_lp_se3$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi3 <- cbind(pred_baselines, pred_lp_df3)
reference <- subset(lp_and_bmi3, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi3 <- lp_and_bmi3 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3 <- HR_plot_fun(lp_and_bmi3,'Fit 3 - age, sex, demographic variables and health&disease variables')
HR_plot3


## save out charts
HR_plot1
HR_plot2
HR_plot3

fn = paste0(out_dir, '/HR_plot1_v3.png')
png(file=fn, width = 800, height = 600)
HR_plot1
dev.off()


fn = paste0(out_dir, '/HR_plot2_v3.png')
png(file=fn, width = 800, height = 600)
HR_plot2
dev.off()


fn = paste0(out_dir, '/HR_plot3_v3.png')
png(file=fn, width = 800, height = 600)
HR_plot3
dev.off()

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
write.csv(hr_all, paste0(out_dir,"/HR_ethnicity_different_bmi.csv"))

  
HR_plot_all<- ggplot(data=hr_all, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=hr_all, aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity_short)), alpha=0.30, show.legend=FALSE)+ 
geom_line(aes(colour=ethnicity_short))+
labs(x=(expression('Body Mass Index' ~kg/m^2)), y = 'Hazard ratio (reference ethnicity==white)')+
theme_few() + scale_colour_lancet() + scale_fill_lancet()

HR_plot_all + ggsave( paste0(out_dir,"/HR_ethnicity_different_bmi.png"))
## Hazard Ratios (sanity check)  

#ratio black-white at 22.5
reference_w22.5 <- subset(lp_and_bmi2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
print(paste0(' the linear predictor for white and bmi=22.5 is ' , rb))

black_22.5 <- subset(lp_and_bmi2, ethnicity_short=='Black' &bmi==22.5) %>% select(fit)
black_22.5 <- black_22.5[[1,1]]
print(paste0('the linear predictor for black and bmi=22.5 is ' , black_22.5))

ratio_black_white_22.5 <- exp(black_22.5 - rb)
print(paste0('HR white - black at bmi=22.5 is  ' , ratio_black_white_22.5))

########################################
## finding BMI with equivalent risk    #
########################################

## model 2 for a bmi of 30
# find the reference: the lp for a white person with bmi 30, as predicted by model 2
# note: this essentially fails at bmi==30 because for the risk for person with ethnicity=SA or Other,
# the risk is greater at any BMI than t is for white person w bmi=35. 
r <- subset(lp_and_bmi2, ethnicity_short=='White' & bmi==30) %>% select(fit)
r <- r[[1,1]]
r225 <- subset(lp_and_bmi2, ethnicity_short=='White' & bmi==22.5) %>% select(fit)
r225 <- r225[[1,1]]
print(paste0('reference fit is ', r))
print(paste0('reference Hr is ', exp(r-r225)))

# table version
# 1. set up a longer table
pred_baselines2 <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=2001),
                            bmi = rep(seq(20,40,0.01), 4))
pred_baselines2 <- baseline_chars_fun(pred_baselines2)

# 2.use longer table to predict HRs and ses for models 2 and 3
pred_lp_se2 <- predict(fit2, pred_baselines2, type='lp', se.fit=TRUE)
pred_risk2_se2 <- predict(fit2, pred_baselines, type='risk', se.fit=TRUE)

pred_lp_df2<- data.frame(fit = pred_lp_se2$fit,
                        se = pred_lp_se2$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi2 <- cbind(pred_baselines2, pred_lp_df2)
reference <- subset(lp_and_bmi2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2 <- lp_and_bmi2 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

# 3. save out the table
write.csv(lp_and_bmi2, paste0(out_dir, '/model2_HRs_2.csv'))
HR_plot2_check <- HR_plot_fun(lp_and_bmi2, 'fit2')

HR_plot2_check


# repeat for model 3
#
# 1. set up a longer table -- ALREADY DONE

# 2.use longer table to predict HRs and ses for models 2 and 3
pred_lp_se3 <- predict(fit3, pred_baselines2, type='lp', se.fit=TRUE)

pred_risk2_se3 <- predict(fit3, pred_baselines, type='risk', se.fit=TRUE)

pred_lp_df3<- data.frame(fit = pred_lp_se3$fit,
                        se = pred_lp_se3$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi3 <- cbind(pred_baselines2, pred_lp_df3)
reference <- subset(lp_and_bmi3, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi3 <- lp_and_bmi3 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

# 3. save out the table
write.csv(lp_and_bmi3, paste0(out_dir, '/model3_HRs_2.csv'))

HR_plot3_check<- HR_plot_fun(lp_and_bmi3, 'Fit 2 - age, sex, demographic variables. spline has df=5')



  
#########################################################
# how doe sit look restricting bmi to >22.5
########################################################

# here we restrict bmi to >22.5 (which I hope beyond which it looks linear/not spliney)
# and fit bmi, ethnicty, and bmi*ethnicity
options(max.print=1000000)
covid_df_bmi975_25 <- subset(covid_df_bmi975, bmi>=25)


# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1f <- coxph(Surv(t+1, death_covid) ~ bmi*ethnicity_short + bmi + ethnicity_short 
              +  poly(age_2020,3) + sex + strata(pr_census_region), covid_df_bmi975_25, weight=w)

fit2f <- coxph(Surv(t+1, death_covid) ~ bmi*ethnicity_short + bmi +ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_bmi975_25, weight=w)

fit3f <- coxph(Surv(t+1, death_covid) ~ bmi*ethnicity_short + bmi + ethnicity_short 
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
              covid_df_bmi975_25, weight=w)
  

  
# use fit3_fbmi as basis for a plot
pred_lp_fbmi <- predict(fit3f, pred_baselines2, type='lp', se.fit=TRUE)
pred_risk_fbmi <- predict(fit3, pred_baselines2, type='risk', se.fit=TRUE)
pred_lp_fbmi <- data.frame(fit = pred_lp_fbmi$fit,
                        se = pred_lp_fbmi$se.fit) %>%
              mutate(se1 = fit+se,
                    se2 = fit - se) %>%
              select(fit, se1, se2)

lp_and_fbmi <- cbind(pred_baselines2, pred_lp_fbmi)
reference <- subset(lp_and_fbmi, ethnicity_short=='White' &bmi==25) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_fbmi <- lp_and_fbmi %>% mutate(HR = exp(fit - rb),
                                    HRse1 = exp(se1- rb),
                                    HRse2 = exp(se2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_fbmi <- ggplot(data=lp_and_fbmi, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=lp_and_fbmi, aes(ymin=HRse2, ymax=HRse1, fill=factor(ethnicity_short)), alpha=0.30)+
geom_line(aes(colour=ethnicity_short))+
#theme_few() + scale_color_manual(values=wes_palette(n=4, name='Royal1'))+ scale_fill_manual(values=wes_palette(n=4, name='Royal1'))+
theme_few() + scale_colour_lancet() + scale_fill_lancet()+
labs(x='BMI', y = 'Hazard ratio (reference ethnicity==white and bmi==22.5)')+
theme(legend.title=element_blank())+
ggtitle('Fit 3 - age, sex, demographic variables and health&disease variables')

HR_plot3_fbmi
  
  
summary(fit2f)
  # bmi is significant, other is significant, and bmi*eths are sig
  # bmi*black = 0.047; bmi*southasian = <0.001, bmi*other = <0.001

summary(fit3f)
  # interactions are significant, bmi is significant, other is significant 

# anova betwen models w and without interaction
fit2_noint <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4) +ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_bmi975, weight=w)

  fit2_int <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short + bmi +ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_bmi975, weight=w)

anova(fit2_noint, fit2_int, type='chisq') # are significantly different (chiqs for whether reduction in the residual sum of squares are statistically significant or not).)
# but not sure if that tells us anything really?
  

  
###########################################################################################
# sense check: these look th same as plots earlier where use ethnicity_short*bmi only)
###########################################################################################
  
fit_3_models_n <- function(df){

# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short +
              bmi + ethnicity_short 
              +  poly(age_2020,3) + sex + strata(pr_census_region), covid_df_bmi975, weight=w)

fit2 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short +
              bmi + ethnicity_short 
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh,
              covid_df_bmi975, weight=w)

fit3 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short +
              bmi + ethnicity_short 
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  + b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant,
              covid_df_bmi975, weight=w)
  return(list(fit1, fit2, fit3))
}

models_df4_eth <- fit_3_models_eth(4, covid_df_bmi975)
fit1_eth <- models_df4_eth[[1]]
fit2_eth <- models_df4_eth[[2]]
fit3_eth <- models_df4_eth[[3]]



  
models_df4_n <- fit_3_models_n(4)
fit1n <- models_df4_n[[1]]
fit2n <- models_df4_n[[2]]
fit3n <- models_df4_n[[3]]
models_df5 <- fit_3_models(5)
fit1_df5 <- models_df5[[1]]
fit2_df5 <- models_df5[[2]]
fit3_df5 <- models_df5[[3]]

pred_lp_3n <- predict(fit3n, pred_baselines, type='lp', se.fit=TRUE)
pred_risk3n <- predict(fit3n, pred_baselines, type='risk', se.fit=TRUE)

pred_lp_3n <- data.frame(fit = pred_lp_3n$fit,
                        se = pred_lp_3n$se.fit) %>%
              mutate(se1 = fit+se,
                    se2 = fit - se) %>%
              select(fit, se1, se2)

lp_and_bmi3n <- cbind(pred_baselines, pred_lp_3n)

reference <- subset(lp_and_bmi3n, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi3n <- lp_and_bmi3n %>% mutate(HR = exp(fit - rb),
                                    HRse1 = exp(se1- rb),
                                    HRse2 = exp(se2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3n <- ggplot(data=lp_and_bmi3n, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=lp_and_bmi3n, aes(ymin=HRse2, ymax=HRse1, fill=factor(ethnicity_short)), alpha=0.30)+
geom_line(aes(colour=ethnicity_short))+
#theme_few() + scale_color_manual(values=wes_palette(n=4, name='Royal1'))+ scale_fill_manual(values=wes_palette(n=4, name='Royal1'))+
theme_few() + scale_colour_lancet() + scale_fill_lancet()+
labs(x='BMI', y = 'Hazard ratio (reference ethnicity==white and bmi==22.5)')+
theme(legend.title=element_blank())+
ggtitle('Fit 3n - age, sex, demographic variables and health&disease variables')

HR_plot3n
  
  
  
  
###################
# major revisions #
###################




# look at the data 
max(covid_df_full$bmi)
min(covid_df_full$bmi)

# see that the tails have some extrme evalues
# on the low tail: obviously erroneous values, such as <5, so makes sense to censor to 15 regardless
# on the high tail - see its gets so patchy for non-white ethnicities
covid_df_full %>% filter(bmi <=15) %>% summarise(count = n())
breaks = seq(0,60, by=5)
x = cut(covid_df_full$bmi, breaks, right = FALSE)
x.freq = table(x)
x.cumfreq = cumsum(x.freq)
x.perc = round(x.freq/x.cumfreq[12]*100, 2)
y = cbind(x.freq, x.perc)
write.csv(y, paste0(out_dir,"/full_sample_bmi_freq.csv"))
  
high_bmi <- covid_df_full %>% filter(bmi >=50)
high_bmi_plot <-ggplot(high_bmi) +
  geom_histogram(aes(bmi), bins=30) +
facet_wrap(~ethnicity_short)

    fn = paste0(out_dir, '/bmi_50plus.png')
png(file=fn, width = 500, height = 500)
high_bmi_plot
dev.off() 


histogram_full_df <- ggplot(covid_df_full) +
  geom_histogram(aes(bmi), bins=30)

histogram_5perc_df <- ggplot(covid_df_bmi975) +
  geom_histogram(aes(bmi), bins=30)

  fn = paste0(out_dir, '/histogram_full_df.png')
png(file=fn, width = 500, height = 500)
histogram_full_df
dev.off() 
  
  fn = paste0(out_dir, '/histogram_5perc_df.png')
png(file=fn, width = 500, height = 500)
histogram_5perc_df
dev.off() 

## trim just the 1%
##covid_df_full <- covid_df_full  %>% arrange(bmi)
#q100 = quantile(covid_df$bmi, c(.005, .995))
#q100
#covid_df_full <- covid_df_full %>%
#  filter((bmi <= q100[[2]]) & (bmi >=q100[[1]]))
#

# Results with no trimming
models_full_data <- fit_3_models(4,covid_df_full)
fit3_full <- models_full_data[[3]]
fit2_full <- models_full_data[[2]]
fit1_full <- models_full_data[[1]]
rm(models_full_data)
rm(covid_df, covid_df_bmi, covid_df_bmi95, covid_df_bmi975)
gc()

# use the functions to plot the models:
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=201),
                            bmi = rep(seq(20,40,0.1), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)





## fit 2 ##
pred_lp_se2 <- predict(fit2_full, pred_baselines, type='lp', se.fit=TRUE)
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

HR_plot2_full<- HR_plot_fun(lp_and_bmi2, 'Fit 2 -  age, sex, demographic variables')

write.csv(lp_and_bmi2, paste0(out_dir, "/hr2_bmi_notrim.csv"))

HR_plot2_full+ ggsave(paste0(out_dir, '/HR_plot1_notrim.png'))
  



  
  


