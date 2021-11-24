#### Run Regression_analysis_mortality.R before this ####

# author: a.summerfield
# description: analysis looking at differences between various characteristics


  
#####################################
# repeat analysis by ages
#####################################

covid_df_young <- covid_df_bmi975 %>% filter(age_2020 <70)
covid_df_old <- covid_df_bmi975 %>% filter(age_2020 >=70)

# NEED TO FIND THE median for younge people, and medians for old people?
  
fit_old <- fit_3_models(4, covid_df_old)
fit_old3 <- fit_old[[3]]
fit_old2 <- fit_old[[2]]
fit_young <- fit_3_models(4, covid_df_young)
fit_young3 <- fit_young[[3]]
fit_young2 <- fit_young[[2]]
  
# find medians for young people
#covid_df_young$n_hh_f<- factor(covid_df_young$n_hh, order=TRUE, levels=c('0-2', '3-4', '5-6', 'X'))
#median(as.numeric(covid_df_young$n_hh_f), na.rm=TRUE)
#for (i in ordered_cat){
#  print(i)
#  print(median(as.numeric(as.character(unlist(covid_df_young[i]))), na.rm=TRUE))
#}
#for (i in continuous_cat){
#  print(i)
#  print(median(as.numeric(unlist(covid_df_young[i]))), na.rm=TRUE)
#}
#for (i in nonordered_cat){
#  print(i)
#  print(mfv(unlist(covid_df_young[i]), na_rm=TRUE))
#}
#  
  # create table with covar equal to a baseline, so ethnicity is only var that changes:
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=2001),
                            bmi = rep(seq(20,40,0.01), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)

pred_baselines_y <- baseline_chars_fun(pred_baselines) %>%
  # set up pred baselines foryoung people
  mutate(age_2020 = 53, sex='2', pr_census_region='L', 
        pop_density_indicator = 0, pop_density=5219, deprived= '2',IMD_decile='4',
        scgpuk11 = '2', hlqpuk11 = '14', tenhuk11_coll = '1', typaccom = '2', n_hh='0-2', Multigen_2020='0.0',
        overcrowded = '0.0', keyworker_type = 'Not keyworker', proximity_to_others = 55.8, exposure_to_disease=10.5,
        hh_keyworker = 'N', child_in_hh = '0.0',
        
        b_cerebralpalsy=0, b_asthma=0, b_AF=0, b_chd =0, b_bloodcancer =0, 
        b_copd=0, b_pulmrare =0, b_dementia =0, diabetes_cat=0, learncat=0, b_CCF=0,
        b_cirrhosis=0,   b_neurorare=0,  b_parkinsons=0, b_pvd=0, b_fracture4=0,
        b_ra_sle=0, b_semi =0, b_stroke=0, b_vte=0,  b2_82=0, b2_leukolaba =0,
        b2_prednisolone=0, renalcat=0, p_solidtransplant=0, b_respcancer=0, b_epilepsy=0, ruralurban_name='Urban major conurbation')


  
pred_baselines_o  <- baseline_chars_fun(pred_baselines) %>%   mutate(age_2020 = 80)
  
# use the model fits to predict linear predictors for risk, for the baseline tables.
# exp(lp-lp) == HR (HR=exp(lp2-lp1)  becuase: Hazard = Ho*exp(lp), so HR =Ho*exp(lp)/Ho*exp(lp)--> exp(lp-lp))
# plot the HR relative to white and bmi==22.5

# fit 3 young ##
pred_lp_se_y <- predict(fit_young3, pred_baselines_y, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df_y<- data.frame(fit = pred_lp_se_y$fit,
                        se = pred_lp_se_y$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_y <- cbind(pred_baselines_y, pred_lp_df_y)
reference <- subset(lp_and_bmi_y, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_y3 <- lp_and_bmi_y %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_y <- HR_plot_fun(lp_and_bmi_y3, 'Fit 3, stratified by age, <70 years') +ylim(0,14)
HR_plot3_y 
    
## fit3 old ##
pred_lp_se_o <- predict(fit_old3, pred_baselines_o, type='lp', se.fit=TRUE)
pred_lp_df_o <- data.frame(fit = pred_lp_se_o$fit,
                        se = pred_lp_se_o$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_o <- cbind(pred_baselines_o, pred_lp_df_o)
reference <- subset(lp_and_bmi_o, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_o <- lp_and_bmi_o %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_o <- HR_plot_fun(lp_and_bmi_o, 'Fit 3, stratified by age, >70 years') + ylim(0,14)
HR_plot3_o
  
# fit young 2
pred_lp_se_y <- predict(fit_young2, pred_baselines_y, type='lp', se.fit=TRUE)
pred_lp_df_y <- data.frame(fit = pred_lp_se_y$fit,
                        se = pred_lp_se_y$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_y <- cbind(pred_baselines_y, pred_lp_df_y)
reference <- subset(lp_and_bmi_y, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_y2 <- lp_and_bmi_y %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2_y <- HR_plot_fun(lp_and_bmi_y2, 'Fit 2, stratified by age, <70 years') + ylim(0,14) 
HR_plot2_y
  

  
  ## old, fit 2 ##
pred_lp_se_o <- predict(fit_old2, pred_baselines_o, type='lp', se.fit=TRUE)
pred_lp_df_o <- data.frame(fit = pred_lp_se_o$fit,
                        se = pred_lp_se_o$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_o <- cbind(pred_baselines_o, pred_lp_df_o)
reference <- subset(lp_and_bmi_o, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_o <- lp_and_bmi_o %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

  
HR_plot2_o <- HR_plot_fun(lp_and_bmi_o, 'Fit 2, stratified by age, >70 years') + ylim(0,14)
HR_plot2_o 
  


#save out age plots
fn = paste0(out_dir, '/HR_model3_young.png')
png(file=fn, width = 500, height = 500)
HR_plot3_y
dev.off()

fn = paste0(out_dir, '/HR_model3_old.png')
png(file=fn, width = 500, height = 500)
HR_plot3_o
dev.off()

fn = paste0(out_dir, '/HR_model2_young.png')
png(file=fn, width = 500, height = 500)
HR_plot2_y
dev.off()

fn = paste0(out_dir, '/HR_model2_old.png')
png(file=fn)
HR_plot2_o
dev.off()
  
#get equivalent values for younger cohort
# 3. save out the table
write.csv(lp_and_bmi_y2, paste0(out_dir, '/model2_HRs_young.csv'))
write.csv(lp_and_bmi_y3, paste0(out_dir, '/model3_HRs_young.csv'))

#For model 2:
#Black – 29.8 (27.3, 32.3)
#South Asian – 25.6 (20.4,29.5)
#Other – 33.1 (30.4,35.3)
#
#For model 3
#Black – 27.8 (25.7,30.7)
#South Asian – 25.5 (20.8,30.7)
#Other – 31.9 (26.4, 34.8)
#
  head(lp_and_bmi_y3)

  
#####################################
# repeat analysis by sex
#####################################

covid_df_m <- covid_df_bmi975 %>% filter(sex ==1)
covid_df_f <- covid_df_bmi975 %>% filter(sex ==2)

  # fit2
fit2_m <-coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_m, weight=w)

fit2_f<- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3)  + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              covid_df_f, weight=w)

  
  # fit3
fit3_m <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  + b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
              covid_df_m, weight=w)

fit3_f <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3)  + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others + exposure_to_disease 
              + hh_keyworker + child_in_hh
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  + b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
              covid_df_f, weight=w)

# todo: use medians and pred-baseline function w/ this
# create table with covar equal to a baseline, so ethnicity is only var that changes:
pred_baselines_m <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=2001),
                            bmi = rep(seq(20,40,0.01), 4)) %>% baseline_chars_fun() %>% mutate(sex==1)

# needs updating
  pred_baselines_f <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=2001),
                            bmi = rep(seq(20,40,0.01), 4)) %>% baseline_chars_fun() %>% mutate(sex==2)

# use the model fits to predict linear predictors for risk, for the baseline tables.
# exp(lp-lp) == HR (HR=exp(lp2-lp1)  becuase: Hazard = Ho*exp(lp), so HR =Ho*exp(lp)/Ho*exp(lp)--> exp(lp-lp))
# plot the HR relative to white and bmi==22.5

## fit 2 
  # fit 2 males #
pred_lp_se_m <- predict(fit2_m, pred_baselines_m, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df_m<- data.frame(fit = pred_lp_se_m$fit,
                        se = pred_lp_se_m$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_m <- cbind(pred_baselines_m, pred_lp_df_m)
reference <- subset(lp_and_bmi_m, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_m <- lp_and_bmi_m %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2_m <- HR_plot_fun(lp_and_bmi_m, 'Fit 2 - sex ==1 (male)')+ylim(0,10.5)
HR_plot2_m 
  
## fit 2 females ##
pred_lp_se_f <- predict(fit2_f, pred_baselines_f, type='lp', se.fit=TRUE)

pred_lp_df_f <- data.frame(fit = pred_lp_se_f$fit,
                        se = pred_lp_se_f$se.fit) %>%
               mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_f <- cbind(pred_baselines_f, pred_lp_df_f)

reference <- subset(lp_and_bmi_f, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_f <- lp_and_bmi_f %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2_f <- HR_plot_fun(lp_and_bmi_f, 'HR fit 2 - sex==2, female')+ylim(0,10.5)
HR_plot2_f

  
# fit 3 males #
pred_lp_se_m <- predict(fit3_m, pred_baselines_m, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df_m<- data.frame(fit = pred_lp_se_m$fit,
                        se = pred_lp_se_m$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_m <- cbind(pred_baselines_m, pred_lp_df_m)
reference <- subset(lp_and_bmi_m, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_m <- lp_and_bmi_m %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_m <- HR_plot_fun(lp_and_bmi_m, 'Fit 3 - sex ==1 (male)')+ylim(0,6.5)
HR_plot3_m 
  
## fit 3 females ##
pred_lp_se_f <- predict(fit3_f, pred_baselines_f, type='lp', se.fit=TRUE)

pred_lp_df_f <- data.frame(fit = pred_lp_se_f$fit,
                        se = pred_lp_se_f$se.fit) %>%
               mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_f <- cbind(pred_baselines_f, pred_lp_df_f)

reference <- subset(lp_and_bmi_f, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_f <- lp_and_bmi_f %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_f <- HR_plot_fun(lp_and_bmi_f, 'HR fit 3 - sex==2, female')+ylim(0,6.5)
HR_plot3_f
#save out age plots
fn = paste0(out_dir, '/HR_model2_male.png')
png(file=fn, width = 800, height = 800)
HR_plot2_m
dev.off()

fn = paste0(out_dir, '/HR_model2_female.png')
png(file=fn, width = 800, height = 800)
HR_plot2_f
dev.off()
  
  fn = paste0(out_dir, '/HR_model3_male.png')
png(file=fn, width = 800, height = 800)
HR_plot3_m
dev.off()

fn = paste0(out_dir, '/HR_model3_female.png')
png(file=fn, width = 800, height = 800)
HR_plot3_f
dev.off()

#################################################
sensitivity wih other ethnicities
#################################################
# may need to recode/relevel the ethnicities?
  
  fit_3_models_eth <- function(df, dataset){

# set up 3 models: HR plot TY coefficients. Note uses weights=w
fit1 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity + 
              +  poly(age_2020,3) + sex + strata(pr_census_region), dataset, weight=w)

fit2 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh +ruralurban_name ,
              dataset, weight=w)

fit3 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity
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
  
models_df4_eth <- fit_3_models_eth(4, covid_df_bmi975)
fit1_eth <- models_df4_eth[[1]]
fit2_eth <- models_df4_eth[[2]]
fit3_eth <- models_df4_eth[[3]]
  

  
# plot the models
  # ODO: fill in ethnicities
pred_baselines_eth <- data.frame(ethnicity=rep(c('White British','White other','Indian','Other',
                                                 'Chinese','Black Caribbean', 'Mixed','Pakistani',
                                                 'Black African', 'Bangladeshi'),
                                               each=151),
                            bmi = rep(seq(20,35,0.1), 10))
pred_baselines_eth <- baseline_chars_fun(pred_baselines_eth)

## use the model fits to predict linear predictors for risk, for the baseline tables.
# exp(lp-lp) == HR (HR=exp(lp2-lp1)  becuase: Hazard = Ho*exp(lp), so HR =Ho*exp(lp)/Ho*exp(lp)--> exp(lp-lp))
# plot the HR relative to white and bmi==22.5

# fit 1 ##
pred_lp_se2 <- predict(fit1_eth, pred_baselines_eth, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df2<- data.frame(fit = pred_lp_se2$fit,
                        se = pred_lp_se2$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi2 <- cbind(pred_baselines_eth, pred_lp_df2)
reference <- subset(lp_and_bmi2, ethnicity=='White British' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2 <- lp_and_bmi2 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)


# White, Black African,Black Caribbean
data <- lp_and_bmi2 %>% filter(ethnicity %in% c('White British', 'Black African',  'Other'))
HR_plot2_eth_1 <- ggplot(data=subset(lp_and_bmi2,ethnicity %in% c('White British', 'Black African', 'Black Caribbean', 'Other')), aes(x=bmi, y=HR, group=ethnicity))+
geom_ribbon(data=subset(lp_and_bmi2, ethnicity %in% c('White British', 'Black African', 'Black Caribbean', 'Other')), aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity)), alpha=0.30, show.legend=FALSE)+
geom_line(aes(colour=ethnicity))+
labs(x=expression('Body Mass Index' ~kg/m^2), y = 'Hazard ratio (reference ethnicity==white british and BMI==22.5)')+
theme_few() +# scale_colour_lancet() + scale_fill_lancet()+
ggtitle('Fit2, ethnicity')+
  theme(legend.title=element_blank()) 

HR_plot2_eth_1
  

  

# White, Black African,Black Caribbean
HR_plot2_eth_2 <- ggplot(data=subset(lp_and_bmi2,ethnicity %in% c('White British','Indian','Mixed','Pakistani',
                                                 'Chinese', 'Bangladeshi')), aes(x=bmi, y=HR, group=ethnicity))+
geom_ribbon(data=subset(lp_and_bmi2, ethnicity %in% c('White British', 'Indian','Mixed','Pakistani',
                                                 'Chinese', 'Bangladeshi')), aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity)), alpha=0.30, show.legend=FALSE)+
geom_line(aes(colour=ethnicity))+
labs(x=expression('Body Mass Index' ~kg/m^2), y = 'Hazard ratio (reference ethnicity==white british and BMI==22.5)')+
theme_few() +# scale_colour_lancet() + scale_fill_lancet()+
ggtitle('Fit2, ethnicity')+
  theme(legend.title=element_blank())
HR_plot2_eth_2
  
HR_plot2_eth_all <- ggplot(data=lp_and_bmi2, aes(x=bmi, y=HR, group=ethnicity))+
#geom_ribbon(data=subset(lp_and_bmi2, ethnicity == c('White British', 'Indian','Mixed','Pakistani',
 #                                                'Chinese', 'Bangladeshi')), aes(ymin=HRci2, ymax=HRci1, fill=factor(ethnicity)), alpha=0.30, show.legend=FALSE)+
geom_line(aes(colour=ethnicity))+
labs(x=expression('Body Mass Index' ~kg/m^2), y = 'Hazard ratio (reference ethnicity==white british and bmi==22.5)')+
theme_few() +# scale_colour_lancet() + scale_fill_lancet()+
ggtitle('Fit2, ethnicity')+
theme(legend.title=element_blank())
HR_plot2_eth_all
  


#save out age plots
fn = paste0(out_dir, '/HR_plot2_eth1.png')
png(file=fn, width = 500, height = 500)
HR_plot2_eth_1
dev.off()

fn = paste0(out_dir, '/HR_plot2_eth2.png')
png(file=fn, width = 500, height = 500)
HR_plot2_eth_2
dev.off() 
  
  
fn = paste0(out_dir, '/HR_plot2_eth_all.png')
png(file=fn, width = 500, height = 500)
HR_plot2_eth_all
dev.off() 
#####################################################
# carehome or not carehome
#####################################################
  
table(covid_df_bmi975$residence_2019)

# stratify by type of residence
df_phh <- covid_df_bmi975 %>% filter(residence_2019=='Private household')
df_care <- covid_df_bmi975 %>% filter(residence_2019=='Care home')
  
# fit models
fit_phh <- fit_3_models(4, df_phh)
fit1_phh = fit_phh[[1]]
fit2_phh = fit_phh[[2]]
fit3_phh = fit_phh[[3]]
  
fit1_care <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short + 
              +  poly(age_2020,3) + sex + strata(pr_census_region), df_care, weight=w)

fit2_care <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) +  IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll    
              + proximity_to_others+ exposure_to_disease 
              + ruralurban_name ,
              df_care, weight=w)
  # removed: child_in_hh, keyworker_type, typaccom, n_hh, Multigen_2020,
  # overcrowded, hh_keyworker, deprived

fit3_care <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) +  IMD_decile  
              + scgpuk11+ hlqpuk11 +  tenhuk11_coll+ proximity_to_others
              + exposure_to_disease 
              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
              + b_cirrhosis + b_neurorare + b_parkinsons
              + b_pvd + b_fracture4 +  b_ra_sle + b_semi 
              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
              df_care, weight=w)
  # removed: deprived, typaccom +n_hh+ Multigen_2020 
  # overcrowded + keyworker_type  + hh_keyworker + child_in_hh

  # find mediansand set up baseline people for carehomes...
ordered_cat <- c('IMD_decile', 'scgpuk11', 'hlqpuk11')
continuous_cat <- c('age_2020', 'pop_density', 'proximity_to_others', 'exposure_to_disease')
nonordered_cat <- c('sex', 'pr_census_region', 'pop_density_indicator', 'tenhuk11_coll',
                  'b_cerebralpalsy', 'b_asthma', 'b_AF', 'b_chd', 'b_bloodcancer', 
                  'b_copd', 'b_pulmrare', 'b_dementia', 'diabetes_cat', 'learncat', 'b_CCF',
                  'b_cirrhosis',   'b_neurorare',  'b_parkinsons', 'b_pvd', 'b_fracture4',
                  'b_ra_sle', 'b_semi', 'b_stroke', 'b_vte',  'b2_82', 'b2_leukolaba',
                  'b2_prednisolone', 'renalcat', 'p_solidtransplant', 'b_respcancer',
                  'b_epilepsy', 'ruralurban_name')
ignore <- c('bmi')

#df_care$n_hh_f<- factor(df_care$n_hh, order=TRUE, levels=c('0-2', '3-4', '5-6', 'X'))
#median(as.numeric(df_care$n_hh_f), na.rm=TRUE)
for (i in ordered_cat){
  print(i)
  print(median(as.numeric(as.character(unlist(df_care[i]))), na.rm=TRUE))
}
for (i in continuous_cat){
  print(i)
  print(median(as.numeric(unlist(df_care[i]))), na.rm=TRUE)
}
for (i in nonordered_cat){
  print(i)
  print(mfv(unlist(df_care[i]), na_rm=TRUE))
}
  
 pred_baselines_care <- data.frame(ethnicity_short=rep(c('White', 'Black', 'Other', 'South Asian'),
                                               each=151),
                            bmi = rep(seq(20,35,0.1), 4)) %>%
  
  mutate(age_2020 = 88, sex='2', pr_census_region='NW', 
        pop_density_indicator = 0, pop_density=3303, deprived= '2',IMD_decile='6',
        scgpuk11 = '3', hlqpuk11 = '10', proximity_to_others = 0, exposure_to_disease=0,
        hh_keyworker = 'N', child_in_hh = '0.0', tenhuk11_coll=0,
        
        b_cerebralpalsy=0, b_asthma=0, b_AF=0, b_chd =0, b_bloodcancer =0, 
        b_copd=0, b_pulmrare =0, b_dementia =0, diabetes_cat=0, learncat=0, b_CCF=0,
        b_cirrhosis=0,   b_neurorare=0,  b_parkinsons=0, b_pvd=0, b_fracture4=0,
        b_ra_sle=0, b_semi =0, b_stroke=0, b_vte=0,  b2_82=0, b2_leukolaba =0,
        b2_prednisolone=0, renalcat=0, p_solidtransplant=0, b_respcancer=0, b_epilepsy=0, ruralurban_name='Urban city and town')

pred_baselines_care$sex <- as.factor(pred_baselines_care$sex)
pred_baselines_care$IMD_decile <- as.factor(pred_baselines_care$IMD_decile)
pred_baselines_care$deprived <- as.factor(pred_baselines_care$deprived)
pred_baselines_care$scgpuk11 <- as.factor(pred_baselines_care$scgpuk11)
#pred_baselines_care$overcrowded <- as.factor(pred_baselines_care$overcrowded)
pred_baselines_care$hlqpuk11 <- as.factor(pred_baselines_care$hlqpuk11)
pred_baselines_care$tenhuk11_coll <- as.factor(pred_baselines_care$tenhuk11_coll)


pred_lp_care <- predict(fit2_care, pred_baselines_care, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_care$fit,
                        se = pred_lp_care$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_care <- cbind(pred_baselines_care, pred_lp_df1)
reference <- subset(lp_and_bmi_care, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_care <- lp_and_bmi_care %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_care <- HR_plot_fun(lp_and_bmi_care, 'Fit 2 - stratified for care homes') 
HR_plot_care <- HR_plot_care + ylim(0,8)
  
# private households
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=151),
                            bmi = rep(seq(20,35,0.1), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)

pred_lp_phh <- predict(fit2_phh, pred_baselines, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_phh$fit,
                        se = pred_lp_phh$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_phh <- cbind(pred_baselines, pred_lp_df1)
reference <- subset(lp_and_bmi_phh, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_phh <- lp_and_bmi_phh %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_phh <- HR_plot_fun(lp_and_bmi_phh, 'Fit 2 - stratified for private households') 
HR_plot_phh <- HR_plot_phh + ylim(0,8)
  
  

  fn = paste0(out_dir, '/HR_plot2_phh.png')
png(file=fn, width = 500, height = 500)
HR_plot_phh
dev.off() 
  
  
  fn = paste0(out_dir, '/HR_plot2_carehome.png')
png(file=fn, width = 500, height = 500)
HR_plot_care
dev.off() 

  
# phh and carehome for fit 3
pred_lp_care <- predict(fit3_care, pred_baselines_care, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_care$fit,
                        se = pred_lp_care$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_care <- cbind(pred_baselines_care, pred_lp_df1)
reference <- subset(lp_and_bmi_care, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_care <- lp_and_bmi_care %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_care3 <- HR_plot_fun(lp_and_bmi_care, 'Fit 3 - stratified for care homes')+
  ylim(0,6)
HR_plot_care3
  
# private households
pred_baselines <- data.frame(ethnicity_short=rep(c('Black', 'Other', 'South Asian', 'White'), each=151),
                            bmi = rep(seq(20,35,0.1), 4))
pred_baselines <- baseline_chars_fun(pred_baselines)

pred_lp_phh <- predict(fit3_phh, pred_baselines, type='lp', se.fit=TRUE) #makes baseline table plus cols for the lp and se of lp
pred_lp_df1<- data.frame(fit = pred_lp_phh$fit,
                        se = pred_lp_phh$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi_phh <- cbind(pred_baselines, pred_lp_df1)
reference <- subset(lp_and_bmi_phh, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_phh <- lp_and_bmi_phh %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_phh3 <- HR_plot_fun(lp_and_bmi_phh, 'Fit 3 - stratified for private households')+
  ylim(0,6)
HR_plot_phh3 
  

  fn = paste0(out_dir, '/HR_plot3_phh.png')
png(file=fn, width = 500, height = 500)
HR_plot_phh3
dev.off() 
  
  
  fn = paste0(out_dir, '/HR_plot3_carehome.png')
png(file=fn, width = 500, height = 500)
HR_plot_care3
dev.off() 
  
  
#####################################################
# by wave
#####################################################

# date of deatH?
# how to?
head(covid_df_bmi975$dod)
# wave 1: 24th jan 2020 - 31st august 2020
df_wave1_deaths <- covid_df_bmi975 %>% filter(("2020-01-24" <= dod) & (dod < "2020-09-01"))
# wave 2: 1st sep 2020 - 31 dec 2020
df_wave2_deaths <- covid_df_bmi975 %>% filter(("2020-09-01" <= dod) & (dod < "2020-12-31"))

# alive
alive <- covid_df_bmi975 %>% filter(death==0)
  
# join the deahs onto the alive people
df_wave1 <- df_wave1_deaths %>% rbind(alive)
df_wave2 <- df_wave2_deaths %>% rbind(alive)
  
# fit models on each
fit_wave1 <- fit_3_models(6, df_wave1)
fit_wave2 <- fit_3_models(6, df_wave2)

pred_baselines <- pred_baselines %>% baseline_chars_fun()

  # wave 1, fit 2
pred_lp_w1 <- predict(fit_wave1[[2]], pred_baselines, type='lp', se.fit=TRUE)
pred_lp_df <- data.frame(fit = pred_lp_w1$fit,
                        se = pred_lp_w1$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_w1 <- cbind(pred_baselines, pred_lp_df)
reference <- subset(lp_and_bmi_w1, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_w1 <- lp_and_bmi_w1 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_w1<- HR_plot_fun(lp_and_bmi_w1, 'Fit 2 -  wave1') + ylim(0,10)
HR_plot_w1
  
  #
pred_lp_w2 <- predict(fit_wave2[[2]], pred_baselines, type='lp', se.fit=TRUE)
pred_lp_df <- data.frame(fit = pred_lp_w2$fit,
                        se = pred_lp_w2$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)
lp_and_bmi_w2 <- cbind(pred_baselines, pred_lp_df)
reference <- subset(lp_and_bmi_w2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi_w2 <- lp_and_bmi_w2 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_w2<- HR_plot_fun(lp_and_bmi_w2, 'Fit 2 -  wave2')+ ylim(0,10)
HR_plot_w2 
  

  fn = paste0(out_dir, '/HR_plot2_wave1.png')
png(file=fn,width = 500, height = 500)
HR_plot_w2
dev.off() 
  
  
  fn = paste0(out_dir, '/HR_plot2_wave2.png')
png(file=fn, width = 500, height = 500)
HR_plot_w1
dev.off() 

#### Run Rgression_analysis.R before this ####

# author: a.summerfield
# description: analysis of shoenfeld residuals



  
#####################
# shoenfield resid
#####################
  
fit3 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
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
              covid_df_bmi975, weight=w)


test.ph <- cox.zph(fit3, transform='km', terms=TRUE)
#test.ph <- cox.zph(fit3f, transform='km')
  # the significance vas are: ethnicity, bmi, deprived, pop density, age, scgp, hlq
  # AF, CHD, copd, leukolaba, ruralurban, bmi*eth

    
## Schoenfield residuals ##

# function                 
get_s_resid <- function(mod, exposure_interest_ph ){
ph.test <- cox.zph(mod,transform="km", terms=FALSE)
  print(summary(ph.test))
labels=as.numeric(unlist(attr(ph.test$y, "dimnames")[1]))
 
mod_terms <- names(mod$coefficients)
mod_terms_n <- grep(exposure_interest_ph, mod_terms)
mod_terms_select <- mod_terms[mod_terms_n] 
    
# get all schoenfield residuals for exposures of interest
ph_test_df <- map_dfc(mod_terms_n , function(x){
 d <- data.frame(y = ph.test$y[ ,x]) 
  
  names(d) <-mod_terms[x]
  d
})  %>%
mutate(t =labels)%>%
                 select(t, everything())
ph_test_df
}

test <- get_s_resid(mod=fit3, exposure_interest_ph='age_2020')
test <- get_s_resid(mod=fit3, exposure_interest_ph='deprived')
test <- get_s_resid(mod=fit3, exposure_interest_ph='ns(bmi, df=4)*ethnicity_short')
test <- get_s_resid(mod=fit3f, exposure_interest_ph='bmi')

  
###########
  # rsidual test
ph.test <- cox.zph(fit3,transform="km", terms=FALSE) # true just gives us NAs
  print(summary(ph.test))
labels=as.numeric(unlist(attr(ph.test$y, "dimnames")[1]))
   
# get all schoenfield residuals for exposures of interest
d <- data.frame(y = ph.test$y[ ,1:4]) 
names(d) <-names(fit3$coefficients[1:4])
ph_test_df <- d %>% mutate(t =labels)%>%
                 select(t, everything())
dim(ph_test_df)

#graph
  sc_resid1 <- sc_resid_plot(ph_test_df, 2)
  sc_resid2 <- sc_resid_plot(ph_test_df, 3)
  sc_resid3 <- sc_resid_plot(ph_test_df, 4)
  sc_resid4 <- sc_resid_plot(ph_test_df, 5)
  sc_resid1
  sc_resid2
  sc_resid3
  sc_resid4
    
# coxph model & residual test with bmi as linear term:
fit3l <- coxph(Surv(t+1, death_covid) ~ bmi +ethnicity_short + bmi*ethnicity_short
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
              covid_df_bmi975, weight=w)

# cox.zph test Shoenfield residuals
ph.test2 <- cox.zph(fit3l,transform='km', terms=FALSE) # true just gives us NAs
  print(summary(ph.test2))
  head(ph.test2)

## plot with ggplot:
labels=as.numeric(unlist(attr(ph.test2$y, "dimnames")[1]))    
# get all schoenfield residuals for exposures of interest over time:
d <- data.frame(y = ph.test2$y[ ,1]) 
names(d) <-names(fit3l$coefficients[1])
ph_test_df2 <- d %>% mutate(t =labels)%>%
                 select(t, everything())
dim(ph_test_df2)
sc_resid_linear <- sc_resid_plot(ph_test_df2, 2)

  sc_resid2<-  ggplot(data = ph_test_df2, aes(x=t, y=ph_test_df2[,2]))+
  geom_point(alpha= 1/10)+  
  geom_smooth(colour="red", fill ="red", alpha=0.2)+
    #geom_line(aes(y=lhr), linetype=2)+
    geom_hline(yintercept = 0)+
    labs(x="Time at risk", y="b(t)", title ='bmi')+
    theme_bw(base_size = 15)#+
  
fn = paste0(out_dir, '/sc_resid.png')
png(file=fn, width = 800, height = 800)
sc_resid
dev.off() 

  fn = paste0(out_dir, '/sc_resid2.png')
png(file=fn, width = 800, height = 800)
sc_resid2
dev.off() 
  

## plot with plot.cox.zph.  from survival package:
sch_resid_plot <- plot(ph.test2, var=1,xlab="Time", lty=3, col=3, lwd=1)
  
#abline(0, 0, lty=3, col=4) 
# Add the linear fit as well  
abline(lm(ph.test2$y[,1] ~ ph.test2$x)$coefficients, lty=4, col=5)  

# try plotting the splines with plot?
plot(ph.test, var=3,xlab="Time", lty=3, col=3, lwd=1)

#model and schoenfield resids with narrower time window
  fit3_time <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4)*ethnicity_short
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
              subset(covid_df_bmi975, t>100), weight=w)

ph_test_time <- cox.zph(fit3_time,transform="km", terms=FALSE) # true just gives us NAs
d <- data.frame(y = ph_test_time$y[ ,1:4]) 
labels<-as.numeric(unlist(attr(ph_test_time$y, "dimnames")[1]))
names(d) <-names(fit3_time$coefficients[1:4])
ph_test_time_df <- d %>% mutate(t =labels)%>%
                 select(t, everything())

sc_resid_plot <- function(df, i){
  sc_resid<- ggplot(data = df, aes(x=t, y=df[,i]))+
    geom_smooth(colour="gray60", fill ="gray60", alpha=0.2)+
    #geom_line(aes(y=lhr), linetype=2)+
    geom_hline(yintercept = 0)+
    #geom_point(alpha= 1/20)+
    labs(x="Time at risk", y="b(t)", title ='bmi')+
    theme_bw(base_size = 15)#+
  return(sc_resid)
}
  
  
sc_resid1_t<- sc_resid_plot(ph_test_time_df, 2)
sc_resid2_t<- sc_resid_plot(ph_test_time_df,3)
sc_resid3_t<- sc_resid_plot(ph_test_time_df,4)
sc_resid4_t<- sc_resid_plot(ph_test_time_df,5)
par(mfrow=c(2,2))
sc_resid1
sc_resid2
sc_resid3
sc_resid4


  
#survival plots
plot(survfit(Surv(t+1, death_covid)~1, data=covid_df_bmi975))
plot(survfit(Surv(t+1, death_covid)~1, data=subset(covid_df_bmi975, bmi<25)))
plot(survfit(Surv(t+1, death_covid)~1, data=subset(covid_df_bmi975, (bmi<35 & bmi>25))))
plot(survfit(Surv(t+1, death_covid)~1, data=subset(covid_df_bmi975, (bmi<40 & bmi>35))))
  
  
# log(-log()) plots
test_fit3 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=4), covid_df_bmi975, weight=w)

plot(survfit(test_fit3), fun="cloglog")
  
test_surv <- Surv(covid_df_bmi975$t, covid_df_bmi975$death_covid)
q100 = quantile(covid_df_bmi975$bmi, c(0, 0.25, 0.5, 0.75,1))
covid_df_bmi975 <- covid_df_bmi975 %>%
  mutate( q = case_when(
    bmi <= 23.2 ~ 1,
    bmi <=26.3 & bmi>23.2 ~ 2,
    bmi<= 29.8 & bmi>26.3 ~ 3,
    bmi > 29.8 ~4,
  TRUE ~ -1))
plot(survfit(test_surv ~ covid_df_bmi975$q), 
     col=c('black', 'red', 'blue', 'green', 'orange'),
     fun="cloglog", ylab='log(-log(survival))', xlab='t', xlim=c(50,300))

#####
# create dummy variables for bmi quartile - uses the bmi itself:
#covid_df_bmi975 <- covid_df_bmi975 %>%
#  mutate(bmi_q1 = case_when(q==1 ~ bmi, TRUE ~ 0),
#         bmi_q2 = case_when(q==2 ~ bmi, TRUE ~ 0),
#         bmi_q3 = case_when(q==3 ~ bmi, TRUE ~ 0),
#         bmi_q4 = case_when(q==4 ~ bmi, TRUE ~ 0),
#        bmi_q1 = na_if(bmi_q1,0),
#        bmi_q2 = na_if(bmi_q2,0),
#        bmi_q3 = na_if(bmi_q3,0),
#        bmi_q4 = na_if(bmi_q4,0))
#  
#  
#  fit3_bmi_q <- coxph(Surv(t+1, death_covid) ~ 
#              bmi_q1 + bmi_q2 + bmi_q3 + bmi_q4
#              +bmi_q1*ethnicity_short + bmi_q2*ethnicity_short + bmi_q3*ethnicity_short + bmi_q4*ethnicity_short +
#              +ethnicity_short
#              + poly(age_2020,3) + sex + strata(pr_census_region)
#              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
#              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
#              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
#              + hh_keyworker + child_in_hh
#              + b_cerebralpalsy + b_asthma + b_AF + b_chd + b_bloodcancer  
#              + b_copd + b_pulmrare + b_dementia + diabetes_cat+learncat + b_CCF
#              + b_cirrhosis + b_neurorare + b_parkinsons
#              + b_pvd + b_fracture4 +  b_ra_sle + b_semi 
#              + b_stroke + b_vte +  b2_82 + b2_leukolaba + b2_prednisolone
#              + renalcat + p_solidtransplant + ruralurban_name + b_respcancer + b_epilepsy,
#              covid_df_bmi975, weight=w)
#  
#ph_test_bmiq <- cox.zph(fit3_bmi_q,transform='km', terms=FALSE) 

# create actual dummy var
q4= quantile(covid_df_bmi975$bmi, c(0, 0.25, 0.5, 0.75,1))
q5 = quantile(covid_df_bmi975$bmi, c(0, 0.2, 0.4, 0.6, 0.8,1))
q10 <-quantile(covid_df_bmi975$bmi, c(0, 0.1, 0.2,0.3, 0.4,0.5, 0.6, 0.7, 0.8,0.9,1))
q10  
covid_df_bmi975 <- covid_df_bmi975 %>%
  
  mutate( q = case_when(
    bmi <= 23.2 ~ 1,
    bmi <=26.3 & bmi>23.2 ~ 2,
    bmi<= 29.8 & bmi>26.3 ~ 3,
    bmi > 29.8 ~4,
  TRUE ~ -1)) %>%
  
  mutate(bmi_d1 = case_when(q==1 ~ 1, TRUE ~ 0),
         bmi_d2 = case_when(q==2 ~ 1, TRUE ~ 0),
         bmi_d3 = case_when(q==3 ~ 1, TRUE ~ 0),
         bmi_d4 = case_when(q==4 ~ 1, TRUE ~ 0)) %>%
  
   mutate( q5 = case_when(
    bmi <= 22.5 ~ 1,
    bmi <=25.1 & bmi>22.5 ~ 2,
    bmi<= 27.55 & bmi>25.1 ~ 3,
    bmi > 27.55 & bmi <=30.87 ~4,
    bmi >30.87~5,
    TRUE ~ -1)) %>%
  
  mutate(bmi_d5_1 = case_when(q5==1~1, TRUE~0),
        bmi_d5_2 = case_when(q5==2~1, TRUE~0),
        bmi_d5_3 = case_when(q5==3~1, TRUE~0),
        bmi_d5_4 = case_when(q5==4~1, TRUE~0),
        bmi_d5_5 = case_when(q5==5~1, TRUE~0)) %>%
  
  mutate(q10 = case_when(
    bmi<20.8 ~1,
    bmi >= 20.8 & bmi <22.5 ~2,
    bmi >= 22.5 & bmi < 23.9 ~3,
    bmi >= 23.9 & bmi < 25.1 ~ 4,
    bmi >= 25.1 & bmi < 26.3 ~5,
    bmi >= 26.3 & bmi < 27.55 ~ 6,
    bmi >= 27.55 & bmi < 29 ~7,
    bmi >= 29 & bmi < 30.87 ~8,
    bmi >= 30.87 & bmi <33.70 ~9,
    bmi >= 33.7~10,
    TRUE ~ -1)) %>%
  mutate(q10 = as.factor(q10))%>%
  
  mutate(bmi_qf = as_factor(q)) %>%
  mutate(bmi_cat = case_when(
    bmi < 25 ~ 'norm',
    bmi < 30 & bmi >= 25 ~ 'overweight',
    bmi >= 30 ~ 'obese',
    TRUE ~ 'other')) %>%
  mutate(c1 = case_when(bmi_cat == 'norm' ~ 1, TRUE~0),
         c2 = case_when(bmi_cat=='overweight'~1, TRUE ~ 0),
          c3 = case_when(bmi_cat=='obese'~1, TRUE~0))
  
  
  
  table(covid_df_bmi975$c1)
  table(covid_df_bmi975$c2)
  table(covid_df_bmi975$c3)
  
        
  
 # fit model 3 with dummy vars 
  fit3_bmi_qd <- coxph(Surv(t+1, death_covid) ~ 
              bmi_d2 + bmi_d3 +bmi_d4 # leav eout bmi_q1 bc if bmi q4, its what is being comapred to
              +bmi_d2*ethnicity_short + bmi_d3*ethnicity_short + bmi_d4*ethnicity_short
              +ethnicity_short
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
              covid_df_bmi975, weight=w)
  
  ph_test_bmiqd <- cox.zph(fit3_bmi_qd,transform='km', terms=FALSE) # true just gives us NAs

  # in model summary comes out as same if coded as factor
    fit3_bmi_qf <- coxph(Surv(t+1, death_covid) ~ 
              bmi_qf # leav eout bmi_q4 bc if bmi q4, its what is being comapred to
              +bmi_qf*ethnicity_short 
              +ethnicity_short
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
              covid_df_bmi975, weight=w)
  
  ph_test_bmi_qf=cox.zph(fit3_bmi_qf,transform='km', terms=FALSE) # true just gives us NAs

  # fit with quntile dummary vars
  fit3_bmi_d5 <- coxph(Surv(t+1, death_covid) ~ 
               bmi_d5_2 +bmi_d5_3 + bmi_d5_4 + bmi_d5_5 # leav eout bmi_q1 bc if bmi q4, its what is being comapred to
              +bmi_d5_2*ethnicity_short + bmi_d5_3*ethnicity_short + bmi_d5_4*ethnicity_short +  bmi_d5_5*ethnicity_short
              +ethnicity_short
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
              covid_df_bmi975, weight=w)
  
  ph_test_bmi_q5=cox.zph(fit3_bmi_d5,transform='km', terms=FALSE) # true just gives us NAs

# fit modelw tih deciles (as factor)
 fit3_bmi_q10 <- coxph(Surv(t+1, death_covid) ~ 
              q10 # leav eout bmi_q4 bc if bmi q4, its what is being comapred to
              +q10*ethnicity_short 
              +ethnicity_short
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
              covid_df_bmi975, weight=w)
  
  ph_test_bmi_q10=cox.zph(fit3_bmi_q10,transform='km', terms=FALSE) # true just gives us NAs
  
  
# fit model 3 with categories
  
fit3_bmi_c <- coxph(Surv(t+1, death_covid) ~ 
              c2 + c3 # leav eout bmi_q1 bc if bmi q4, its what is being comapred to
              +c2*ethnicity_short + c3*ethnicity_short 
              +ethnicity_short
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
              covid_df_bmi975, weight=w)
  
  ph_test_bmic <- cox.zph(fit3_bmi_c,transform='km', terms=FALSE) # true just gives us NAs
ph_test_bmic



# author: a.summerfield
# description: analysis looking at impact of different spline parameters
# Run Rgression_analysis.R before this




############################################################
# check : how different are HR plots for spline with 5 df? #
############################################################
## fit 2 ##

models_df5 <- fit_3_models(5, covid_df_bmi975)
fit1_df5 <- models_df5[[1]]
fit2_df5 <- models_df5[[2]]
fit3_df5 <- models_df5[[3]]

pred_lp_se2_df5 <- predict(fit2_df5, pred_baselines, type='lp', se.fit=TRUE)
pred_risk2_df5 <- predict(fit2_df5, pred_baselines, type='risk', se.fit=TRUE)
pred_lp_df2_df5 <- data.frame(fit = pred_lp_se2_df5$fit,
                        se = pred_lp_se2_df5$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit -(1.96*se)) %>%
              select(fit, ci1, ci2)

lp_and_bmi2_df5 <- cbind(pred_baselines, pred_lp_df2_df5)
reference <- subset(lp_and_bmi2_df5, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2_df5 <- lp_and_bmi2_df5 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot2_df5 <- HR_plot_fun(lp_and_bmi2_df5, 'Fit 2 - age, sex, demographic variables. spline has df=5')

## fit 3 ##
pred_lp_se3_df5<- predict(fit3_df5, pred_baselines, type='lp', se.fit=TRUE)
pred_risk3_df5 <- predict(fit3_df5, pred_baselines, type='risk', se.fit=TRUE)

pred_lp_df3_df5 <- data.frame(fit = pred_lp_se3_df5$fit,
                        se = pred_lp_se3_df5$se.fit) %>%
              mutate(ci1 = fit+(1.96*se),
                    ci2 = fit - (1.96*se) %>%
              select(fit, ci1, ci2)
lp_and_bmi3_df5 <- cbind(pred_baselines, pred_lp_df3_df5)

reference <- subset(lp_and_bmi3_df5, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi3_df5 <- lp_and_bmi3_df5 %>% mutate(HR = exp(fit - rb),
                                    HRci1 = exp(ci1- rb),
                                    HRci2 = exp(ci2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot3_df5 <- HR_plot_fun(lp_and_bmi3_df5,'Fit 3 - age, sex, demographic variables and health&disease variables. spline df=5')

HR_plot3_df5
HR_plot2_df5

fn = paste0(out_dir, '/HR_plot2_df5.png')
png(file=fn)
HR_plot2_df5
dev.off()


fn = paste0(out_dir, '/HR_plot3_df5.png')
png(file=fn)
HR_plot3_df5
dev.off()

  
########################
# sensitivity analysis #
########################
  
# compare splines visually
# compare natural splines with differ df (which become different no. knots) (rememebr - natural/
# splines are linear beyond the edge knots)
# will use the fit 2 model, change spline parameters

  # compare splines visually
spline_compare <- function(df){
fit2 <- coxph(Surv(t+1, death_covid) ~ ns(bmi, df=df)*ethnicity_short
              + poly(age_2020,3) + sex + strata(pr_census_region)
              + pop_density_indicator*poly(pop_density,2) + deprived + IMD_decile  
              + scgpuk11+ hlqpuk11 + tenhuk11_coll +  typaccom +n_hh+ Multigen_2020 
              + overcrowded + keyworker_type + proximity_to_others+ + exposure_to_disease 
              + hh_keyworker + child_in_hh,
              covid_df_bmi975, weight=w)
  pred_lp_se2 <- predict(fit2, pred_baselines, type='lp', se.fit=TRUE)
pred_risk2 <- predict(fit2, pred_baselines, type='risk', se.fit=TRUE)

pred_lp_df2 <- data.frame(fit = pred_lp_se2$fit,
                        se = pred_lp_se2$se.fit) %>%
              mutate(se1 = fit+se,
                    se2 = fit - se) %>%
              select(fit, se1, se2)

lp_and_bmi2 <- cbind(pred_baselines, pred_lp_df2)

reference <- subset(lp_and_bmi2, ethnicity_short=='White' &bmi==22.5) %>% select(fit)
rb <- reference[[1,1]] # the reference is eth==white and bmi==22 (TODO: use 22.5)
lp_and_bmi2 <- lp_and_bmi2 %>% mutate(HR = exp(fit - rb),
                                    HRse1 = exp(se1- rb),
                                    HRse2 = exp(se2 -rb)) # since HR = Ho*exp(B2X)/Ho*exp(B1X), can simplify toHR= exp(B2-B1)

HR_plot_spline <- ggplot(data=lp_and_bmi2, aes(x=bmi, y=HR, group=ethnicity_short))+
geom_ribbon(data=lp_and_bmi2, aes(ymin=HRse2, ymax=HRse1, fill=factor(ethnicity_short)), alpha=0.30)+
geom_line(aes(colour=ethnicity_short))+
labs(x='BMI', y = 'Hazard ratio (reference ethnicity==white and bmi==22.5)')+
theme(legend.title=element_blank())+
ggtitle(paste0('Fit 2 - age, sex, demographic variables, spline with df=', df))
  
return(HR_plot_spline)
}
spline1 <- spline_compare(1)
spline2 <- spline_compare(2)
spspline6 <- spline_compare(6)
spline8 <- spline_compare(8)
spline12 <- spline_compare(12)
  
fn = paste0(out_dir, '/spline_df2.png')
png(file=fn)
spline2
dev.off()
fn = paste0(out_dir, '/spline_df6.png')
png(file=fn)
spline6
dev.off()
fn = paste0(out_dir, '/spline_df8.png')
png(file=fn)
spline8
dev.off()
  
fn = paste0(out_dir, '/spline_df12.png')
png(file=fn)
spline12
dev.off()

  
