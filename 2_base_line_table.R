# this code aims to create a summary stats table, split by ethnicity for the dataset we sample from: Namely: a ds of gpes linked, aged >40.
# creates version with bmi, and version without

library(tidyverse)
library(sparklyr)


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
out_dir= paste0(dir, "/Results")
path <- out_dir

# if using ft_bucketizer: buckets are defined by [x,y)

#################################
## read in data and select vars
#################################

linked_data <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cleaned_linked_census_vn2")%>%
      filter(linked_gpes == 1, age_2020 >=40) %>%
      select(ethnicity_short, death_date=dod, death_covid, bmi, age_2020, sex, pr_census_region, pop_density_indicator,
             pop_density, deprived, IMD_decile, scgpuk11, hlqpuk11, tenhuk11_coll, typaccom, n_hh, Multigen_2020,
             overcrowded, keyworker_type,proximity_to_others, exposure_to_disease, hh_keyworker, child_in_hh,
             b_cerebralpalsy, b_asthma, b_AF, b_chd, b_bloodcancer, b_copd, b_pulmrare, b_dementia, diabetes_cat,learncat,
             b_CCF, b_cirrhosis,b_neurorare, b_parkinsons, b_pvd, b_fracture4, b_ra_sle, b_semi, b_stroke, b_vte,  b2_82,
             b2_leukolaba, b2_prednisolone, renalcat,  p_solidtransplant,ruralurban_name, b_respcancer, b_epilepsy)



# read in the variable name lookup file
var_lookup <- read.csv(paste0('cen_dth_hes/COVID19_BMI', '/varnames_lookup.csv'))%>%
select(variable=names, labels, type)

# read in the meaning (decodings) lookup
value_lookup <- read.csv(paste0('cen_dth_hes/COVID19_BMI', '/values_lookup.csv'))%>%
select(variable, value, value_label) %>%
  mutate(variable = as.character(variable),
        value = as.character(value),
        value_label = as.character(value_label))


#############################################
# define tabulating functions 
#############################################

# creates list of continuous vars - used later to sort out how to treat vars
continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  ) # added bmi
binary_vars <- c()  


get_tables_per_var <- function(df, vars){
  
  res <- list()
  
  for (v in vars){
    print(v)
    
    v <- as.symbol(v)
    v_str <- as.character(v)
    
    df_var <- df %>%
      select(v)
    
    continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  )
    
    if (!(v_str %in% continuous_vars)){
      df_summary <- df_var %>%
        select(v) %>%
        group_by(v) %>%
        tally() %>%
        collect() %>%
        as.data.frame()%>%
        arrange()

      df_summary$percentage <- df_summary$n / sum(df_summary$n) * 100
  
      df_summary$percentage <- round(df_summary$percentage, 2)

      df_summary$n <- format(df_summary$n, big.mark=",", scientific=FALSE)
      df_summary[[v_str]] <- as.character(df_summary[[v_str]])
      
     
      df_summary[['Count (%)']] <- paste0(df_summary$n, ' (', format(df_summary$percentage, nsmall = 2), ')')
      print(df_summary )
      df_summary <- select(df_summary, -n, -percentage)
      
      if (v_str %in% binary_vars){
        df_summary <-df_summary %>%
        filter()
      }
      
    }
    
    else if (v_str %in% continuous_vars){
      df_summary <- df_var %>%
      sdf_describe(cols=v_str) %>%
      filter(summary=='stddev' | summary=='mean') %>%
      collect() %>%
      arrange() %>%
      t() %>%
      as.data.frame()
      
      colnames(df_summary) <- c('mean', 'sd')
      df_summary <- df_summary[-1,]
      df_summary$sd <- as.numeric(as.character(df_summary$sd))
      df_summary$sd <- round(df_summary$sd, 2)
      df_summary$sd <- format(df_summary$sd, big.mark=",", scientific=FALSE)
      df_summary$mean <- as.numeric(as.character(df_summary$mean))
      df_summary$mean <- round(df_summary$mean, 2)
      df_summary$mean <- format(df_summary$mean, big.mark=",", scientific=FALSE)
      
      
      new_col <- rep('temp', nrow(df_summary))
      mean <- df_summary$mean
      sd <- df_summary$sd
      
      for (i in 1:length(new_col)){
        new_col[i] <- paste0(mean[i], ' (', sd[i], ')')
      }
      
      df_summary[['Mean (SD)']] <- new_col
      df_summary <- select(df_summary, -mean, -sd)
      
    }
    
    df_summary$variable <- rep(v_str, nrow(df_summary))
    
    res <- append(res, list(df_summary))
    
    rm(df_var, df_summary)
    gc()
    
  }
  
  return(res)
}


###############################################
# split data into having valid bmi and not
##############################################

# everyone
linked_data_full <- linked_data

# but we do want to run this just on people with valid bmi
linked_data_p2 <- linked_data %>%
    filter(isNotNull(bmi))

# just people without bmi
linked_data <- linked_data %>%
  filter(isNull(bmi))


#count_na <- function(x){sum(is.na(x))}
#d <- collect(test)
#na_summary <- d %>%
#  summarise_all(count_na) %>%
#  t()

###################################################################################
# get summary stats and compare on the data WITH valid bmi and missing BMI  #
###################################################################################

## first section: uses function to create summary stats ad saves (takes a long time!)
ignore <- c('death_covid', 
             'death_date', 'occ_custom', 'occ')

# create summary stats
columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)]
#columns <-"pr_census_region"
sum_stats <- get_tables_per_var(linked_data, columns)
sum_stats_p2 <- get_tables_per_var(linked_data_p2, columns)

# save out summary stats
path <- out_dir

if (!file.exists(path)){
  dir.create(path)
}

#saveRDS(sum_stats, paste0(path, '/bmi_summary_stats_missingbmi.rds'))
#saveRDS(sum_stats_p2, paste0(path, '/bmi_summary_stats_p2_havebmi.rds'))


## 2nd section: reads in summary stats and wrangles into a table (quick!)
## read in summary stats
#sum_stats <- readRDS(paste0(path,'/bmi_summary_stats_missingbmi.rds'))
#sum_stats_p2 <- readRDS(paste0(path,'/bmi_summary_stats_p2_havebmi.rds'))


# redefine continuous var (this needs to match the list in 'define tabulating functions' section'.
# if running wholte script in one go this  line isnt needed)
continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  )

# for the cat/con (isNull) and cat_covid_p2/con_covid_p2 (isNotNull) select the right vars
ind <- c(sapply(continuous_vars, function(x) which(columns == x)[[1]])) # create list of the contiue var
cat <- sum_stats[-ind]
con <- sum_stats[ind]
cat_covid_p2 <- sum_stats_p2[-ind]
con_covid_p2 <- sum_stats_p2[ind]

## next section does manipulation of cat and con to sort values, bring into 1 df, label correctly
# within each variable's df, sort by value - e.g. for sex sort so 1 then 2
for (i in 1:length(cat)){
  print(colnames(cat[[i]]))
  colnames(cat[[i]]) <- c('value', 'Count (n)', 'variable')
  cat[[i]]<- arrange(cat[[i]],value)
}

for (i in 1:length(cat_covid_p2)){
  colnames(cat_covid_p2[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_covid_p2[[i]]<- arrange(cat_covid_p2[[i]], value)
}

# for the con and cat:
# rbind the variables individual dfs together
cat <- do.call('rbind', cat) 
# use the lookup to get the variable's labels joined on
cat <- cat %>% left_join(var_lookup)%>% group_by(variable)%>%
  filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
  as.data.frame()

con <- do.call('rbind', con)%>%
  mutate(value="", labels="", type="")
names(con) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )
  
rbind(cat, con)
cat %>% select(labels, variable, value, `Count (n)`)
  
# for the cat_covid_p2 and con_covid_p2:
# rbind the indivudal variabl'es dfs togther for cat_covid_p2
cat_covid_p2 <- do.call('rbind', cat_covid_p2)  
#use the look up to get the value names
cat_covid_p2 <- cat_covid_p2 %>% left_join(var_lookup)%>% group_by(variable)%>%
  filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
  as.data.frame()

con_covid_p2 <- do.call('rbind', con_covid_p2)%>%
  mutate(value="", labels="", type="")
names(con_covid_p2) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )
  
## next section joins the cat with cat_covid_p2, and the con with con_covid_p2, in order to compare between cases With and Without BMI
# join the 2 continuous dfs toger: compare between the con and covid_con
con_2 <- con_covid_p2 %>% select('Count (n)', 'value')  # select the needed columns
con_compare <- con %>% select('value', 'Count (n)') %>% # select the needed columns and merge together. 
  left_join(con_2, by=c('value'='value'), suffix = c(' full dataset', ' valid bmi')) %>%
  rename('variable'=1, 'Mean (SD) cases without BMI'=2, 'Mean (SD) cases with BMI'=3 )
con_compare <- con_compare %>% left_join(var_lookup, by=c('variable'='variable')) 
#con_compare %>% select(-c(type, variable)) %>% rename(variable=labels)%>% select(variable, .)
  
# labelling the categorical vars:
# join the cat and covid_cat  compare between the cat and covid_cat
cat_2 <- cat_covid_p2 %>% select('variable', 'value', 'Count (n)')
cat_compare <- cat %>% select('labels','variable','value', 'Count (n)') %>%
  left_join(cat_2, by=c('value', 'variable'), suffix = c(' missing bmi',' valid bmi')) %>%
  left_join(value_lookup, by=c('variable', 'value'))%>%
  select(-c('variable')) 
cat_compare_neat <- cat_compare %>%
  select(c('labels', 'value_label', 'Count (n) missing bmi', 'Count (n) valid bmi')) %>%
  rename('Variable'=labels, 'Value'= value_label, 
         'Count (%) for cases without BMI'= 'Count (n) missing bmi',
        'Count (%) for cases with BMI' = 'Count (n) valid bmi')

  
#write_csv(rbind(cat_covid_p2, con_covid_p2), paste0(path, '/desc_all_validbmi.csv'))

#write_csv(con_compare, paste0(path, '/comparebmi_con.csv'))
#write_csv(cat_compare_neat, paste0(path, '/comparebmi_cat.csv'))

write_csv(con_compare, paste0(path, '/comparebmi_con.csv'))
write_csv(cat_compare_neat, paste0(path, '/comparebmi_cat.csv'))

  
########################################################################
# run through with different ethnicities and valid BMI and join together
########################################################################

## First section: filters data and uses function to generate summary stats (takes a long time!)
# Filter table of valid BMIs by ethnicity:

linked_data_w <- linked_data_p2 %>%
    filter(isNotNull(bmi), ethnicity_short=='White')

linked_data_b <- linked_data_p2 %>%
    filter(isNotNull(bmi), ethnicity_short=='Black')

linked_data_sa <- linked_data_p2 %>%
    filter(isNotNull(bmi), ethnicity_short=='South Asian')

linked_data_o <- linked_data_p2 %>%
    filter(isNotNull(bmi), ethnicity_short=='Other')

  
ignore <- c('death_covid', 'death_date', 'occ_custom', 'occ')

## create summary stats by ethnicity
columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)]
#columns <-"pr_census_region"
stats_w <- get_tables_per_var(linked_data_w, columns)
stats_b <- get_tables_per_var(linked_data_b, columns)
stats_sa <- get_tables_per_var(linked_data_sa, columns)
stats_o <- get_tables_per_var(linked_data_o, columns)

## save out summary stats by ethnicity
path <- out_dir

if (!file.exists(path)){
  dir.create(path)
}

saveRDS(stats_w, paste0(path, '/bmi_summary_stats_white.rds'))
saveRDS(stats_b, paste0(path, '/bmi_summary_stats_black.rds'))
saveRDS(stats_sa, paste0(path, '/bmi_summary_stats_southasian.rds'))
saveRDS(stats_o, paste0(path, '/bmi_summary_stats_other.rds'))


## 2nd section: reads in summary stats and wrangles into tables
## read in summary stats by ethnicity
sum_stats_w  <- readRDS(paste0(path,'/bmi_summary_stats_white.rds'))
sum_stats_b <- readRDS(paste0(path,'/bmi_summary_stats_black.rds'))
sum_stats_sa <- readRDS(paste0(path,'/bmi_summary_stats_southasian.rds'))
sum_stats_o <- readRDS(paste0(path,'/bmi_summary_stats_other.rds'))
ignore <- c('death_covid', 'death_date', 'occ_custom', 'occ')

## create summary stats by ethnicity
columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)]

  

# redefine continuous var (this needs to match list in the tabulations functions section
# if running whole script in one go this line isnt needed)
continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  )

  
## select the right vars by ethnicity
ind <- c(sapply(continuous_vars, function(x) which(columns == x)[[1]])) # create list of the contiue var
cat <- sum_stats_w[-ind]
con <- sum_stats_w[ind]
cat_b <- sum_stats_b[-ind]
con_b <- sum_stats_b[ind]
cat_sa <- sum_stats_sa[-ind]
con_sa <- sum_stats_sa[ind]
cat_o <- sum_stats_o[-ind]
con_o <- sum_stats_o[ind]
  
  
# next section does manipulation of cat and con to sort values, bring into 1 df, label correctly
## by ethnicity: within each variable' df, sort by value - e.g. for sex sort so 1 then 2
for (i in 1:length(cat)){
  print(colnames(cat[[i]]))
  colnames(cat[[i]]) <- c('value', 'Count (n)', 'variable')
  cat[[i]]<- arrange(cat[[i]],value)
}

for (i in 1:length(cat_b)){
  colnames(cat_b[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_b[[i]]<- arrange(cat_b[[i]], value)
}

for (i in 1:length(cat_sa)){
  colnames(cat_sa[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_sa[[i]]<- arrange(cat_sa[[i]], value)
}
  
for (i in 1:length(cat_o)){
  colnames(cat_o[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_o[[i]]<- arrange(cat_o[[i]], value)
}

  
## by ethnicity: rbind the variables individual dfs together and join on the variable names 
# white
cat <- do.call('rbind', cat) 
  cat <- cat %>%
    left_join(var_lookup)%>% 
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con <- do.call('rbind', con)%>%
  mutate(value="", labels="", type="")
  names(con) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

# black 
#rbind the indivudal variabl'es dfs togther for cat_b
cat_b <- do.call('rbind', cat_b)  
  cat_b <- cat_b %>%
    left_join(var_lookup)%>% 
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_b <- do.call('rbind', con_b)%>%
  mutate(value="", labels="", type="")
  names(con_b) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )
  
# south asian
cat_sa <- do.call('rbind', cat_sa)  
  cat_sa <- cat_sa %>%
    left_join(var_lookup)%>% 
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_sa <- do.call('rbind', con_sa)%>%
  mutate(value="", labels="", type="")
  names(con_sa) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

# other
cat_o <- do.call('rbind', cat_o)  
  cat_o <- cat_o %>%
    left_join(var_lookup)%>% 
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_o <- do.call('rbind', con_o)%>%
  mutate(value="", labels="", type="")
  names(con_o) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

  
# join the continuous dfs toger: join each onto the white as a spine
  # this join relies on there being no NAs in the value column!!! (NAs results in many-to-many join)
con_b <- con_b %>% select('Count (n)', 'value')  # select the needed columns
con_sa <- con_sa %>% select('Count (n)', 'value')  # select the needed columns
con_o <- con_o %>% select('Count (n)', 'value')  # select the needed columns

con_compare <- con %>% select('value', 'Count (n)') %>% # select the needed columns and merge together. 
  left_join(con_b, by=c('value'='value'), suffix = c('', ' black')) %>%
  left_join(con_sa, by=c('value'='value'), suffix = c('', ' south asian')) %>%
  left_join(con_o, by=c('value'='value'), suffix = c('', ' other')) %>%
  rename('variable'=1, 'Mean (SD) white'=2, 'Mean (SD) black'=3, 'Mean (SD) south asian'=4, 'Mean (SD) other'=5)
  
# labelling the categorical vars:  
# join the cat and covid_cat  compare between the cat and covid_cat
cat_b <- cat_b %>% select('variable', 'value', 'Count (n)')
cat_sa <- cat_sa %>% select('variable', 'value', 'Count (n)')
cat_o <- cat_o %>% select('variable', 'value', 'Count (n)')
  
cat_compare <- cat %>% select('labels','variable','value', 'Count (n)') %>%
  left_join(cat_b, by=c('value', 'variable'), suffix = c('',' black')) %>%
  left_join(cat_sa, by=c('value', 'variable'), suffix = c('',' south asian')) %>%
  left_join(cat_o, by=c('value', 'variable'), suffix = c('',' other')) %>%
  left_join(value_lookup, by=c('variable', 'value'))%>%
  select(-c('variable'))
  
cat_compare_neat <- cat_compare %>%
  select(c('labels', 'value_label', 'Count (n)', 'Count (n) black','Count (n) south asian', 'Count (n) other')) %>%
  rename('Variable'=labels, 'Value'= value_label, 
         'Count (%) white'= 'Count (n)',
        'Count (%) black' = 'Count (n) black',
        'Count (%) south asian' = 'Count (n) south asian',
        'Count (%) other' = 'Count (n) other')

  
#write_csv(rbind(cat_covid_p2, con_covid_p2), paste0(path, '/desc_all_validbmi.csv'))

write_csv(con_compare, paste0(path, '/ethnicity_validbmi_con.csv'))
write_csv(cat_compare_neat, paste0(path, '/ethnicity_validbmi_cat.csv'))

# rename the ethncity * has bmi verisons of the cat and con table
cat_ethnicity <- cat_compare_neat
con_ethnicity <- con_compare
  
########################################
# by ethnicity - only without bmi
########################################
  
## first section
## Filter by ethnicity
# valid bmi and ethnicity white
linked_data_w <- linked_data %>%
    filter(isNull(bmi), ethnicity_short=='White')

linked_data_b <- linked_data %>%
    filter(isNull(bmi), ethnicity_short=='Black')

linked_data_sa <- linked_data %>%
    filter(isNull(bmi), ethnicity_short=='South Asian')

linked_data_o <- linked_data %>%
    filter(isNull(bmi), ethnicity_short=='Other')

  
ignore <- c('death_covid', 'death_date', 'occ_custom', 'occ')

## create summary stats by ethnicity
columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)]
#columns <-"pr_census_region"
stats_w <- get_tables_per_var(linked_data_w, columns)
stats_b <- get_tables_per_var(linked_data_b, columns)
stats_sa <- get_tables_per_var(linked_data_sa, columns)
stats_o <- get_tables_per_var(linked_data_o, columns)

## save out summary stats by ethnicity
path <- out_dir

if (!file.exists(path)){
  dir.create(path)
}

saveRDS(stats_w, paste0(path, '/bmi_summary_stats_white_missingbmi.rds'))
saveRDS(stats_b, paste0(path, '/bmi_summary_stats_black_missingbmi.rds'))
saveRDS(stats_sa, paste0(path, '/bmi_summary_stats_southasian_missingbmi.rds'))
saveRDS(stats_o, paste0(path, '/bmi_summary_stats_other_missingbmi.rds'))



## read in summary stats by ethnicity
sum_stats_w  <- readRDS(paste0(path,'/bmi_summary_stats_white_missingbmi.rds'))
sum_stats_b <- readRDS(paste0(path,'/bmi_summary_stats_black_missingbmi.rds'))
sum_stats_sa <- readRDS(paste0(path,'/bmi_summary_stats_southasian_missingbmi.rds'))
sum_stats_o <- readRDS(paste0(path,'/bmi_summary_stats_other_missingbmi.rds'))

  

# redefine continuous var (this needs to match definition before saving - if running wholte script in one go this
# line isnt needed)
continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  )

  
## select the right vars by ethnicity
ind <- c(sapply(continuous_vars, function(x) which(columns == x)[[1]])) # create list of the contiue var
cat <- sum_stats_w[-ind]
con <- sum_stats_w[ind]
cat_b <- sum_stats_b[-ind]
con_b <- sum_stats_b[ind]
cat_sa <- sum_stats_sa[-ind]
con_sa <- sum_stats_sa[ind]
cat_o <- sum_stats_o[-ind]
con_o <- sum_stats_o[ind]
  
  
# next section does manipulation of cat and con to sort values, bring into 1 df, label correctly
## by ethnicity: within each variable' df, sort by value - e.g. for sex sort so 1 then 2
for (i in 1:length(cat)){
  print(colnames(cat[[i]]))
  colnames(cat[[i]]) <- c('value', 'Count (n)', 'variable')
  cat[[i]]<- arrange(cat[[i]],value)
}

for (i in 1:length(cat_b)){
  colnames(cat_b[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_b[[i]]<- arrange(cat_b[[i]], value)
}

for (i in 1:length(cat_sa)){
  colnames(cat_sa[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_sa[[i]]<- arrange(cat_sa[[i]], value)
}
  
for (i in 1:length(cat_o)){
  colnames(cat_o[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_o[[i]]<- arrange(cat_o[[i]], value)
}

  
## by ethnicity: rbind the variables individual dfs together
# white
cat <- do.call('rbind', cat) 
  cat <- cat %>%
    left_join(var_lookup)%>% # use the lookup to get the variable's labels joined on
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con <- do.call('rbind', con)%>%
  mutate(value="", labels="", type="")
  names(con) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

#rbind(cat, con)
#cat %>% select(labels, variable, value, `Count (n)`)

# black 
#rbind the indivudal variabl'es dfs togther for cat_b
cat_b <- do.call('rbind', cat_b)  
  cat_b <- cat_b %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_b <- do.call('rbind', con_b)%>%
  mutate(value="", labels="", type="")
  names(con_b) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )
  
# south asian
cat_sa <- do.call('rbind', cat_sa)  
  cat_sa <- cat_sa %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_sa <- do.call('rbind', con_sa)%>%
  mutate(value="", labels="", type="")
  names(con_sa) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

# other
cat_o <- do.call('rbind', cat_o)  
  cat_o <- cat_o %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_o <- do.call('rbind', con_o)%>%
  mutate(value="", labels="", type="")
  names(con_o) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

  
# join the 2 continuous dfs toger: compare between the con and covid_con
con_b <- con_b %>% select('Count (n)', 'value')  # select the needed columns
con_sa <- con_sa %>% select('Count (n)', 'value')  # select the needed columns
con_o <- con_o %>% select('Count (n)', 'value')  # select the needed columns

con_compare <- con %>% select('value', 'Count (n)') %>% # select the needed columns and merge together. 
  left_join(con_b, by=c('value'='value'), suffix = c('', ' black')) %>%
  left_join(con_sa, by=c('value'='value'), suffix = c('', ' south asian')) %>%
  left_join(con_o, by=c('value'='value'), suffix = c('', ' other')) %>%
  rename('variable'=1, 'Mean (SD) white (missing bmi)'=2,
         'Mean (SD) black (missing bmi)'=3,
         'Mean (SD) south asian (missing bmi)'=4,
         'Mean (SD) other (missing bmi)'=5)
  
# labelling the categorical vars:  
# join the cat and covid_cat  compare between the cat and covid_cat
cat_b <- cat_b %>% select('variable', 'value', 'Count (n)')
cat_sa <- cat_sa %>% select('variable', 'value', 'Count (n)')
cat_o <- cat_o %>% select('variable', 'value', 'Count (n)')
  
cat_compare <- cat %>% select('labels','variable','value', 'Count (n)') %>%
  left_join(cat_b, by=c('value', 'variable'), suffix = c('',' black')) %>%
  left_join(cat_sa, by=c('value', 'variable'), suffix = c('',' south asian')) %>%
  left_join(cat_o, by=c('value', 'variable'), suffix = c('',' other')) %>%
  left_join(value_lookup, by=c('variable', 'value'))%>%
  select(-c('variable'))
  
cat_compare_neat <- cat_compare %>%
  select(c('labels', 'value_label', 'Count (n)', 'Count (n) black','Count (n) south asian', 'Count (n) other')) %>%
  rename('Variable'=labels, 'Value'= value_label, 
         'Count (%) white (missing bmi)'= 'Count (n)',
        'Count (%) black (missing bmi)' = 'Count (n) black',
        'Count (%) south asian (missing bmi)' = 'Count (n) south asian',
        'Count (%) other (missing bmi)' = 'Count (n) other')

  
#write_csv(rbind(cat_covid_p2, con_covid_p2), paste0(path, '/desc_all_validbmi.csv'))
  
write_csv(con_compare, paste0(path, '/ethnicity_missingbmi_con.csv'))
write_csv(cat_compare_neat, paste0(path, '/ethnicity_missingbmi_cat.csv'))


  # rename the ethnicity + missig bmi versions
cat_ethnicity_nobmi <- cat_compare_neat
con_ethnicity_nobmi <- con_compare
  
####################################################################
# now need to join together the with and without bmi ethnicity
####################################################################

cat_ethnicity_joined <- cat_ethnicity %>% left_join(cat_ethnicity_nobmi, by=c('Variable', 'Value'))
con_ethnicity_joined <- con_ethnicity %>% left_join(con_ethnicity_nobmi, by='variable')
con_ethnicity_joined <- con_ethnicity_joined <- con_compare %>% left_join(var_lookup, by=c('variable'='variable')) 

  
# save out he combined data
write_csv(con_ethnicity_joined, paste0(path, '/table1_ethnicity_comparebmi_con.csv'))
write_csv(cat_ethnicity_joined, paste0(path, '/table1_ethnicity_comparebmi_cat.csv'))


############################################################
# tabulate whole ds by ethnicity (but not by WITH BMI/WITHOUT BMI)
############################################################
 
## first section
## Filter by ethnicity
# valid bmi and ethnicity white
linked_data_w <- linked_data_full %>%
    filter(ethnicity_short=='White')

linked_data_b <- linked_data_full %>%
    filter(ethnicity_short=='Black')

linked_data_sa <- linked_data_full %>%
    filter(ethnicity_short=='South Asian')

linked_data_o <- linked_data_full %>%
    filter(ethnicity_short=='Other')

  
ignore <- c('death_covid', 'death_date', 'occ_custom', 'occ')

## create summary stats by ethnicity
columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)]
#columns <-"pr_census_region"
stats_w <- get_tables_per_var(linked_data_w, columns)
stats_b <- get_tables_per_var(linked_data_b, columns)
stats_sa <- get_tables_per_var(linked_data_sa, columns)
stats_o <- get_tables_per_var(linked_data_o, columns)

## save out summary stats by ethnicity
path <- out_dir

if (!file.exists(path)){
  dir.create(path)
}

saveRDS(stats_w, paste0(path, '/bmi_summary_stats_white_fullsd.rds'))
saveRDS(stats_b, paste0(path, '/bmi_summary_stats_black_fullsd.rds'))
saveRDS(stats_sa, paste0(path, '/bmi_summary_stats_southasian_fullsd.rds'))
saveRDS(stats_o, paste0(path, '/bmi_summary_stats_other_fullsd.rds'))



## read in summary stats by ethnicity
sum_stats_w  <- readRDS(paste0(path,'/bmi_summary_stats_white_fullsd.rds'))
sum_stats_b <- readRDS(paste0(path,'/bmi_summary_stats_black_fullsd.rds'))
sum_stats_sa <- readRDS(paste0(path,'/bmi_summary_stats_southasian_fullsd.rds'))
sum_stats_o <- readRDS(paste0(path,'/bmi_summary_stats_other_fullsd.rds'))

  

# redefine continuous var (this needs to match definition before saving - if running wholte script in one go this
# line isnt needed)
continuous_vars <- c('age_2020','pop_density','exposure_to_disease' , 'proximity_to_others', 'bmi'  )

  
## select the right vars by ethnicity
ind <- c(sapply(continuous_vars, function(x) which(columns == x)[[1]])) # create list of the contiue var
cat <- sum_stats_w[-ind]
con <- sum_stats_w[ind]
cat_b <- sum_stats_b[-ind]
con_b <- sum_stats_b[ind]
cat_sa <- sum_stats_sa[-ind]
con_sa <- sum_stats_sa[ind]
cat_o <- sum_stats_o[-ind]
con_o <- sum_stats_o[ind]
  
  
# next section does manipulation of cat and con to sort values, bring into 1 df, label correctly
## by ethnicity: within each variable' df, sort by value - e.g. for sex sort so 1 then 2
for (i in 1:length(cat)){
  print(colnames(cat[[i]]))
  colnames(cat[[i]]) <- c('value', 'Count (n)', 'variable')
  cat[[i]]<- arrange(cat[[i]],value)
}

for (i in 1:length(cat_b)){
  colnames(cat_b[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_b[[i]]<- arrange(cat_b[[i]], value)
}

for (i in 1:length(cat_sa)){
  colnames(cat_sa[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_sa[[i]]<- arrange(cat_sa[[i]], value)
}
  
for (i in 1:length(cat_o)){
  colnames(cat_o[[i]]) <- c('value', 'Count (n)', 'variable')
  cat_o[[i]]<- arrange(cat_o[[i]], value)
}

  
## by ethnicity: rbind the variables individual dfs together
# white
cat <- do.call('rbind', cat) 
  cat <- cat %>%
    left_join(var_lookup)%>% # use the lookup to get the variable's labels joined on
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con <- do.call('rbind', con)%>%
  mutate(value="", labels="", type="")
  names(con) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

# black 
#rbind the indivudal variabl'es dfs togther for cat_b
cat_b <- do.call('rbind', cat_b)  
  cat_b <- cat_b %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_b <- do.call('rbind', con_b)%>%
  mutate(value="", labels="", type="")
  names(con_b) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )
  
# south asian
cat_sa <- do.call('rbind', cat_sa)  
  cat_sa <- cat_sa %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_sa <- do.call('rbind', con_sa)%>%
  mutate(value="", labels="", type="")
  names(con_sa) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

# other
cat_o <- do.call('rbind', cat_o)  
  cat_o <- cat_o %>%
    left_join(var_lookup)%>% #use the loook up to get the value names
    group_by(variable)%>%
    filter(is.na(type)|type == ""|type=="binary"&as.numeric(value) ==1)%>%
    as.data.frame()

con_o <- do.call('rbind', con_o)%>%
  mutate(value="", labels="", type="")
  names(con_o) <-c('Count (n)', 'value', "variable" , "labels" ,   "type" )

  
# join the continuous dfs toger:join the black, sa, and other tables onto white.
con_b <- con_b %>% select('Count (n)', 'value')  # select the needed columns
con_sa <- con_sa %>% select('Count (n)', 'value')  # select the needed columns
con_o <- con_o %>% select('Count (n)', 'value')  # select the needed columns

con_compare <- con %>% select('value', 'Count (n)') %>% # select the needed columns and merge together. 
  left_join(con_b, by=c('value'='value'), suffix = c('', ' black')) %>%
  left_join(con_sa, by=c('value'='value'), suffix = c('', ' south asian')) %>%
  left_join(con_o, by=c('value'='value'), suffix = c('', ' other')) %>%
  rename('variable'=1, 'Mean (SD) white '=2,
         'Mean (SD) black '=3,
         'Mean (SD) south asian '=4,
         'Mean (SD) other '=5)
  
# labelling the categorical vars:  
# join the cat and covid_cat  compare between the cat and covid_cat
cat_b <- cat_b %>% select('variable', 'value', 'Count (n)')
cat_sa <- cat_sa %>% select('variable', 'value', 'Count (n)')
cat_o <- cat_o %>% select('variable', 'value', 'Count (n)')
  
cat_compare <- cat %>% select('labels','variable','value', 'Count (n)') %>%
  left_join(cat_b, by=c('value', 'variable'), suffix = c('',' black')) %>%
  left_join(cat_sa, by=c('value', 'variable'), suffix = c('',' south asian')) %>%
  left_join(cat_o, by=c('value', 'variable'), suffix = c('',' other')) %>%
  left_join(value_lookup, by=c('variable', 'value'))%>%
  select(-c('variable'))
  
cat_compare_neat <- cat_compare %>%
  select(c('labels', 'value_label', 'Count (n)', 'Count (n) black','Count (n) south asian', 'Count (n) other')) %>%
  rename('Variable'=labels, 'Value'= value_label, 
         'Count (%) white'= 'Count (n)',
        'Count (%) black' = 'Count (n) black',
        'Count (%) south asian' = 'Count (n) south asian',
        'Count (%) other' = 'Count (n) other')

  

write_csv(con_compare, paste0(path, '/ethnicity_fullds_con.csv'))
write_csv(cat_compare_neat, paste0(path, '/ethnicity_fullds_cat.csv'))



  
