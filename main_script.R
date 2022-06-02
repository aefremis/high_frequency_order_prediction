###################################################
# Program Purpose:                                #
# Forecast customer level orders & spend          #
# Date :28/2/2022                                 #
# Author :Andreas Effraimis                       #
###################################################

##### libraries ####

# Data manipulation
library(data.table);library(lubridate);library(parallel)
# Data visualization
library(ggplot2);library(scales);library(DT);library(gridExtra)
# ML & Statistical modeling 
library(e1071);library(caret);library(klaR);library(MASS)
# User defined functions
source('auxiliary_functions.R')


##### load data with Greek characters encoding ##### 
Sys.setlocale(category = "LC_ALL",locale = "Greek")
data_init <-  fread("bq-results-20220225-100559-oj5v19xv19ix.csv",encoding = 'UTF-8')

##### data exploration #####

# duplicates check - are orders unique to customers? 
if (identical(nrow(data_init),
              uniqueN(data_init$order_id))) cat('Orders are unique \nin this set') 

# number of customers & orders 
total_orders <- data_init[,.(sum_orders=.N,
                             sum_value=sum(total_order_value)),
                          by=customer_id]

# binning of total number of orders  
total_orders[,orders_bin:=cut(total_orders$sum_orders,
                              breaks=c(quantile(total_orders$sum_orders,
                                                probs = seq(0, 1, by = 0.20))),
                              include.lowest=TRUE)]


# visual of number of customers by total orders bin
p1 <- ggplot(total_orders, aes(x=orders_bin)) +
  geom_bar(stat="count", fill="#EC2E2E")+
  geom_text(aes(label = ..count..), stat = "count", vjust=1.6, color="white", size=3.5)+
  scale_y_continuous(labels = label_number(suffix = " K",scale = 1e-3))+
  ggtitle("Number of customers  \nby order bin") +
  xlab("Orders bin") +
  ylab("Number of customers") +
  theme_light()
p1

# visual of threshold user in forecasting eligibility
p2 <- ggplot(total_orders[sum_orders < 30], aes(x=sum_orders)) +
  geom_density( fill="#EC2E2E", alpha=0.6)+
  geom_vline(xintercept=quantile(total_orders$sum_orders,0.6),size=1,color="black")+
  ggtitle("Number of customers  \ndensity per order") +
  xlab("Number of orders ") +
  ylab("Proportion of customers") +
  annotate("text", x = 15, y = 0.14, label = "Stochastic forecasting")+
  annotate("text", x = 5, y = 0.14, label = "Naive forecasting")+
  geom_segment(aes(x = 20, y = 0.1, xend = 25, yend = 0.1),
               arrow = arrow(length = unit(0.5, "cm")))+
  theme_light()
p2

# stochastic forecasting threshold
stochastic_forecast_thres <- as.numeric(quantile(total_orders$sum_orders,0.6))

# panel forecasts by customer and vertical 
vertical_cuisine_performance <- data_init[,.(unique_customers=uniqueN(customer_id),
                                             turnover=sum(total_order_value)),
                                          by=.(vertical_type,primary_cuisine)]
# turnover per customer kpi 
vertical_cuisine_performance[,turnover_per_customer:=turnover/unique_customers][order(-turnover)]

# table of kpis to justify paneling 
datatable(vertical_cuisine_performance,
          rownames = F,
          options = list(scrollY = FALSE))%>%
  formatCurrency(c(4,5) ,"€")
cat('Consider paneling on \nvertical')

##### feature engineering #####

# create chronological features based on date
data_init[ , `:=` (created_at = as.Date(created_at,format = '%Y-%m-%d'),
                   day_of_week = wday(created_at,label = T),
                   week_in_month = ceiling(day(created_at) /7))]

# keep only relevant features for our modelling process 
data_reduced <- data_init[,.(created_at,
                             vertical_type,
                             customer_id,
                             day_of_week,
                             week_in_month,
                             total_order_value,
                             num_items_ordered)]

# create a list of data.tables for each customer/vertical
customer_blocks_list <- split(data_reduced,
                              by=c("customer_id","vertical_type"),
                              drop = T)

# decide on forecast type (naive or stochastic)
type_list <- lapply(customer_blocks_list,
                    function(x) type_of_forecast(x = nrow(x),
                                                 threshold = stochastic_forecast_thres))

# create full time sequence per block (customer/vertical) and sum orders by date -fill zeros
maximum_date <- max(data_init$created_at)
minimum_date <- min(data_init$created_at)
full_date_range <- seq(from = minimum_date,
                       to = maximum_date,
                       by = "days")

# parallel execution of date filling and aggregations per block
cl <- makeCluster(detectCores() - 1);seed <- 42
clusterSetRNGStream(cl, seed)
clusterExport(cl, c('full_date_range', "fill_missing_dates"))
clusterEvalQ(cl, {
  library(data.table)
  library(lubridate)
})
model_blocks_list <- parLapply(cl=cl, customer_blocks_list,
                               function(x) fill_missing_dates(x = x,full_date_range))
stopCluster(cl)  

# verify all blocks have this proper length of observations
length(which(lapply(model_blocks_list, nrow) != length(full_date_range)))

# save up space
rm(customer_blocks_list)

##### model selection & forecasting #####

# create the forecast set for rest of March 2019
minimum_pred_date <- max(data_init$created_at) +1
maximum_pred_date <- minimum_pred_date +16
full_prediction_range <- seq(from = minimum_pred_date,
                             to = maximum_pred_date,
                             by = "days")
newdata <- data.table(created_at = full_prediction_range) 
newdata[ , `:=` (day_of_week = as.numeric(wday(created_at,label = T)),
                 week_in_month = ceiling(day(created_at) /7))]

# forecasting per block (customer/vertical)
cl <- makeCluster(detectCores() - 1);seed <- 42
clusterSetRNGStream(cl, seed)
clusterExport(cl, c('model_blocks_list','type_list','newdata'))
clusterEvalQ(cl, {
  library(data.table)
  library(e1071)
  library(caret)
  library(klaR)
  library(MASS)
})
clusterEvalQ(cl, sink(paste0(getwd(),"/logs/",Sys.getpid(), ".txt")))
customer_forecast <- parLapply(cl=cl, names(model_blocks_list),forecast_customer_orders_spend)
stopCluster(cl)  
customer_forecast_dt <- rbindlist(customer_forecast)

# aggregate at a customer level - sum up verticals
customer_forecast_dt_agg <- customer_forecast_dt[,.(number_of_orders = sum(number_of_orders),
                                                    total_order_value = sum(total_order_value)),
                                                 by = .(user_id)] 

# sample forecasting diagnostics 
build_prediction_plots(names(model_blocks_list)[sample(1:length(model_blocks_list),1)])

# export forecasts 
fwrite(customer_forecast_dt_agg,"user_preds.csv")

