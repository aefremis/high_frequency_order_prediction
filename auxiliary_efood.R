### decide between naive or stochastic forecasting ###

type_of_forecast <- function(x,threshold){
  type <- ifelse(x > threshold, 'stochastic','naive')
  return(type)
}


### fill missing dates with zero and aggregate at day level###
fill_missing_dates <- function(x,full_date_range){
  x[,c('vertical_type','customer_id'):=NULL]
  setkey(x, created_at)
  x <- x[J(full_date_range), roll=0]
  x[ , `:=` (day_of_week = wday(created_at,label = T),
             week_in_month = ceiling(day(created_at) /7))]
  x[, c('total_order_value','num_items_ordered')][is.na(x[, c('total_order_value','num_items_ordered')])] <- 0
  x <- x[,.(day_of_week = unique(day_of_week),
            week_in_month = unique(week_in_month),
            total_order_value = sum(total_order_value),
            num_orders = .N),
         by = .(created_at)]
  x[total_order_value == 0,num_orders:=0]
  return(x)
}


### run models over customers ###
forecast_customer_orders_spend <- function(listname){
  training_set <- model_blocks_list[[listname]]
  method_of_forecast <- type_list[[listname]]
  training_set[,day_of_week:=as.numeric(day_of_week)]
  cat(listname,"\n",grep(listname,names(model_blocks_list))," out of ",length(model_blocks_list))
  if (method_of_forecast == 'stochastic'){
    # item model
    nb_class  <- tryCatch(nb_class  <- train(x = training_set[,c("day_of_week","week_in_month")],
                                y = as.factor(training_set$num_orders),
                                'nb',
                                trControl=trainControl(method='cv',number=10),
                                metric = "MAPE"), 
                          error = function(e){
                            nb_class  <- naiveBayes(num_orders ~ day_of_week + week_in_month ,data = training_set)
                          })
    
    
    predictions <- tryCatch(predict(nb_class, newdata),
                            error = function(e){
                              as.factor(rep(0,17))
                            })
    newdata[,num_orders:= as.numeric(as.character(predictions))]
    
    value_order <- mean(training_set$total_order_value/training_set$num_orders,na.rm=T)
    newdata[,total_order_value := num_orders * value_order]
    
    customer_dt <- data.table(user_id = strsplit(listname,".",fixed = T)[[1]][1],
                              vertical = strsplit(listname,".",fixed = T)[[1]][2],
                              number_of_orders = sum(newdata$num_orders),
                              total_order_value = sum(newdata$total_order_value))
    
    
  }else{
    naive_orders <- ceiling(mean(training_set$num_orders))
    naive_value <- sum(training_set$total_order_value)/sum(training_set$num_orders)
    customer_dt <- data.table(user_id = strsplit(listname,".",fixed = T)[[1]][1],
                              vertical = strsplit(listname,".",fixed = T)[[1]][2],
                              number_of_orders = naive_orders,
                              total_order_value = naive_value)
  }
  return(customer_dt)
}


### generate individual level diagnostics ###
build_prediction_plots <- function(listname){
  
  training_set <- model_blocks_list[[listname]]
  method_of_forecast <- type_list[[listname]]
  training_set[,day_of_week:=as.numeric(day_of_week)]
  
  if (method_of_forecast == 'stochastic'){
    nb_class  <- tryCatch(train(x = training_set[,c("day_of_week","week_in_month")],
                                y = as.factor(training_set$num_orders),
                                'nb',
                                trControl=trainControl(method='cv',number=10),
                                metric = "MAPE"), 
                          error = function(e){
                            naiveBayes(num_orders ~ day_of_week + week_in_month ,data = training_set)
                          })
    
    predictions <- predict(nb_class, newdata)
    newdata[,num_orders:= as.numeric(as.character(predictions))]
    value_order <- mean(training_set$total_order_value/training_set$num_orders,na.rm=T)
    newdata[,total_order_value := num_orders * value_order]
    
    fullset <- rbind(training_set[,.(created_at,
                                 day_of_week,
                                 week_in_month,
                                 num_orders,
                                 total_order_value)],newdata)
    fullset[,num_items_ordered:=as.numeric(as.character(num_orders))]
    
    p3 <- ggplot(training_set, aes(x=created_at, y=num_orders)) +
      geom_line( color="#EC2E2E") + 
      geom_point() +
      ggtitle("Time series  \nnumber of orders") +
      xlab("Date ") +
      ylab("Number of orders")+
      theme_light()
    p4 <-  ggplot(training_set, aes(x=num_orders)) +
      geom_bar(stat="count", fill="#EC2E2E")+
      geom_text(aes(label = ..count..), stat = "count", vjust=1.6, color="white", size=3.5)+
      ggtitle("Number of orders  \nby day") +
      xlab("Number of orders") +
      ylab("Number of days") +
      theme_light()
    p5 <-  ggplot(newdata, aes(x=num_orders)) +
      geom_bar(stat="count", fill="#EC2E2E")+
      geom_text(aes(label = ..count..), stat = "count", vjust=1.6, color="white", size=3.5)+
      ggtitle("Number of orders  \nby day in forecast period") +
      xlab("Number of orders") +
      ylab("Number of days") +
      theme_light()
    p6 <- ggplot(fullset, aes(x=created_at, y=num_orders)) +
      geom_line( color="#EC2E2E") + 
      geom_point() +
      annotate("rect", xmin = minimum_pred_date, xmax = maximum_pred_date+1,
               ymin = 0, ymax = max(fullset$num_items_ordered),
               alpha = .1,fill = "blue")+
      annotate("text", x = minimum_pred_date+8, y =  max(fullset$num_items_ordered)+1,
               label = "Forecast period")+
      ggtitle("Forecasting for  \nnumber of orders") +
      xlab("Date ") +
      ylab("Number of orders")+
      theme_light()
    
    grid.arrange(p3,p4,p5,p6)
  } else {
    NULL
  }
  
}