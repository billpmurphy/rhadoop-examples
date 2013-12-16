#initialize RHadoop, benchmarking tools
library("rmr2")
Sys.setenv("HADOOP_CMD"="/Users/williammurphy/hadoop-1.2.1/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="/Users/williammurphy/hadoop-1.2.1/contrib/streaming/hadoop-streaming-1.2.1.jar")
library("microbenchmark")

###########################################################################################
## Writing data to the HDFS
###########################################################################################

write_test <- function(N){
    #Move vector of size N to HDFS
    to.dfs(1:N)
}

###########################################################################################
## Retrieving data from the HDFS
###########################################################################################

read_test <- function(data_source){
    #Retrieve and return data
    from.dfs(data_source)
}

###########################################################################################
## Summing the numbers 1 through N
###########################################################################################

sum_test <- function(N){
    #Move data to HDFS
    hdfs.data <- to.dfs(1:N)
    
    #Define and run addition function 
    result <- mapreduce(input = hdfs.data,
        map = function(k, v) keyval(1, v),
        reduce = function(k, v) keyval(k, sum(v)),
        combine = T)
    
    #Retrieve and return the sum
    from.dfs(result)
}

###########################################################################################
## Computing the variance of N numbers
###########################################################################################

var_test <- function(N){
    #Move data to HDFS
    hdfs.data <- to.dfs(1:N)
    
    #Run mapreduce job to calculate count, sum, and sum of squares
    sum_squares <- mapreduce(input = hdfs.data,
        map = function(k, v)
            keyval(1, cbind(1, v, v^2)),
        reduce = function(k, v)
            keyval(k, t(colSums(v))),
        combine = T)
        
    #Retrieve result and compute variance
    sums <- from.dfs(sum_squares)$val
    (sums[3] - (sums[2]^2)/sums[1])/(sums[1]-1)
}

############################################################################################
## Bag of words (word count)
############################################################################################

bag_of_words_test <- function(){
    #Move data to HDFS
    hdfs.data <- to.dfs(readLines('tests/big.txt'))
    
    #Define and run word-counting function 
    job <- mapreduce(input = hdfs.data,
        map = function(., lines) 
            keyval(unlist(strsplit(x = lines, split = " ")), 1),
        reduce = function(k, v) keyval(k, sum(v)),
        combine = T)
        
    #Retrive the result, convert it to a data frame, and show the most frequent words
    results <- from.dfs(job)
    results.df <- as.data.frame(results, stringsAsFactors=F)
    colnames(results.df) <- c('word', 'count')
    results.df
}

############################################################################################
## Linear least squares 
############################################################################################

linear_leastsq_test <- function(N, m){
    #Create a large matrix and write to the HDFS
    X <- matrix(rnorm(N), ncol = m)
    hdfs.data <- to.dfs(cbind(1:nrow(X), X))
    
    #Create the y vector
    y <- as.matrix(rnorm(N/m))

    #Helper function to compute a matrix sum
    Sum <- function(., YY) 
        keyval(1, list(Reduce('+', YY)))
    
    #Compute transpose(X) * X using one MapReduce job
    XtX < - values(from.dfs(
        mapreduce(input = hdfs.data,
            map = function(., Xi) {
                yi = y[Xi[,1],]
                Xi = Xi[,-1]
                keyval(1, list(t(Xi) %*% Xi))},
            reduce = Sum,
            combine = T)))[[1]]

    #Compute transpose(X) * X using another MapReduce job
    Xty <- values(from.dfs(
        mapreduce(input = hdfs.data,
            map = function(., Xi) {
                yi = y[Xi[,1],]
                Xi = Xi[,-1]
                keyval(1, list(t(Xi) %*% yi))},
            reduce = Sum,
            combine = T)))[[1]]

    #Solve the system
    solve(XtX, Xty)
}

############################################################################################
## Logistic Regression
############################################################################################

logistic_regression_test <- function(N, dimensions, iterations, alpha){
    #Create a large matrix and write to the HDFS
    input <- matrix(rnorm(N), ncol = dimensions)
    hdfs.data <- to.dfs(cbind(1:nrow(input), input))
    
    #Initialize decision bourdary, logistic function 
    plane <- t(rep(0, dimensions))
    g <- function(z) 1/(1 + exp(-z))
    
    #Map function for each iteration: 
    #Compute contribution of subset of points to the gradient
    lr.map <- function(., M) {
        Y <- M[,1] 
        X <- M[,-1]
        keyval(1, Y * X * g(-Y * as.numeric(X %*% t(plane))))
    }

    #Reduce function for each step: Sum these up
    lr.reduce <- function(k, Z) 
        keyval(k, t(as.matrix(apply(Z, 2, sum))))
    
    #Perform gradient descent, and return after a fixed # of iterations
    for (i in 1:iterations) {
        gradient <- values(from.dfs(
            mapreduce(input = hdfs.data,
                map = lr.map,     
                reduce = lr.reduce,
                combine = T)))
        plane = plane + alpha * gradient
    }
    plane 
}

############################################################################################
## K-Means
############################################################################################

kmeans_test <- function(data_size, num_clusters, num_iterations) {
    #Create data and move data to HDFS
    input <- do.call(rbind,rep(list(matrix(rnorm(data_size, sd=10), ncol=2)), 20))
        + matrix(rnorm(200), ncol=2)
    hdfs.data <- to.dfs(input) 
    
    #Helper function: Euclidian distance
    dist.fun <- function(C, P) {
        apply(C, 1,
          function(x)
            colSums((t(P) - x)^2))
    }

    #Map: Compute distances of points to centroids
    kmeans.map <- function(., P) {
        nearest = {
            if(is.null(C))
                sample(1:num_clusters, nrow(P), replace = T)
            else {
                distance = dist.fun(C, P)
                nearest = max.col(-distance)
            }
        }
        keyval(nearest, P)
    }

    #Reduce: Compute new centroids
    kmeans.reduce <- function(k, P)
        t(as.matrix(apply(P, 2, mean)))
    
    C = NULL
    for(i in 1:num_iterations ) {
      C = values(from.dfs(
            mapreduce(hdfs.data,
              map = kmeans.map,
              reduce = kmeans.reduce)))
    }
    C
}


