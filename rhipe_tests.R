#initialize Rhipe, benchmarking tools
library("Rhipe")
library("microbenchmark")
rhinit()


###########################################################################################
## Writing data to the HDFS
###########################################################################################

write_test <- function(N, directory){
    #Move vector of size N to HDFS
    rhwrite(1:N, directory)
}

###########################################################################################
## Retrieving data from the HDFS
###########################################################################################

read_test <- function(directory){
    #Retrieve and return data
    rhread(directory)
}

###########################################################################################
## Summing the numbers 1 through N
###########################################################################################

sum_test <- function(N){
    #Move data to HDFS
    rhwrite(1:N, directory)
    
    #Define and run addition function 
    map <- expression(lapply(map.values, 
        function(v) rhcollect(1, v)))
    reduce <- expression(pre = {total=0}, 
        reduce = {total <- sum(total, unlist(reduce.values))}, 
        post={rhcollect(1, total)}) 
    
    job <- rhmr(map=map,
        reduce = reduce,
        inout=c("text","sequence"),
        ifolder=directory, ofolder="/output_01",
        mapred=mapred, jobname="sum") 
    rhex(job) 
    
    #Retrieve and return the sum
    rhread("/output_01")
}

###########################################################################################
## Computing the variance of N numbers
###########################################################################################

var_test <- function(N, directory){
    #Move data to HDFS
    rhwrite(rnorm(N, mean=0, sd=5), directory)
    
    #Run mapreduce job to calculate count, sum, and sum of squares
    map <- expression(lapply(map.values, 
        function(v) rhcollect(1, cbind(1, v, v^2))))
    reduce <- expression(pre = {total=c(0, 0, 0)}, 
        reduce = {total <- colSums(total, t(reduce.values))}, 
        post={rhcollect(1, total)})

    job <- rhmr(map=map,
        reduce = reduce,
        inout=c("text","sequence"),
        ifolder=directory, ofolder="/output_01",
        mapred=mapred, jobname="variance") 
    rhex(job) 

    #Retrieve result and compute variance
    sums <- rhread("/output_01")
    (sums[3] - (sums[2]^2)/sums[1])/(sums[1]-1)
}

############################################################################################
## Bag of words (word count)
############################################################################################

bag_of_words_test <- function(){
    #Move data to HDFS
    rhwrite(readLines('tests/big.txt'), directory)
    
    #Define and run word-counting function 
    map <- expression({ 
        words_vector <- unlist(strsplit(unlist(map.values)," ")) 
        lapply(words_vector, function(i) rhcollect(i, 1))}) 
    
    reduce <- expression(pre = {total=0}, 
        reduce = {total <- sum(total, unlist(reduce.values))}, 
        post={rhcollect(reduce.key, total)}) 
    
    job <- rhmr(map=map,
        reduce = reduce,
        inout=c("text","sequence"),
        ifolder="test/big.txt", ofolder="/output_01",
        mapred=mapred, jobname="word_count") 
    rhex(job) 
    
    #Retrive the result, convert it to a data frame, and show the most frequent words
    results <- rhread("/output_01")
    results.df <- as.data.frame(results, stringsAsFactors=F)
    colnames(results.df) <- c('word', 'count')
    results.df
}

############################################################################################
## Linear least squares 
############################################################################################

linear_leastsq_test <- function(N, m, directory){
    #Create a large matrix and write to the HDFS
    X <- matrix(rnorm(N), ncol = m)
    rhwrite(cbind(1:nrow(X), X), directory)
    
    #Create the y vector
    y <- as.matrix(rnorm(N/m))

    #Helper function to compute a matrix sum
    Sum <- expression(pre = {total = NULL}, 
        reduce = {total <- if total == NULL list(Reduce('+', reduce.values))
            else list(Reduce('+', rbind(total, reduce.values)))}, 
        post={rhcollect(reduce.key, total)})
    
    #Compute transpose(X) * X using one MapReduce job
    xtx_map <- expression(lapply(map.values),
        function(Xi) {
            yi = y[Xi[,1],]
            Xi = Xi[,-1]
            rhcollect(1, list(t(Xi) %*% Xi))})
    xtx_job <- rhmr(map=xtx_map,
        reduce = Sum,
        inout=c("text","sequence"),
        ifolder=directory, ofolder="/xtx",
        mapred=mapred, jobname="xtx") 
    rhex(xtx_job) 

    #Compute transpose(X) * X using another MapReduce job
    xty_map <- expression(lapply(map.values),
        function(Xi) {
            yi = y[Xi[,1],]
            Xi = Xi[,-1]
            rhcollect(1, list(t(Xi) %*% yi))})
    xty_job <- rhmr(map=xty_map,
        reduce = Sum,
        inout=c("text","sequence"),
        ifolder=directory, ofolder="/xty",
        mapred=mapred, jobname="xty") 
    rhex(xty_job) 

    #Solve the system
    XtX <- rhread("/xtx")
    Xty <- rhread("/xty")
    solve(XtX, Xty)
}

############################################################################################
## Logistic Regression
############################################################################################

logistic_regression_test <- function(N, dimensions, iterations, alpha, directory){
    #Create a large matrix and write to the HDFS
    input <- matrix(rnorm(N), ncol = dimensions)
    rhwrite(cbind(1:nrow(input), input), directory)
    
    #Initialize decision bourdary, logistic function 
    plane <- t(rep(0, dimensions))
    g <- function(z) 1/(1 + exp(-z))
    
    #Map function for each iteration: 
    #Compute contribution of subset of points to the gradient
    lr.map <- expression(lapply(map.values), function(M) {
        Y <- M[,1] 
        X <- M[,-1]
        rhcollect(1, Y * X * g(-Y * as.numeric(X %*% t(plane))))
    })

    #Reduce function for each step: Sum these up
    lr.reduce <- expression(pre = {total = NULL}, 
        reduce = {total <- if total == NULL t(as.matrix(apply(reduce.values, 2, sum))))
            else t(as.matrix(apply(rbind(total, reduce.values), 2, sum)))}, 
        post={rhcollect(reduce.key, total)})
    
    #Perform gradient descent, and return after a fixed # of iterations
    for (i in 1:iterations) {
        job <- rhmr(map=lr.map,
            reduce = lr.reduce,
            inout=c("text","sequence"),
            ifolder=directory, ofolder="/output",
            mapred=mapred, jobname="logreg") 
        rhex(job)  
        gradient <- rhread("/output")
        plane = plane + alpha * gradient
    }
    plane 
}

############################################################################################
## K-Means
############################################################################################

kmeans_test <- function(data_size, num_clusters, num_iterations, directory) {
    #Create data and move data to HDFS
    input <- do.call(rbind,rep(list(matrix(rnorm(10*data_size, sd=10), ncol=2)), 20))
        + matrix(rnorm(200), ncol=2)
    rhwrite(input, directory) 
    
    #Helper function: Euclidian distance
    dist.fun <- function(C, P) {
        apply(C, 1,
          function(x)
            colSums((t(P) - x)^2))
    }

    #Map function: Compute distances of points to centroids
    kmeans.map <- expression(lapply(map.values), function(P) {
        nearest = {
            if(is.null(C))
                sample(1:num_clusters, nrow(P), replace = T)
            else {
                distance <- dist.fun(C, P)
                nearest <- max.col(-distance)
            }
        }
        rhcollect(nearest, P)})

    #Reduce: Compute new centroids
    kmeans.reduce <- expression(pre = {total = NULL}, 
        reduce = {total <- if total == NULL t(as.matrix(apply(reduce.values, 2, sum))))
            else t(as.matrix(apply(rbind(total, reduce.values), 2, sum)))}, 
        post={rhcollect(reduce.key, total)})
    
    C = NULL
    for(i in 1:num_iterations ) {
        job <- rhmr(map=kmeans.map,
            reduce = kmeans.reduce,
            inout=c("text","sequence"),
            ifolder=directory, ofolder="/output",
            mapred=mapred, jobname="kmeans") 
        rhex(job)  
      C = rhread("/output")
    }
    C
}