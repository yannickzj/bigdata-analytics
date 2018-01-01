# Bigdata Analytics

## Introduction

This project aims to perform analytical computations over a data set of movie ratings. It consists of 4 different analytical tasks.

## Data set

The input data set is a matrix of movie ratings. Each row represents one movie, and each column represents one user. Movie titles are provided in the leftmost column, and the= remaining columns represent numerical ratings. Columns are delimited by commas, and you may assume that any commas in the movie titles have been stripped. Ratings are integers ranging from 1 to 5 inclusive. Blanks denote missing data.

Sample data can be found in `sample_input/smalldata.txt`.

## Task 1

For each movie, output the column numbers of users who gave the highest rating. If the highest rating is given by multiple users, output all of them in ascending numerical order. Assume that there is at least one non-blank rating for each movie.

## Task 2

Compute the total number of (non-blank) ratings. Output the total as a single number on a single line.

## Task 3

For each user, output the user's column number and the total number of ratings for that user.

## Task 4

For each pair of movies, compute the number of users who assigned the same (non-blank) rating to both movies.

## How to run the program

For the Hadoop-based solutions, you can run the following command:
```
./cluster_hadoop.sh <task id> <input file>
```

For the Spark-based solutions, you can run the following command:
```
./cluster_spark.sh <task id> <input file>
```
