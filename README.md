# Big Data Project
###Big Data: Twitter Analysis with Hadoop MapReduce

The goal of this project is to develop several simple Map/Reduce programs to analyze one provided dataset.
The dataset contained 18 million Twitter messages captured during the London 2012 Olympics period. All messages are related in some way to the events happening in London (as they have a term such as London2012).
The format for the input of a Mapreduce job is as follows:

```
tweetId;date;hashtags;tweet
```

hashtags are space separated in the individual field, with the hash symbol already removed.


An example entry for the dataset (with no hashtags) is: 
```
228940055878701056;2012-07-27, 20:48:57, BST;;RT @Mets: R.A. Dickey (1996 in Atlanta), Jon Rauch (2000 in Sydney) and first base coach Tom Goodwin (1988 in Seoul) - all on Olympic teams.;
```
#####Components:
A set of Map/Reduce jobs developed within this project can process the given input and generate the data required to answer the following questions:

######A. TEXT ANALYSIS
You can find a Histogram plot in the pdf report that depicts the distribution of sizes (measured in number of characters) among the Twitter dataset. 

######B. TIME ANALYSIS
You can find a Plot with time series with the number of Tweets that were posted each day of the event (one bar per day).

######C. HASHTAG ANALYSIS
During the olympics the supporters from several countries were expressing their support by adding specific hashtags to its messages, in many cases with the form team__ (e.g. #teamgb). or, go___ (#gousa). 
You can find a table with all the team support hashtags you can find (based on these patterns), and the number of support messages. 

For complete set of analysis results, please check the pdf report
