REGISTER json-simple-1.1.jar;
REGISTER elephant-bird-core.jar;
REGISTER elephant-bird-pig.jar;
REGISTER elephant-bird-hadoop-compat.jar;
raw_business_data = LOAD 'hdfs://172.31.27.181:9000/data/yelp/business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json: map[]);

business_data1 = FOREACH raw_business_data GENERATE 
  json#'business_id' as business_id:chararray,
  json#'name' as name:chararray,
  json#'city' as city:chararray,
  json#'state' as state:chararray,
  json#'stars' as stars:float,
  json#'review_count' as review_count:int,
  json#'categories' as categories:{t:(cat:chararray)},
  json#'attributes' as (attributes: map[]);

business_data2 = FOREACH business_data1 GENERATE business_id, name, city, state, stars, review_count, categories, attributes#'Ambience' as ambience:[];
business_data3 = FOREACH business_data2 GENERATE business_id, name, city, state, stars, review_count, categories, 
              ambience#'hipster' as hipster:chararray, ambience#'divey' as divey:chararray, ambience#'trendy' as trendy:chararray;
cool1 = FILTER business_data3 BY hipster is not null AND divey is not null and trendy is not null;
cool2 = FILTER cool1 BY (hipster == 'true') or (divey == 'true') or (trendy == 'true');
group_data = GROUP cool2 BY (city, state);
cool_count = FOREACH group_data GENERATE group, COUNT(cool2)
