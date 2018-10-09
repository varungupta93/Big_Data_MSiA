USE vgupta;


CREATE EXTERNAL TABLE IF NOT EXISTS grouplens(
    user_id INT,
    movie_id INT,
    rating INT,
    time TIMESTAMP)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/home/public/course/recommendationEngine/u.data' into table grouplens;

SELECT movie_id, COUNT(*) AS Frequency, AVG(rating) as Rating_score
FROM grouplens AS tbl 
WHERE tbl.movie_id NOT IN (SELECT movie_id FROM grouplens AS tbl2 WHERE tbl2.user_id = 220)
AND tbl.movie_id IS NOT NULL 
GROUP BY tbl.movie_id
ORDER BY Rating_score DESC, Frequency DESC LIMIT 10; 
