DROP TABLE IF EXISTS query1 CASCADE;
CREATE TABLE query1 AS
SELECT genres.name AS name, COUNT(hasagenre.genreid) AS moviecount
FROM genres NATURAL JOIN hasagenre
GROUP BY name
;


DROP TABLE IF EXISTS query2 CASCADE;
CREATE TABLE query2 AS
SELECT genres.name AS name , AVG(rating) AS rating
FROM genres 
NATURAL JOIN hasagenre
NATURAL JOIN ratings
GROUP BY name
ORDER BY name
;




DROP TABLE IF EXISTS query3 CASCADE;
CREATE TABLE query3 AS
SELECT movies.title , COUNT(rating) AS CountOfRatings
FROM movies
NATURAL JOIN ratings
GROUP BY title
HAVING COUNT(rating)>=10
ORDER BY title ASC
;




DROP TABLE IF EXISTS query4 CASCADE;
CREATE TABLE query4 AS
SELECT movieid AS movieid , title AS title
FROM movies
NATURAL JOIN genres
NATURAL JOIN hasagenre
WHERE genres.name='Comedy'
ORDER BY movieid ASC
;



DROP TABLE IF EXISTS query5 CASCADE;
CREATE TABLE query5 AS
SELECT title AS title , AVG(rating) AS average
FROM movies
NATURAL JOIN ratings
GROUP BY title
ORDER BY title ASC
;


DROP TABLE IF EXISTS query6 CASCADE;
CREATE TABLE query6 AS
SELECT AVG(rating) AS average
FROM genres
NATURAL JOIN hasagenre
NATURAL JOIN ratings
WHERE genres.name='Comedy'
;



DROP TABLE IF EXISTS comedy_romance_movieids;
SELECT hasagenre.movieid
INTO TEMP comedy_romance_movieids
FROM hasagenre
NATURAL JOIN genres
WHERE genres.name='Romance'
INTERSECT						
SELECT hasagenre.movieid
FROM hasagenre
NATURAL JOIN genres
WHERE genres.name='Comedy';

DROP TABLE IF EXISTS query7 CASCADE;
CREATE TABLE query7 AS
SELECT AVG(rating) AS average
FROM ratings
INNER JOIN comedy_romance_movieids
ON comedy_romance_movieids.movieid=ratings.movieid
;




DROP TABLE IF EXISTS comedy_romance_movieids;
SELECT hasagenre.movieid
INTO TEMP comedy_romance_movieids
FROM hasagenre
NATURAL JOIN genres
WHERE name='Romance'
EXCEPT						
SELECT hasagenre.movieid
FROM hasagenre
NATURAL JOIN genres
WHERE name='Comedy';

DROP TABLE IF EXISTS query8 CASCADE;
CREATE TABLE query8 AS
SELECT AVG(rating) AS average
FROM ratings
INNER JOIN comedy_romance_movieids
ON comedy_romance_movieids.movieid=ratings.movieid
;





DROP TABLE IF EXISTS query9 CASCADE;
CREATE TABLE query9 AS
SELECT ratings.movieid AS movieid, ratings.rating AS rating
FROM ratings
WHERE ratings.userid= :v1
ORDER BY ratings.movieid
;
