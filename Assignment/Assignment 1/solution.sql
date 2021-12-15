CREATE TABLE users (
                    userid INT,
                    name TEXT, 
                    PRIMARY KEY (userid)
                    );


CREATE TABLE movies (
                    movieid INT,
                    title TEXT, 
                    PRIMARY KEY (movieid)
                    );

CREATE TABLE taginfo (
                    tagid INT,
                    content TEXT, 
                    PRIMARY KEY (tagid)
                    );

CREATE TABLE genres (
                    genreid INTEGER,
                    name TEXT, 
                    PRIMARY KEY (genreid)
                    );

CREATE TABLE ratings(
                    userid INT,
                    movieid INT,
                    rating NUMERIC,
                    timestamp bigint, 
			        PRIMARY KEY (userid,movieid),
			        FOREIGN KEY (userid) REFERENCES users , 
			        FOREIGN KEY (movieid) REFERENCES movies,
			        CONSTRAINT rating_min CHECK(rating>=0.0::numeric),
                    CONSTRAINT rating_max CHECK(rating<=5.0::numeric)
                    );



CREATE TABLE hasagenre (
                       movieid INT,
                       genreid INT, 
                       PRIMARY KEY (movieid,genreid),
                       FOREIGN KEY (movieid) REFERENCES movies, 
                       FOREIGN KEY (genreid) REFERENCES genres );

CREATE TABLE tags(userid INT,movieid INT,tagid INT,timestamp bigint,
             PRIMARY KEY (userid,movieid,tagid),
			 FOREIGN KEY (userid) REFERENCES users, 
		     FOREIGN KEY (movieid) REFERENCES movies,
		     FOREIGN KEY (tagid) REFERENCES taginfo);
			 