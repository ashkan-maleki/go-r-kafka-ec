-- IF
-- NOT EXISTS(SELECT 1
--     FROM pg_catalog.pg_database
--     WHERE lower(datname) = lower('posts'))
--     THEN

CREATE DATABASE posts OWNER postgres
--     IF NOT EXists;
-- END IF

