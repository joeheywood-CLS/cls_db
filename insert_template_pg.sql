DROP TABLE %1$s;
CREATE TABLE %1$s (
	%2$s
);

COPY %1$s FROM '%3$s' WITH DELIMITER '|';

DELETE FROM jsonmeta WHERE clstable = '%1$s';

COPY jsonmeta FROM '%4$s' WITH DELIMITER '|'; 
