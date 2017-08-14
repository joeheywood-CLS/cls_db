DROP TABLE %1$s;
CREATE TABLE %1$s (
	%2$s
);

COPY INTO %1$s FROM '%3$s';

DELETE FROM jsonmeta WHERE clstable = '%1$s';

COPY INTO jsonmeta FROM '%4$s'; 
