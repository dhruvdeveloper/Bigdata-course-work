E = LOAD 'large-twitter.csv' USING PigStorage(',') AS (a, b);

E = FOREACH E GENERATE b, a;

AA = GROUP E BY b;

BB = FOREACH AA GENERATE group AS b, SIZE(E.a) as a;

CC = FOREACH BB GENERATE a, 1 AS b;

DD = GROUP CC BY a;

EE = FOREACH DD GENERATE group AS a, SIZE(CC.b) as b;

STORE EE INTO 'output' USING PigStorage (',');
