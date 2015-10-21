#rozdzielenie duzego inserta na N pojedynczych insertow
sed 's/,(/;\ninsert into categorylinks values(/g' cl.sql > cli.sql

# usuniecie \'
sed "s/\\\'/''/g" cli.sql > cli2.sql

# dodanie sekwencji
sed 's/values(/values(SEQ_CATEGORYLINKS.nextval,/g' cli2.sql > cli3.sql
sed 's/VALUES (/values(SEQ_CATEGORYLINKS.nextval,/g' cli3.sql > cli4.sql
