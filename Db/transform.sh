#rozdzielenie duzego inserta na N pojedynczych insertow
sed 's/,(/;\ninsert into categorylinks values(/g' cl.sql > cli.sql

# usuniecie \'
sed "s/\\\'/''/g" cli.sql > cli2.sql