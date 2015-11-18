export READ_DUMP_MAX_ARTICLES=1000
export READ_DUMP_USER_LOGIN=143351sw
export READ_FOLDER=~/KAW/wikiclassifier/SourceCode/kaw
export READ_DUMP_JAR_PLACEMENT=~/KAW/wikiclassifier/SourceCode/kaw/target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar
export READ_DUMP_PLWIKI_PLACEMENT=/macierz/home/143365sk/KAW/plwiki-20151002-pages-articles2.xml
export READ_DUMP_OUTPUT_DIRECTORY=/macierz/home/143365sk/KAW/1000articles
#opdalane z des01
#java -jar $READ_DUMP_JAR_PLACEMENT dump --local $READ_DUMP_PLWIKI_PLACEMENT $READ_DUMP_OUTPUT_DIRECTORY --start 0 --skip 2 --max $READ_DUMP_MAX_ARTICLES &
ssh $READ_DUMP_USER_LOGIN@des02.eti.pg.gda.pl "cd $READ_FOLDER && java -jar $READ_DUMP_JAR_PLACEMENT dump --local $READ_DUMP_PLWIKI_PLACEMENT $READ_DUMP_OUTPUT_DIRECTORY --start 1 --skip 2 --max $READ_DUMP_MAX_ARTICLES" &
ssh $READ_DUMP_USER_LOGIN@des03.eti.pg.gda.pl "cd $READ_FOLDER && java -jar $READ_DUMP_JAR_PLACEMENT dump --local $READ_DUMP_PLWIKI_PLACEMENT $READ_DUMP_OUTPUT_DIRECTORY --start 2 --skip 2 --max $READ_DUMP_MAX_ARTICLES" &
