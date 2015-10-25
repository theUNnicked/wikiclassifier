export WIKIDUMPS_FILE=/home/wojtek/Pobrane/plwiki-20151002-pages-articles2.xml
export WIKIDUMPS_OUTPUT_FOLDER
java -jar ./target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar --dump $WIKIDUMPS_FILE wikidumps
