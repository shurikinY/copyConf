
JVM_OPTS="$JVM_OPTS -Dcomponent1=1234567890123456"
JVM_OPTS="$JVM_OPTS -Dcomponent2=1234567890123456"

java -jar target\copyConf-1.0.5.15-SNAPSHOT.jar -m save -url "jdbc:postgresql://localhost:5432/solar_saltpay" -username postgres -encryptedPassword "gmLx13XItduY3F+mJQpwMA==;1QX16RVGFz3GCyddvG/vNQ==" -s object_configdb_trf_v1.0.9.json -w object_task.json -r DataFromDB.json -d "2022-04-15 00:00:00" -f filter_trf_demo.json

::java -jar target\copyConf-1.0.5.15-SNAPSHOT.jar -m save -url "jdbc:postgresql://localhost:5432/solar_saltpay" -username postgres -password "PROMT" -s object_configdb_trf_v1.0.9.json -w object_task.json -r DataFromDB.json -d "2022-04-15 00:00:00" -f filter_trf_demo.json