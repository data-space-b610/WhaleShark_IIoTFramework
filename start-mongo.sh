MONGOD=$MONGODB_HOME/bin/mongod
DBPATH=./data

if [ ! -d "$DBPATH" ]; then
  mkdir "$DBPATH"
fi

$MONGOD --dbpath "$DBPATH"

