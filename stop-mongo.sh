MONGOD=$MONGODB_HOME/bin/mongod
DBPATH=./data

if [ ! -d "$DBPATH" ]; then
  mkdir "$DBPATH"
fi

$MONGOD --shutdown --dbpath "$DBPATH"

