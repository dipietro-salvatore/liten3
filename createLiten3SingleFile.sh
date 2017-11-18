#!/usr/bin/env bash

DIR="dev"
OUT="liten3.py"
MAINCUT=40

egrep -v '^from [A-Z]' ${DIR}/main.py | head -n $MAINCUT > $OUT
egrep -v "^import " ${DIR}/DB.py >> $OUT
egrep -v "^import " ${DIR}/Walk.py | egrep -v "^from " >> $OUT
egrep -v "^import " ${DIR}/Report.py >> $OUT
egrep -v "^import " ${DIR}/Interactive.py >> $OUT
tail -n +$MAINCUT ${DIR}/main.py >> $OUT