#!/bin/bash

cd /home/mararpi5/Documents/prog/amst_re/
source env/bin/activate

python main.py prod 0 none
if [ $? -ne 0 ]; then
    # only run with ssl if vanilla parsing fails
    python main.py prod 0 ssl
fi

deactivate
