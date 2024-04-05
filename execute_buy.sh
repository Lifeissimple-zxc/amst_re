#!/bin/bash

cd /home/mararkarp/projects/amst_re
source env/bin/activate

python main.py prod 0 none buy
python main.py prod 0 ssl buy

deactivate
