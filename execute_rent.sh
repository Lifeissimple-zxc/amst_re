#!/bin/bash

cd /home/mararkarp/projects/amst_re
source env/bin/activate

python main.py prod 0 none
python main.py prod 0 ssl

deactivate
