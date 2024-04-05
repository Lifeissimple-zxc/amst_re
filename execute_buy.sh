#!/bin/bash

cd /home/mararkarp/projects/amst_re
source env/bin/activate

python main_purchase.py prod 1 none buy
python main_purchase.py prod 1 ssl buy

deactivate
