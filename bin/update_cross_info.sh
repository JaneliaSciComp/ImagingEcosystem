#!/bin/bash

slide_codes='processed_slide_codes.txt'
for prop in cross_description effector_description lab_member lab_project
do
   python3 update_jacs_line.py --property $prop --manifold prod --file $slide_codes --write
done
