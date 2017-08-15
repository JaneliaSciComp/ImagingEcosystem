#!/bin/bash

/bin/rm /tmp/monthly_lsm_cleanup.log
date > /tmp/monthly_lsm_cleanup.log
echo "Cleaning up Polarity LSMs" >> /tmp/monthly_lsm_cleanup.log
#./image_reconcile_directory.pl -path /groups/flylight/flylight/polarity/confocalStacks >>/tmp/monthly_lsm_cleanup.log
echo "Cleaning up MCFO LSMs" >> /tmp/monthly_lsm_cleanup.log
#./image_reconcile_directory.pl -path /groups/flylight/flylight/flip/confocalStacks >>/tmp/monthly_lsm_cleanup.log
echo "# files staged for deletion" >> /tmp/monthly_lsm_cleanup.log
find /groups/flylight/flylight/lsm_archive -name *lsm | wc -l >> /tmp/monthly_lsm_cleanup.log
echo "Disk space staged for deletion" >> /tmp/monthly_lsm_cleanup.log
du -sh /groups/flylight/flylight/lsm_archive >> /tmp/monthly_lsm_cleanup.log
echo "Deleting files" >> /tmp/monthly_lsm_cleanup.log
/bin/rm -rf /groups/flylight/flylight/lsm_archive/2*
date >> /tmp/monthly_lsm_cleanup.log
echo "Done" >> /tmp/monthly_lsm_cleanup.log
echo "The monthly LSM cleanup log for Fly Light is attached." | mail -s "Monthly LSM cleanup" -a /tmp/monthly_lsm_cleanup.log robsvi@gmail.com,svirskasr@hhmi.org