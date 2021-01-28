
sudo apt-get install python3-lxml

python -m pip install numpy scipy matplotlib

pip install Cython




# n.b. run command
python3 Gdelter.py -o -d /home/chagerman/Projects/NewsAggregator/Gdelter/Data -k lastupdate


# start cron service
sudo /etc/init.d/cron start

# rync project to server
# make script(s) executable
chmod +x /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/crontest.sh



########################################################################################################################
#  Cron Tab
########################################################################################################################

# add an entry to crontab -e to run the script on a schedule

# --------------
# mpat crontab
# --------------
# BAD:
#*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/run_mpat.sh   >> /var/log/cronlog.log

*/15 * * * * bash /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/run_mpat.sh   >> /var/log/cronlog.log
*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && bash /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/run_mpat.sh   >> /var/log/cronlog.log


# or...
  #*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter  && pyenv activate myenv && /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/run_mpat.sh   >> /var/log/cronlog.log



########################################################################################################################
#  Cron Experiments
########################################################################################################################

# add an entry to crontab -e to run the script on a schedule
# Working cron entry for testing purposes. First a bash script
*/2 * * * * /bin/bash -c ". ~/.bashrc; /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/crontest.sh"
# working python script
*/2 * * * * /bin/bash -c ". ~/.bashrc; /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/cronpytest.py"
# same as above but cd-ing to project dir and specifying python interpreter
*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/cronpytest.py


*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python ./scripts/cronpytest.py


*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter bash ./scripts/run_mapt.sh




# same as above but using relative path for script
*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python scripts/cronpytest.py

*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python scripts/run.sh

*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && pyenv activate myenv && scripts/run.sh






