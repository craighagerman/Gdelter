
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


# add an entry to crontab -e to run the script on a schedule
# Working cron entry for testing purposes. First a bash script
*/2 * * * * /bin/bash -c ". ~/.bashrc; /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/crontest.sh"
# working python script
*/2 * * * * /bin/bash -c ". ~/.bashrc; /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/cronpytest.py"
# same as above but cd-ing to project dir and specifying python interpreter
*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/cronpytest.py
# same as above but using relative path for script
*/2 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python scripts/cronpytest.py




*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python scripts/run.sh

*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && pyenv activate myenv-3.7 && scripts/run.sh



# Current Crontab
*/15 * * * * /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/run.sh


# Improved Crontab

*/15 * * * * cd /home/chagerman/Projects/NewsAggregator/Gdelter && /home/chagerman/.pyenv/shims/python /home/chagerman/Projects/NewsAggregator/Gdelter/crontest.py





