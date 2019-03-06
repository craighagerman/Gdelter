

Open crontab editor
    
    crontab -e


Create a schedule for python-gdelt container to run every 15 minutes

    */15 * * * * /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/docker_run_titanx.sh  >> /var/log/cronlog.log



Get output of applications run by cron

    tail -f /var/mail/chagerman
