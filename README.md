### COMMIT OOPS CODE - GIT COMMANDS
* git add .
* git commit -m "init"
* git push origin master
* git config --global user.name "Abhishek Gaur"
* git config --global user.email "gaurmmec@gmail.com"
* git clone git@gitlab.com:quantsderiveq/strategy/oops.git

#### Push Existing Folder
* cd existing_folder
* git init
* git remote add origin git@gitlab.com:quantsderiveq/strategy/brics.git
* git add .
* git commit -m "Initial commit"
* git push -u origin master
* clone specific branch - git clone -b <branch> <remote_repo>
* git clone https://gaurmmec@gitlab.com/finnoveshcore/strategy/brics.git

#### Remove Unwanted Files
* git rm --cached */__pycache__/*
* git commit -m "remove unwanted files'
* Add files to .gitignore if you don't want to see in commit list

#### Putty Commands
* nohup python /home/ec2-user/venv/python36/lib/python3.6/site-packages/oops1/src/controller/OrderGeneration/OrderGenerationMain.py &
* nohup docker logs -f brics1_v1_sit > /home/ubuntu/brics1_v1_sit/brics1_v1_sit.log  2>&1 & 

### Crontab Scheduler
30 2 * * 1-5 sudo reboot

28 4-9 * * 1-5 sudo reboot

####### Scheduler 
53 3 * * 1-5 cd /home/ec2-user/venv/python36/lib/python3.6/site-packages && /home/ec2-user/venv/python36/bin/python3.6 /home/ec2-user/venv/python36/lib/python3.6/site-packages/DNDScheduler.py > /home/ec2-user/cronlog.txt 2&>1

30 4-9 * * 1-5 cd /home/ec2-user/venv/python36/lib/python3.6/site-packages && /home/ec2-user/venv/python36/bin/python3.6 /home/ec2-user/venv/python36/lib/python3.6/site-packages/DNDScheduler.py > /home/ec2-user/cronlog.txt 2&>1

 BECOME ROOT USER AND THEN REBOOT:
---------------------------------
sudo sh
reboot

### CRON FILE Location
---------------------------------------
var/spool/cron/ec2-user

## RDS DB points:
* Exception : lock-wait-timeout-exceeded
* show open tables where in_use>0;
* show processlist;
* kill 1161160

## Generate Requirements.txt
            pip install pipreqs
            
            pipreqs /path/to/project
            
## Jenkins Url
             http://api.dev.deriveq.com/job/oops3/configure
             dev/DEVderiveq18$
             
## Docker commands:
        1. docker ps -a
        
        2. docker logs <container-id> -f 
           docker logs <container-id> -f -t since 30m
           
        3. Stop and Remove Stopped Containers
               docker stop <container-id>
               docker container rm cc3f2ff51cab cd20b396a061  
            
        4. docker logs <container-id> 2>&1 | grep "nifty"
        
        5. docker logs oops3_v1_sit >& /home/opc/logs/oops3_v1_sit.log  -- Important - Copy logs to file till that point
        
        6. Keep writing logs to file (Latest)
             docker logs -f oops3_v1_sit &> /home/opc/logs/oops3_v1_sit.log &  
             docker logs -f oops3_v2_sit &> /home/opc/logs/oops3_v2_sit.log & 
             docker logs -f oops3_v3_sit &> /home/opc/logs/oops3_v3_sit.log & 
             
             nohup docker logs -f oops4 > /home/ubuntu/oops4/oops4.log  2>&1 &
        
        References:
        https://hackernoon.com/to-boldly-log-debug-docker-apps-effectively-using-logs-options-tail-and-grep-53d2e655abcb
        https://stackoverflow.com/questions/41144589/how-to-redirect-docker-container-logs-to-a-single-file

## Swagger Link
            http://140.238.249.247:8090/swagger  -- oops3_SIT1
            http://140.238.249.247:8091/swagger  -- oops3_UAT1
            http://140.238.249.247:8092/swagger  -- oops3_EBF1
            
## Gitlab Configuration in Windows Machine

            --> Open Git Bash
            $ ssh-keygen -t rsa -b 4096 or ssh-keygen -t ed25519 -C "gaurmmec@gmail.com"
            $ ~/.ssh
           
            $ ~/.ssh
            $ eval `ssh-agent -s` ---  if this does not work use -->  eval "$(ssh-agent)"
            Agent pid 10076
            
            
            $ ~/.ssh
            $ ssh-add ./id_rsa
            Identity added: ./id_rsa (./id_rsa)
            
            $ ~/.ssh
            $ ssh git@gitlab.com:quantsderiveq/strategy/pairstrading.git
            
            New Way:
            ssh-keygen -t ed25519 -C "gaurmmec@gmail.com"
            ssh-add ~/.ssh/id_ed25519
            
            Clone with https:
            git clone https://gaurmmec@gitlab.com/<work>/strategy/<strategyname>.git
            It will prompt you for your password.
            
            
            References:
            1. https://medium.com/uncaught-exception/setting-up-multiple-gitlab-accounts-82b70e88c437
            2. https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent
            3. https://stackoverflow.com/questions/10054318/how-do-i-provide-a-username-and-password-when-running-git-clone-gitremote-git
                       
## My SQL
            1. To copy with indexes and triggers do these 2 queries:
            -------------------------------------------------------
            CREATE TABLE newtable LIKE oldtable; 
            INSERT INTO newtable SELECT * FROM oldtable;
            
            2. To copy just structure and data use this one:
            -------------------------------------------------------
            CREATE TABLE tbl_new AS SELECT * FROM tbl_old;
            
## Config Server Url
            https://gitlab.com/quantsderiveq/config/configserver/-/blob/dev/dev/oops3

### Troubleshooting
----------------------
1. **Prod Logs not getting wriiten on oops1/.../oops.log**  

**Solution**: 

    https://stackoverflow.com/questions/35898160/logging-module-not-writing-to-file/51843801#51843801
    Add the following lines before the logging.basicConfig() and it worked for me.
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

**2. GRANT COMMAND IN MYSQL 8.0, CREATE USER FIRST THEN GRANT**

    _CREATE USER 'dq_prd1_2020'@'%' IDENTIFIED BY 'deriveq18$';_
    
    _GRANT ALL PRIVILEGES ON oops2_PRD.* TO 'dq_prd1_2020'@'%' WITH GRANT OPTION;_

**3. sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection timed out**

    https://stackoverflow.com/questions/24956894/sql-alchemy-queuepool-limit-overflow
    
 **4. set innodb_lock_wait_timeout=116**

 * 4 a). show variables like "max_connections"; 
 * 4 b). set global max_connections = 200; -- you have to be super user to execute this query
 * 4 c). Create new parameter group , modify instance and 
   update max_connections in parameter group to 200
   
**5. EC2 space issue :**

Modify volume size. From AWS Console, you can modify the size of a volume.

    From AWS console, open 'ELASTIC BLOCK STORE/Volume'
    Select your volume and Modify volume(from Actions button)
    Change size (e.g. 8 to 20gib)
    Click Modify.
    Reboot from EC2 Dashboard.
    check size is changed by df -h
    
**6. Max Connections Issue**
   
       --> Create new parameter group , modify instance and 
       update max_connections in parameter group to 200

### Points to Remember:
----------------------
1. CALL_SELL AND PUT_BUY Delta will always be negative

        