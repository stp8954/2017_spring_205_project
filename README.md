# 2017_spring_205_project


Steps:

I could not figure out how to share the VM image with public on Azure. To get around, I have instantiated a VM from the image. None of the following steps are run on it.


1. Get key file to connect to VM
wget https://raw.githubusercontent.com/stp8954/2017_spring_205_project/master/pem.key

2. Open 4 SSH connections to the VM using 

ssh -i pem.key w205@13.88.20.241

3. Clone git repository
git clone https://github.com/stp8954/2017_spring_205_project.git

4. CD to the project dir in all 4 terminals
cd 2017_spring_205_project/

5. Make the shell script file executable

chmod 755 ./runsparkaggregate.sh
chmod 755 ./runsparkstreaming.sh
chmod 755 ./runproducer.sh

6. Run the following scripts in each terminal

./runproducer.sh
./runsparkstreaming.sh
./runsparkaggregate.sh

7. In the 4th terminal , run following commands

cd ~/2017_spring_205_project/portal
virtualenv api
source api/bin/activate
sudo pip install -r requirements.txt
sudo python app.py

8. Open browser on any machine and go to 

For real time data
http://13.88.20.241

For heat maps . The streaming data needs to run for at least 1 hour to have any heat map data
If the streaming scripts are started at 5/02/2017  1:25PM PST , then at 2:30PM  PST run open the following link
http://13.88.20.241/heatmap?datetime=2017-05-02%2021:00:00

Logic for the URL datetime={YYYY-MM-DD HH:00:00} -> Time is UTC 
