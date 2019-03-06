
# How To Install and Use Docker on Ubuntu 18.04

see:
[How To Install and Use Docker on Ubuntu 18.04](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)



Update your existing list of packages:

    sudo apt update

 Install a few prerequisite packages

    sudo apt install apt-transport-https ca-certificates curl software-properties-common

Add the GPG key

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

Add the Docker repository to APT sources:

    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"

Update the package database 

    sudo apt update

Install from the Docker repo
 
    apt-cache policy docker-ce


Install Docker

    sudo apt install docker-ce

Check that it's running

    sudo systemctl status docker






# Executing the Docker Command Without Sudo 


Add your username to the docker group:

    sudo usermod -aG docker ${USER}


Log out of the server and back in, or type the following:

    su - ${USER}

Confirm that you user are added to the docker group

    id -nG







