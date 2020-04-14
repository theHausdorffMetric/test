===========
PDF spiders
===========


Some port authorities communicate data in the form of PDF files.


Abstract
========

We have two tools to parse PDF files. To extract tables inside PDF documents,
we use tabula (jar file). To parse other documents, we use pdftotext from
poppler-utils linux package.

We deploy the custom docker image kp-scrapers (stored on our ECS container
repo) in the production-pdf project on Scrapinghub in order to have these
dependencies available.

We also make use of the subprocess shebang in the PDF Spiders. The current
methodology is using POpen to open a subprocess and the communicate method to
close it. We also raise timeout exceptions if the call to tabula or pdftotext
takes to much time to finish.

Scrapinghub pdf environnement is `production-pdf`.

If you wish to create a new pdf environnement, it should have `pdf` in its name
in order to use the `.tools/cli/deploy.sh` script.


Requirements
============

  * Install the AWS client installed if not already done through `dev-requirements.txt` ::

      $ pip install awscli==1.14.11

  * Then you will need to configure the aws client. Pprepare your secret and
    access keys, and keep the default region to `eu-west-1`::

      aws configure

    The settings that you need should be in ::

      ~/.ssh/aws.ssh

  * To install and setup docker, you can follow the official documentation:
    https://docs.docker.com/engine/installation/

  * To install the AWS CLI and Docker and for more information on the steps
    below, visit the ECR documentation page.


How to deploy pdf spiders
=========================

You should use `./tools/cli/deploy.sh` script to deploy pdf spiders, from the root of the repository.
This script performs the following steps:

* Decrypt needed files (kp-requirements.txt.crypt)
* Build docker image
* Save docker image to aws ecr
* Deploy image to scrapinghub
* Clean files

PDF scrapers are deployed using a docker image, quite differently than the
others that don't require custom build. `deploy.sh` will decide how to deploy
the code depending on th env name you target. If `pdf` is found in the name, it
will follow the steps below.


.. code:: bash

    ./tools/cli/deploy.sh production-pdf
    # deploy on `playground-pdf` for testing


A few inconvenience you might encounter:

* The exception ``ImportError: No module named kp_scrapers`` means your
  environement variable `PYTHONPATH` is not correctly set. This variable is the
  default search path for python module files. As your project root directory
  is not included in it, the script can't be run. Here is how to fix it:

.. code:: bash

    export PYTHONPATH=$PWD:$PYTHONPATH

Note that running the script as `sudo` user will also change your runtime
environment and will prevent the script to access this variable. This is a
common use case when using docker as root for example. The best practice is to
stop_ doing so (if you really insist, you can still run `sudo` with the `-e`
flag).


* If you get an exception when the script tries to load docker and returns a
  warning like: ``WARNING: Error loading config file:
  /home/user/.docker/config.json -stat /home/user/.docker/config.json:
  permission denied`` You need to change ownership and permissions of docker
  directory ``.docker``:

.. code:: bash

    sudo chown "$USER":"$USER" /home/"$USER"/.docker -R
    sudo chmod g+rwx "/home/$USER/.docker" -R


You can also manually deploy:

* Retrieve the docker login command that you can use to authenticate your
  Docker client to your registry:

.. code:: bash

    aws ecr get-login --region eu-west-1

* Run the docker login command that was returned in the previous step. Remove
  the the ``-e none`` option from the command if you have the folowwing error
  ``unknown shorthand flag: 'e' in -e``

* Build your Docker image using the following command. You need to specify a
  ``<REPOSITORY_NAME>``. Pdf-spider docker images are stored in kp-scrapers-pdf
  on EC2 Container Registry (ECR).


.. code:: bash

    docker build -t <REPOSITORY_NAME> .

* After the build completes, tag your image before pushing it to the
  repository. Specify a ``<VERSION>`` that can be the name of your banche and a
  version number, so you can keep an history of docker images.


.. code:: bash

    docker tag <REPOSITORY_NAME>:latest 447157256452.dkr.ecr.eu-west-1.amazonaws.com/<REPOSITORY_NAME>:<VERSION>

* Run the following command to push this image to the repository:


.. code:: bash

    docker push 447157256452.dkr.ecr.eu-west-1.amazonaws.com/<REPOSITORY_NAME>:<VERSION>


* To deploy the pdf spiders to scraping hub, use the folowing command.
  ``<SH_PROJECT_NAME>`` should be ``production-pdf``, and ``<VERSION>`` the one
  you set with docker tag.


.. code:: bash

    cd kp_scrapers
    AWS_CREDS="$(aws ecr get-login --region eu-west-1)"
    AWS_USERNAME="$(echo $AWS_CREDS |cut -d' ' -f 4)"
    AWS_PASSWORD="$(echo $AWS_CREDS |cut -d' ' -f 6)"
    shub image deploy --username $AWS_USERNAME --password $AWS_PASSWORD --version ``<VERSION>`` ``<SH_PROJECT_NAME>``


.. _stop: https://docs.docker.com/install/linux/linux-postinstall/
