# lazr
> A data lake architecture for telepresence robots

## Table of contents

- [Architecture](#architecture)
- [Installations](#installations)
- [How to use](#how-to-use)
- [References](#references)

## Architecture

Telepresence robotic avatars one such as from [Avatar](https://g.co/kgs/KfuJ42), where a operator can control remotely located robot through a jacket. There are two different kinds of data generated here:

- Sensory Data and:
  
  Since the nature of these sensors generate time dependent data. The information generated from jacket consists of sensors such as following:

  * Feedback
  * Galvanic Skin Receptors
  * Haptic Sensors and 
  * Motion Sensors


- Video information:

  The video and audio feedback from robot while the operating the robot.

Considerations and understanding on what basis we've choosen this architecture, you may check out [this link](https://docs.google.com/document/d/1Q2nAtQ_UcUGdLwagbxyHWk7RiwRFSflorGsx32z_9mc/edit?usp=sharing).


## Installations

Following are technical stack we've used:

- [Anaconda](https://docs.google.com/document/d/1j_7M_d7pD1tuuEVWgVmpW8lwZ1VxH1-cF8JhZJWO3Ac/edit?usp=sharing)

- [MongoDB](https://docs.google.com/document/d/1R6Zv9BGfJY_acG323mk71RC7DlkifUg9R2NWBgvk8e8/edit?usp=sharing)

- [PySpark](https://docs.google.com/document/d/1MJl3Hi3cOmC5VgVG0qfSsF11LPbk68vii-gOKOMeYxg/edit?usp=sharing)

- [Hadoop](https://docs.google.com/document/d/1lyAd5bzZWu_nRXTS0wU0KZG-USIep5SkxOaBhLVtX5s/edit?usp=sharing)

- [Kafka](https://github.com/ATR-Lab/getting-started-kafka#installation)


## How to use

```
# Automate shell script is responsible for getting up Kafka cluster up, 
# running Kafka Producer and Consumer, and
# Connecting PySpark to MongoDB

$ bash automate.sh


# Change script is responsible for publishing the data to HDFS through URI
$ bash change.sh

```
## References












