=============
Next and Port
=============


This diagram indicates the relations between Next, Port and the Data Donation task

.. figure:: /_static/arch.png
   :alt: Alternative text
   :align: center
   
   The relationship between the Data Donation Task and Next


Next
====

The data donation task is primarily created to be used in conjunction with [Next](https://github.com/eyra/mono). Next is a software as a service platform developed by [Eyra](https://eyra.co/) to facilitate scientific research.

Port
====

Port is a service on Next which you can use to perform a complete data donation study. You can use Port to:

- Personalize your study
- Setup data storage for your study
- Setup the study itself
- Integrate with qualtrics
- Administer the data donation task to participants
- Track the progress of your study

The Data Donation Task
======================

The data donation task is a fork of [Feldspar](https://github.com/eyra/feldspar) with some extra functionalities added to it. Feldspar is a framework which can be used to build applications specifically for Next. An example of such an application is the data donation task which you can find in this repository. 


Frontend
========

The data donation task (and Feldspar) is only a *front end* to be used with Next. In order for it to be used in a live study it needs to be hosted with Next.
The wiki will discuss the options you have for using the data donation task in an actual study.
