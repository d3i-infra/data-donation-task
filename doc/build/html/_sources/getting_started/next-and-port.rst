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

The data donation task is primarily created to be used in conjunction with Next_. Next is a software as a service platform developed by Eyra_ to facilitate scientific research.

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

The data donation task is the software that extracts the data a researcher wants from a DDP it should be on Next.

The data donation task is a fork of Feldspar_, Feldspar_ is a framework developed by Eyra which can be used to build applications specifically for Next.
An example of such an application is the data donation task which you can find in this repository. 

The data donation task aims to be up to date with the master branch of Feldspar_. 

The differences between the data donation task are:

- The data donation task has standard scripts for various platforms that can be used as a basis for your own study
- The data donation task has a means to visualize data for a participant before donation


The Data Donation Task with Next
================================

The data donation task (and Feldspar_) is only a *front end* to be used with Next. In order for it to be used in a live study it needs to be hosted with Next_.
The wiki will discuss the options you have for using the data donation task in an actual study.


.. _Next: https://next.eyra.co/
.. _Eyra: https://eyra.co/
.. _Feldspar: https://github.com/eyra/feldspar
