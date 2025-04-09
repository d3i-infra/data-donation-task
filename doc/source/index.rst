======================
The Data Donation Task
======================

The data donation task is front end that guides participants through the data donation steps, used in conjunction with Next_.

Completely new to data donation?
================================

`Start here!`_


What is the Data Donation Task?
===============================

The data donation task is part of a research tool that enables researchers to collected digital trace data for academic research from participants in a secure, transparent, and privacy-preserving way.

Data donation allows researchers to invite participants to share their data download packages (DDPs). A major challenge is however that DDPs potentially contain very sensitive data, and often not all data is needed to answer the specific research question under investigation. To circumvent these challenges, the following framework framework was developed:

1. The participant requests their personal DDP at the platform of interest.
2. They download it onto their own personal device.
3. By means of local processing (i.e. in the browser of the participant) only the features of interest to the researcher are extracted from that DDP.
4. The participant inspects the extracted features after which they can consent (or decline) to donate.

To allow for step 3 and 4 to take place the data donation task was developed. The data donation task is front end that guides participants through the data donation steps. The data donation task is primarily created to be used in conjunction with Next_. Next is a software as a service platform developed by Eyra_ to facilitate scientific research.

Below you can find an architecture diagram outlining the relationships between Next and the Data Donation Task.

.. figure:: _static/arch.png
   :alt: Alternative text
   :align: center
   
   The relationship between the Data Donation Task and Next



How does the data donation task work?
=====================================

The idea behind the data donation task
--------------------------------------

This data donation task repository contains in essence a toolkit with which you can build your own data donation flow. The donation flow is at the heart of the data donation task, and is at the core of a data donation study. 
It is the step where the participant is actually going to donate their data.

The data donation flow goes as follows:

1. The participant goes to your data donation task app in a browser
2. The participant is prompted to submit their data download package (DDP)
3. A Python script you wrote, extracts the data you need for your research
4. That data gets presented to the participant on screen. (The participant gets to interact with their data)
5. The participants decides to donate and you receive the data 

We opted for a toolkit approach because it offers several benefits:

1. Every study requires unique elements for its participants, and a toolkit can facilitate the creation of this distinct experience.
2. You can extract (and possibly aggregate) only the data you need for your study, which we believe is important in order to preserve the privacy of the participant and is often required by an ethical review board.


The design of the data donation task
------------------------------------

The data donation task has reusable components (such as: a screen that prompts the participant to submit their DDP and a screen with tables that the participants need to review prior to donation) that you can use and combine/rearrange in creative ways to make your own study.
These components are combined in a Python script that is created by the researcher or a research engineer.

On a high level the script works as follows:

1. The Python script determines which user interface (UI) component needs to be shown to the participant
2. The participant interacts with the UI component on screen. Whenever the participant is done interacting with the UI component, the result of that interaction is returned to the script.
3. The script handles the return result en determine the next UI component that the participant needs to see or interact with, go back to step 1 until the end of the donation flow.


The architecture of the data donation task
------------------------------------------

The data donation task is a web application (build with React_ and Pyodide_) that completely runs in the browser of the participant. 
The Python script and the UI components will run completely in the browser of the participant.
Data is only sent to the server upon the participant clicking a consent button.


Creating your own study
-----------------------

A researcher can implement their own data donation flow by altering a Python script included in the repository called script.py_.
``script.py`` has 2 different purposes:

1. It determines the data donation flow. i.e. what screens (for example a file prompt) does the participant gets to see and when. 
2. You can place functions here that extract the data you are interested in from the participants submission. Here is were Python really shines, you can use most data extraction methods you are familiar with! (As long as it's available in Pyodide_.

A typical script includes the following steps:

1. Prompt the participant to submit a file
2. Handling the submission from step 1. This is the step where you can extract the data you are interested in.
3. The extracted data is presented on screen accompanied with a consent button. After consent is given, the data is sent to a storage location of the researcher (not included in the data donation task).

A example such a script is included in the github repo: script.py_.
We recommend you use that script as starting point for your own data donation study.

Check the getting started section for a detailed tutorial.



Getting started 
===============

Checkout the following articles to get started:

.. toctree::
   :maxdepth: 2

   getting_started/index

API Reference
=============

You can find the API documentation here:

.. toctree::
   :maxdepth: 2

   api/index


Standard scripts
================

We provide standard extraction scripts for a various platforms which you can find here:

.. toctree::
   :maxdepth: 1

   standard_scripts/index


    
.. _Next: https://github.com/eyra/mono/
.. _Eyra: https://eyra.co/
.. _script.py: https://github.com/d3i-infra/data-donation-task/blob/master/src/framework/processing/py/port/script.py
.. _Pyodide: https://pyodide.org/en/stable/
.. _Start Here!: https://datadonation.eu/data-donation/
.. _React: https://react.dev/
