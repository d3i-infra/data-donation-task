Platform Documentation
=============================

For various platforms we provide default extraction scripts, so you do not have to reinvent the wheel.

Freel free to use the extraction scripts as you see fit.

In order to use the scripts open the file ``src/framework/processing/py/port/main.py`` and change this line:

.. code-block:: python

    from port.script import process

to:

.. code-block:: python

    #from port.script import process

    # Change to (in this case the standard script for instagram will be used):
    from port.platforms.instagram import process

Available platforms
-------------------

Currently we have default scripts for the following platforms:

* ChatGPT
* Instagram

You can find these scripts in: ``/src/framework/processing/py/port/platforms/``.

If a platform you are interested in is not listed. Please get in contact with us (a lot of scripts have been developed but they haven't been standardized yet), we would be happy to provide a standardized version for you.

