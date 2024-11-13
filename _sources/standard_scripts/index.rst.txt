Platform Documentation
=============================

For various platforms we provide default extraction scripts, so you do not have to invent the wheel.

Freel free to use the extraction scripts as you see fit.

In order to use the scripts open the file `src/framework/processing/py/port/main.py` and change this line:

.. code-block:: python

    from port.script import process

to:

.. code-block:: python

    #from port.script import process

    # Change to (in this case the standard script for instagram will be used):
    from port.platforms.instagram import process

Available platforms
-------------------

.. automodule:: port.platforms.chatgpt


Instagram
---------

.. automodule:: port.platforms.instagram
