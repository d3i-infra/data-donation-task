Platform Documentation
=============================

For various platforms we provide default extraction scripts, so you do not have to reinvent the wheel.

Feel free to use the extraction scripts however you see fit.

In order to use the scripts open the file ``packages/python/port/main.py`` and change this line:

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
* Facebook
* LinkedIn
* Netflix
* TikTok
* WhatsApp
* X
* YouTube

You can find these scripts in: ``packages/python/port/platforms``.

If a platform you are interested in is not listed. Please get in contact with us (a lot of scripts have been developed but they haven't been standardized yet), we would be happy to provide a standardized version for you.


Before using
____________

Please note that platforms are subject to change over time—data formats, export options, and structures may be updated without notice. As a result, some extraction scripts may stop working or behave unexpectedly. If you encounter issues with a script, please let us know—we’ll do our best to provide an updated, standardized version.

Some platforms allow you to request a DDP in different formats. To understand which DDP filetypes a specific extraction script expects, you should look inside the module and check the DDP_CATEGORIES definition. This constant lists the categories of data the script is designed to process. If you see that the request format that you want is not included, please let us know, we can add it.

Additionally, make sure to read the module-level docstrings carefully! Each platform may have specific details, limitations, or processing nuances that are important to understand before using or modifying the scripts.
