Platform Documentation
=============================

For various platforms we provide default extraction scripts built on the **FlowBuilder** pattern,
so you do not have to reinvent the wheel.

Each platform module in ``packages/python/port/platforms/`` contains a ``FlowBuilder`` subclass
that handles the full donation flow (file prompt, validation, extraction, consent, donation)
and a ``process()`` function that ``script.py`` calls via ``yield from``.

To run a single platform, you can either:

1. Set the ``VITE_PLATFORM`` environment variable to the platform name (used by per-platform release builds), or
2. Edit the ``PLATFORM_REGISTRY`` list in ``script.py`` to include only the platform(s) you want

Available platforms
-------------------

Currently we have default scripts for the following platforms:

* ChatGPT
* Chrome
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
