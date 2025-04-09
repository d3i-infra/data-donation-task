# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath('../../packages/python/'))

# Ignore external dependencies
autodoc_mock_imports = ['pandas', 'numpy']

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Port'
copyright = '2024, Boeschoten et al.'
author = 'Boeschoten et al.'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['myst_parser', 'sphinx.ext.autodoc', 'sphinx.ext.autosummary', 'sphinx.ext.napoleon']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ['_static']

#html_sidebars = {
#    '**': [
#        'globaltoc.html',
#    ]
#}

html_theme_options = {
    "globaltoc_collapse": False,
    "home_page_in_toc": True,
    "repository_url": "https://github.com/d3i-infra/data-donation-task",
    "use_repository_button": True,
}

html_title = "The data donation task"
