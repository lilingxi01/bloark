# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Blocks Architecture (BloArk)'
copyright = '2023, Lingxi Li. All rights reserved'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['myst_parser', 'sphinx.ext.autodoc', 'sphinx_favicon']

templates_path = ['_templates']
exclude_patterns = ['.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_book_theme'
html_static_path = ['_static']
html_title = "BloArk"
html_theme_options = {
    "logo": {
        "image_light": "./_static/logo.png",
        "image_dark": "./_static/logo_dark.png",
    },
    "repository_url": "https://github.com/lilingxi01/bloark",
    "use_repository_button": True,
    "show_toc_level": 3,
    "announcement": "<b>WARNING:</b> BloArk is under active development and is not stable yet!",
}

favicons = [
    {"href": "favicon.ico"},
]
