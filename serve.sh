cd docs
poetry run make clean
cd ..
poetry run sphinx-autobuild docs/source docs/build/html
