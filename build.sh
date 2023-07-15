poetry build
cd docs
poetry run make clean
poetry run make html
cd ..
