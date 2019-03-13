

see:

[How to dockerize a python application](https://runnable.com/docker/python/dockerize-your-python-application)


Create a Dockerfile



Build an image from the Dockerfile.

    docker build -t python-gdelt .
    docker build -t py-gdelt:latest .]

Run the image

    docker run python-barcode

    docker run --name=python-gdelt -v "${HOME}/Projects/2019_News/Gdelt_Data":/Gdelt_Data python-gdelt

