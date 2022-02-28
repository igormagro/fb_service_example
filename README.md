# Fusionbase Service: 'Name' 

This repository contains the python code for the {NAME} service.

## 👨‍💻 Development 

All Fusionbase related credentials 🔒 are passed via environment variables.
Look at the Dockerfile for more information.

The following command builds the Docker image
```bash
docker build --build-arg GIT_TOKEN=$GIT_TOKEN -t fb_service__NAME_OF_SERVICE .
```

With the following command you can run the Docker container
```bash
docker run -p 8000:80 fb_service__NAME_OF_SERVICE
```

<!-- optional section start -->

##  🗒 Notes
Please install package xyz by using:
```
brew install xyz
git clone another_thing
sudo make 'i think you get the idea of this section'
```
<!-- optional section end -->

## 🚧 Roadmap 
See the [open issues](https://github.com/FusionbaseHQ/fb_service__NAME_OF_SERVICE/issues) for a list of proposed features (and known issues).


<!-- This section should only be used if you wanna leave important notes such as special prerequisites etc. or relevant information to other engineers otherwise please remove the section from your individual readme  -->
