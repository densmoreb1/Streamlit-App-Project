# Streamlit App

This is an app made with Streamlit, Docker, and Heroku. I was given data from Safegraph and was tasked with creating an interactive way for users to explore the data.

## Installation 
### Prerequisites: 
- Docker 

### Steps:
1. Navigate into the cloned repository
2. `docker build -t {NAME} .`
3. `docker run -p 8501:8501 {NAME}`
4. This should open a browser, if not then navigate to `localhost:8501`

## Heroku
This repository is also (was, no longer is) connected to a deployed container on Heroku. It can be accessed in the browser at `https://secure-coast-58551.herokuapp.com`
