FROM quay.io/astronomer/astro-runtime:12.5.0

# Kaggle credentials
ENV KAGGLE_USERNAME=${KAGGLE_USERNAME}
ENV KAGGLE_KEY=${KAGGLE_KEY}