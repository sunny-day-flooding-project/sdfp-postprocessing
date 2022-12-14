FROM python:3.10.4

MAINTAINER Ian Hoyt-McCulllough <ianiac@email.unc.edu>

LABEL io.k8s.description="Processing script for Sunny Day Flooding Project" \
 io.k8s.display-name="sdfp-processing" \
 io.openshift.expose-services="5432:http"

WORKDIR /code

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . .

USER 1001
EXPOSE 5432

ENTRYPOINT ["python"]
CMD ["sftp-processing.py"]