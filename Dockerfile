FROM python:3.10.4

WORKDIR /code

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . .

EXPOSE 5432

# CMD ["python", "process_old_pressure.py"]
CMD ["python", "drift_correction_old_data.py"]